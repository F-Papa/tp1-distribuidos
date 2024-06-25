import selectors
from collections import defaultdict
import errno
import random
import signal
import socket
import time
import logging
from typing import Iterable, Optional
import docker
from enum import Enum
import os
import logging
import threading
from src.utils.config_loader import Configuration

class ShuttingDown(Exception):
    pass

HEALTHCHECK_REQUEST_CODE = 1
HEALTHCHECK_RESPONSE_CODE = 2

CONNECTION_PORT = 12345
INT_ENCODING_LENGTH = 1
CONNECTION_RETRIES = 5 #3 !!!
SETUP_GRACE_PERIOD = 30
RETRY_INTERVAL = 2

CONNECTION_TIMEOUT = 5 #5
SETUP_TIMEOUT = 7 #15
TIMEOUT = 4
OK_TIMEOUT = 4 #
ELECTION_TIMEOUT = 2 #4
RESOLUTION_APROX_TIMEOUT = 4
LEADER_TIMEOUT = 60
REVIVE_TIME = 15
COORDINATOR_TIMEOUT = LEADER_TIMEOUT
COORDINATOR_OK_TIMEOUT = 15 #15

VOTING_DURATION = 8
LEADER_ANNOUNCEMENT_DURATION = 8
HEALTHCHECK_INTERVAL_SECONDS = 5
HEALTHCHECK_TIMEOUT = 25
VOTING_COOLDOWN = 6

MSG_REDUNDANCY = 1

#region: Messages
class Message(Enum):
    CONNECT = 1
    CONNECTED = 2
    ELECTION = 3
    OK = 4
    COORDINATOR = 5
    ACCEPTED = 6
    HEALTHCHECK = 7
    IM_ALIVE = 8
    HELLO = 9
    COORDINATOR_OK = 10

def decode_int(bytes: bytes) -> int:
    return int.from_bytes(bytes, "big")

def sender_id(received: bytes) -> Optional[str]:
    length = decode_int(received[INT_ENCODING_LENGTH:2*INT_ENCODING_LENGTH])
    id = received[INT_ENCODING_LENGTH*2:].decode("utf-8")
    if len(id) != length:
        logging.error(f"Incomplete sender_id: {id} | {length} {len(id)}")
        return None
    return id

def is_type(data: bytes, msg_type: Message) -> bool:
    return decode_int(data[:INT_ENCODING_LENGTH]) == msg_type.value

def coordinator_ok_msg():
    return Message.COORDINATOR_OK.value.to_bytes(INT_ENCODING_LENGTH, "big")

def connect_msg(medic_number: int) -> bytes:
    return (
        b""
        + Message.CONNECT.value.to_bytes(INT_ENCODING_LENGTH, "big")
        + medic_number.to_bytes(INT_ENCODING_LENGTH, "big")
    )

def hello_msg(medic_number: int) -> bytes:
    return (
        b""
        + Message.HELLO.value.to_bytes(INT_ENCODING_LENGTH, "big")
        + medic_number.to_bytes(INT_ENCODING_LENGTH, "big")
    )

def im_alive_msg(medic_id: str) -> bytes:
    return (
        b""
        + Message.IM_ALIVE.value.to_bytes(INT_ENCODING_LENGTH, "big")
        + len(medic_id).to_bytes(INT_ENCODING_LENGTH, "big")
        + medic_id.encode('utf-8')
    )

def connected_msg() -> bytes:
    return Message.CONNECTED.value.to_bytes(INT_ENCODING_LENGTH, "big")

def election_msg() -> bytes:
    return Message.ELECTION.value.to_bytes(INT_ENCODING_LENGTH, "big")

def healthcheck_msg(sender_id: str) -> bytes:
    return (
        b""
        + Message.HEALTHCHECK.value.to_bytes(INT_ENCODING_LENGTH, "big")
        + len(sender_id).to_bytes(INT_ENCODING_LENGTH, "big")
        + sender_id.encode('utf-8')
    )

def ok_msg() -> bytes:
    return Message.OK.value.to_bytes(INT_ENCODING_LENGTH, "big")

def coord_msg() -> bytes:
    return Message.COORDINATOR.value.to_bytes(INT_ENCODING_LENGTH, "big")

#endregion
#region: Sockets
def free_socket(sock: socket.socket):
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except:
        pass

    sock.close()

def recv_bytes(sock: socket.socket, length: int):
    received = b""
    while len(received) < length:
        received += sock.recv(length - len(received))
        if not received:
            return None
    return received

def send_bytes(sock: socket.socket, data: bytes):
    bytes_sent = 0
    while bytes_sent < len(data):
        bytes_sent += sock.send(data[bytes_sent:])

#endregion
class Medic:
    CONTROLLER_TYPE = "medic"

    def __init__(self, controllers_to_check: dict, config: Configuration):
        # dict{nombre_controller: address_controller}
        self._number_of_medics = config.get("NUMBER_OF_MEDICS")
        self._medic_number = config.get("MEDIC_NUMBER")
        self._other_medics = {}
        self._greater_medics = {}
        self._smaller_medics = {}

        for i in range(1, self._number_of_medics + 1):
            if i > self._medic_number:
                self._greater_medics[f"medic{i}"] = f"medic{i}"
            if i != self._medic_number:
                self._other_medics[f"medic{i}"] = f"medic{i}"
            if i < self._medic_number:
                self._smaller_medics[f"medic{i}"] = f"medic{i}"

        self.controllers_to_check = controllers_to_check.copy()
        self.controllers_to_check.update(self._other_medics)

        self._listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._id = f"{self.CONTROLLER_TYPE}{self._medic_number}"
        
        self._is_leader = False
        self._leader = None
        
        self.docker_client = docker.DockerClient(base_url="unix://var/run/docker.sock")
        self._udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        
        self._connections_lock = threading.Lock()
        self._connections = {}
        self._cached_ips = {}

        self._check_thread: Optional[threading.Thread] = None
        self._transfering_leader_condvar = threading.Condition()
        self._transfering_leader = False
        self._last_contact_timestamp = dict()
        self._shutting_down = False

    def shutdown(self):
        self._shutting_down = True
        with self._connections_lock:
            open_conns = list(self._connections.items())
        for _, conn in open_conns:
            self.close_socket(conn)
        
        self._transfering_leader = True
        with self._transfering_leader_condvar:
            self._transfering_leader_condvar.notify()
        
        self.close_socket(self._listen_socket)
        logging.info("Shutting down")

    # region: Connection Setup
    def setup_connections(self):
        """Setup up connection with all other medics, some of them may not be established succesfully"""
        self._listen_socket.bind(("0.0.0.0", CONNECTION_PORT))
        self._listen_socket.listen(self._number_of_medics)

        time.sleep(SETUP_GRACE_PERIOD)

        connection_threads = []
        for id in self._greater_medics:
            thread = threading.Thread(target=self.connect_to, args=(id,))
            thread.start()
            connection_threads.append(thread)

        self.accept_connection_from_smaller_medics()

        for thread in connection_threads:
            thread.join()

    def new_connection(self, id: str):
        """Establish a TCP connection with 'id'. Returns the socket if successful or None otherwise."""

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(CONNECTION_TIMEOUT)
        expected_errors = [errno.ECONNREFUSED, errno.ETIMEDOUT]
        
        for _ in range(CONNECTION_RETRIES):
            try:
                sock.connect((id, CONNECTION_PORT))
                return sock
            except socket.gaierror:
                # Hostname could not be resolved to an address
                logging.error(f"at connect_to_other ({id}): Error resolving name")
                break
            except OSError as e:
                if e.errno in expected_errors:
                    continue
                else: 
                    raise e
            except Exception as e:
                logging.error(f"Unexpected error connecting to {id}: {e}")
                break
        
        return None

    def handshake(self, sock: socket.socket, target_id: str):
        try:
            send_bytes(sock, connect_msg(self._medic_number))
        except:
            logging.error("Unexpected error sending Handshake to {id}: {e}")
            free_socket(sock)
            return 
        try:
            sock.settimeout(CONNECTION_TIMEOUT) 
            received = recv_bytes(sock, INT_ENCODING_LENGTH)

            if not received:
                free_socket(sock)
                return

            if is_type(received, Message.CONNECTED):
                with self._connections_lock:
                    self._connections[target_id] = sock

        except socket.timeout:
            logging.error(f"{target_id} timed-out during handshake")
            free_socket(sock)
        except:
            logging.error("Unexpected error receiving Handshake response from {id}: {e}")
            free_socket(sock)

    def connect_to(self, target_id: str):
        """Establish the connection and initiate the handshake. If the handshake is successful, 
        the connection is saved in the state, otherwise it is closed"""

        # Conect via TCP
        conn = self.new_connection(target_id)
        if not conn:
            return
        
        # App Layer Handshake: Say who I am
        self.handshake(conn, target_id)
       
    def accept_connection(self, sock: socket.socket):
        """Listen for a TCP conection and respond to the handshake. If it is successful it will save it
        in the state, otherwise it will be closed"""
        
        sock.settimeout(SETUP_TIMEOUT)
        conn, addr = sock.accept()
        try:
            received = recv_bytes(conn, INT_ENCODING_LENGTH*2)
        except OSError as e:
            if e.errno == errno.ETIMEDOUT:
                # If one times out, elevate the exception to break the loop
                raise e
            logging.error(f"Unexpected error receiving Handshake from {addr}: {e}")
            return
        
        if not received:
            free_socket(conn)
            return

        if is_type(received, Message.CONNECT):
            received_id = decode_int(received[INT_ENCODING_LENGTH:INT_ENCODING_LENGTH*2])
            received_id = f"medic{received_id}"
            try:
                send_bytes(conn, connected_msg())
                with self._connections_lock:
                    self._connections[received_id] = conn
            except Exception as e:
                logging.error(f"Unexpected error sending Handshake response to {received_id}: {e}")
                return
        else:
            logging.info(f"at accept_connection: Received unexpected message from {addr}: {received}")

    def send_hello_to_smaller_medics(self):
        """Send a UDP datagram for to a smaller medics so that they initiate the connection"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        for medic in self._smaller_medics:
            try:
                sock.sendto(hello_msg(self._medic_number), (medic, CONNECTION_PORT))
            except:
                pass

    def accept_connection_from_smaller_medics(self):
        """Accept connection from all smaller medics, prompting them via UDP to initiate it"""
        self.send_hello_to_smaller_medics()
        try:
            for _ in range(len(self._smaller_medics)):
                self.accept_connection(self._listen_socket)
        except socket.timeout:
            pass
    
    def succesful_connections(self) -> list[str]:
        with self._connections_lock:
            connected_to = list(self._connections.keys())
            connected_to.sort()
            return connected_to
    #endregion: Connection

    #region: Loop handlers
    def is_leader_dead(self) -> int:
        if self._leader not in self._last_contact_timestamp:
            return True
        time_elapsed = time.time() - self._last_contact_timestamp[self._leader]
        return time_elapsed >= LEADER_TIMEOUT

    def handle_udp_message(self):
        received, addr = self._udp_sock.recvfrom(1024)

        if is_type(received, Message.HELLO):
            number = decode_int(received[INT_ENCODING_LENGTH:])
            id = f"medic{number}"
            logging.info(f"📣   HELLO from {id}")
            self._cached_ips[id] = addr[0]
            self.connect_to(id)
            logging.info(f"📶  Connected to {id}")

        elif is_type(received, Message.HEALTHCHECK):
            id = sender_id(received)
            if not id:
                return
            
            self._cached_ips[id] = addr[0]
            if id in self._cached_ips:
                self._udp_sock.sendto(
                    im_alive_msg(self._id),
                    (self._cached_ips[id], CONNECTION_PORT),
                )
            else:
                self._udp_sock.sendto(
                    im_alive_msg(self._id),
                    (id, CONNECTION_PORT),
                )

            self._last_contact_timestamp[id] = time.time()

        elif is_type(received, Message.IM_ALIVE):
            id = sender_id(received)
            if not id:
                return
            self._cached_ips[id] = addr[0]
            #logging.info(f"💓   Received IM ALIVE from {id}")
            self._last_contact_timestamp[id] = time.time()
        else:

            logging.error(f"Unkown UDP message received: {received}")

    def handle_tcp_message(self, sock: socket.socket, sel: selectors.DefaultSelector):
        sender = self.resolve_socket(sock)
        if not sender:
            logging.error("At handle_tcp_message: Invalid Socket")
            sel.unregister(sock)
            self.close_socket(sock)
            return

        try:
            received = recv_bytes(sock, INT_ENCODING_LENGTH)
        except Exception as e:
            logging.error(f"Exception {e} happened in: {sender}")
            sel.unregister(sock)
            self.close_socket(sock)
            return

        if not received:
            sel.unregister(sock)
            self.close_socket(sock)
            return

        if is_type(received, Message.COORDINATOR):
            self.set_leader(sender)
            self.send_to(coordinator_ok_msg(), sender)
            logging.info(f"⭐   Z COORDINATOR: {sender}")

        elif is_type(received, Message.ELECTION):
            logging.info(f"Election received from: {sender}")
            try:
                self.send_to(ok_msg(), sender)
            except Exception as e:
                logging.error(f"at loop with {sender}: {e}")
                self.close_socket(sock)
            self.election(initiator_id=sender)
        else:
            logging.info(f"at loop: unexpected message from {sender}: {received}")

    def loop(self):
        self._udp_sock.bind(("0.0.0.0", CONNECTION_PORT))
        sel = selectors.DefaultSelector()
        
        self.register_connections(sel, self._other_medics)
        self._udp_sock.setblocking(False)
        sel.register(self._udp_sock, selectors.EVENT_READ)
        self._listen_socket.setblocking(False)
        sel.register(self._listen_socket, selectors.EVENT_READ)

        try:
            while not self._shutting_down:
                events = sel.select(timeout=LEADER_TIMEOUT + 2*self._medic_number)

                if not events:
                    if self.is_leader_dead():
                        logging.error("Leader is dead")
                        self.election(self._id)
                    # TODO: Check if election failed
                    break     

                for key, _ in events:
                    
                    sock: socket.socket = key.fileobj  # type: ignore

                    if sock == self._listen_socket:
                        self.accept_connection(sock)
                        continue

                    elif sock == self._udp_sock:
                        self.handle_udp_message()
                        continue
                    
                    else:
                        self.handle_tcp_message(sock, sel)
        
        except ShuttingDown:
            pass
        finally:
            sel.close()
    #endregion

    #region: Start
    def start(self):
        try:
            self.setup_connections()
            connections = self.succesful_connections()
            logging.info(f"📶   Connections: {connections}")
            self.election(self._id)
        except ShuttingDown:
            return
        
        # TODO: Check if election failed        
        self.loop()
    #endregion
    def send_to(self, message: bytes, medic_id: str):
        with self._connections_lock:
            if conn := self._connections.get(medic_id):
                try:
                    send_bytes(conn, message)
                except Exception as e:
                    logging.error(f"Exception sending message to {medic_id}: {e}")
            else:
                msg_type = int.from_bytes(message[:INT_ENCODING_LENGTH], "big")
                logging.error(f"Cannot send {Message(msg_type).name} message to {medic_id}. Not connected")

    def register_connections(self, sel: selectors.DefaultSelector, medic_ids: Iterable[str]):
        for id in medic_ids:
            with self._connections_lock:
                if conn := self._connections.get(id):
                    conn.setblocking(False)
                    sel.register(conn, selectors.EVENT_READ)

#region: Election

    def election(self, initiator_id: str):
        
        if initiator_id != self._id:
            self.send_to(ok_msg(), initiator_id)

        self.send_election()
        self.answer_to_elections(initiator_id)
        
        oks_received = set()
        coord_received = set()
        self.listen_for_oks(oks_received, coord_received)

        # Someone else won election
        if coord_received or oks_received:
            
            if coord_received:
                leader_id = self.greatest_id(coord_received)
            else:
                leader_id = self.listen_for_coordinator()

            if leader_id is None:
                # TODO: Restart election
                logging.error("Election failed")
                exit(1)

            self.set_leader(leader_id)
            self.send_to(coordinator_ok_msg(), leader_id)

            logging.info(f"⭐   Coordinator: {self._leader}")

        # I won election
        else:
            self.set_leader(self._id)
            self.announce_coordinator()
            self.wait_for_coordinator_oks()
            logging.info(f"🌟   Coordinator: {self._leader}")
    
    def set_leader(self, leader_id: str):
        self._leader = leader_id
        if leader_id == self._id:
            self._is_leader = True
            if not self._check_thread:
                controllers_to_check = self.controllers_to_check.copy()
                controllers_to_check.update(self._other_medics)
                check_thread = threading.Thread(target=self.check_on_controllers, args=(controllers_to_check,))
                self._check_thread = check_thread
                check_thread.start()
        else:
            self._is_leader = False
            if self._check_thread:
                self._transfering_leader = True
                with self._transfering_leader_condvar:
                    self._transfering_leader_condvar.notify()
                self._check_thread.join()
                self._check_thread = None
                logging.info("Check thread Joined")

    def send_election(self):
        logging.info("Sending elections")
        for medic_id in self._greater_medics:
            try:
                self.send_to(election_msg(), medic_id)
            except Exception as e:
                logging.error(f"Unexpected error sending Election to: {medic_id}: {e}")

    def answer_to_elections(self, initiator_id: str):
        logging.info("Answering to elections")

        sel = selectors.DefaultSelector()  
        self.register_connections(sel, self._smaller_medics)

        elections_received = set()
        if initiator_id != self._id:
            elections_received.add(initiator_id)

        while len(elections_received) < len(self._smaller_medics) and not self._shutting_down:
            events = sel.select(timeout=ELECTION_TIMEOUT)

            if not events:
                break

            for key, _ in events:
                sock: socket.socket = key.fileobj  # type: ignore
                self.answer_to_elections_aux(sel, sock, elections_received)

        logging.info(
            f"{len(elections_received)}/{len(self._smaller_medics)} Elections received"
        )

        sel.close()

    def answer_to_elections_aux(self, sel: selectors.DefaultSelector, sock: socket.socket, elections_received: set):
        sender = self.resolve_socket(sock)
        if not sender:
            logging.error("At answer_to_elections_aux: Invalid Socket")
            sel.unregister(sock)
            self.close_socket(sock)
            return
        
        try:
            received = recv_bytes(sock, INT_ENCODING_LENGTH)
        except Exception as e:
            logging.error(f"at wait_for_coordinator_oks_aux: {e}")
            self.close_socket(sock)
            sel.unregister(sock)
            return
        
        if not received:
            self.close_socket(sock)
            sel.unregister(sock)
            return            

        if is_type(received, Message.ELECTION):
            elections_received.add(sender)
            self.send_to(ok_msg(), sender)

        else:
            logging.error(
                f"at answer_to_elections: Unexpected message received from {id}: {received}"
            )




    def resolve_socket(self, sock: socket.socket) -> Optional[str]:
        with self._connections_lock:
            for id in self._connections:
                if self._connections[id] == sock:
                    return id
        
        return None

    def wait_for_coordinator_oks(self):
        sel = selectors.DefaultSelector()
        self.register_connections(sel, self._smaller_medics)
        
        coord_oks_received = set()
        while len(coord_oks_received) < len(self._smaller_medics) and not self._shutting_down:
            events = sel.select(timeout=COORDINATOR_OK_TIMEOUT)
            
            if not events:
                break

            for key, _ in events:
                sock: socket.socket = key.fileobj  # type: ignore
                self.wait_for_coordinator_oks_aux(sel, sock, coord_oks_received)

        if len(coord_oks_received) < len(self._smaller_medics):
            logging.info(f"({len(coord_oks_received)}/{len(self._smaller_medics)}) COORDINATOR OKs received: {list(coord_oks_received)}")
        else:
            logging.info(f"({len(coord_oks_received)}/{len(self._smaller_medics)}) COORDINATOR OKs received")

        sel.close()

    def wait_for_coordinator_oks_aux(self, sel: selectors.DefaultSelector, sock: socket.socket, coordinator_oks_received: set):
        sender = self.resolve_socket(sock)
        if not sender:
            logging.error("At wait_for_coordinator_oks_aux: Invalid Socket")
            sel.unregister(sock)
            self.close_socket(sock)
            return
        try:
            received = recv_bytes(sock, INT_ENCODING_LENGTH)
        except Exception as e:
            logging.error(f"at wait_for_coordinator_oks_aux: {e}")
            self.close_socket(sock)
            sel.unregister(sock)
            return

        if not received:
            self.close_socket(sock)
            sel.unregister(sock)
            return

        elif is_type(received, Message.COORDINATOR_OK):
            coordinator_oks_received.add(sender)

        elif is_type(received, Message.ELECTION):
            self.send_to(ok_msg(), sender)
            self.send_to(coord_msg(), sender)
        
        else:
            logging.error(
                f"at listen_for_coordinator_oks: Unexpected message received from {sender}: {received}"
            )

    def listen_for_coordinator(self) -> Optional[str]:
        sel = selectors.DefaultSelector()
        self.register_connections(sel, self._greater_medics)

        coord_received = set()
        while len(coord_received) < 1 and not self._shutting_down:
            events = sel.select(timeout=COORDINATOR_TIMEOUT)
            if not events:
                logging.error("No coordinator received")
                break

            for key, _ in events:
                sock: socket.socket = key.fileobj  # type: ignore
                self.listen_for_coordinator_aux(sel, sock, coord_received)

        sel.close()
        return self.greatest_id(coord_received)

    def listen_for_coordinator_aux(self, sel: selectors.DefaultSelector, sock: socket.socket, coord_received: set):
        sender = self.resolve_socket(sock)
        if not sender:
            logging.error("At wait_for_coordinator_oks_aux: Invalid Socket")
            sel.unregister(sock)
            self.close_socket(sock)
            return
        
        try:
            received = recv_bytes(sock, INT_ENCODING_LENGTH)
        
        except Exception as e:
            logging.error(f"at listen_for_coordinator: {e}")
            self.close_socket(sock)
            sel.unregister(sock)
            return
            
        if not received:
            self.close_socket(sock)
            sel.unregister(sock)
            return
    
        if is_type(received, Message.COORDINATOR):
            logging.info(f"Received coordinator from {sender}")
            coord_received.add(sender)
        else:
            logging.error(
                f"at listen_for_coordinator: Unexpected message received from {id}: {received}"
            )

    def announce_coordinator(self):
        with self._connections_lock:
            for id in self._smaller_medics:
                if conn := self._connections.get(id):
                    # logging.info(f"Sent coord to: {id}")
                    send_bytes(conn, coord_msg())

    def listen_for_oks(self, oks_received: set, coord_received: set):
        """Listens for OK messages and adds the senders to 'oks_received'. 
        If a COORDINATOR message is received, it returns it"""

        sel = selectors.DefaultSelector()
        self.register_connections(sel, self._greater_medics)

        coord_received.clear()
        oks_received.clear()

        while len(oks_received) < len(self._greater_medics) and not self._shutting_down:
            events = sel.select(timeout=OK_TIMEOUT * 4)
            if not events:
                break

            for key, _ in events:
                sock: socket.socket = key.fileobj  # type: ignore
                self.listen_for_oks_aux(sel, sock, oks_received, coord_received)

        logging.info(
            f"{len(oks_received)}/{len(self._greater_medics)} Oks received: {list(oks_received)}"
        )
        sel.close()

    def listen_for_oks_aux(self, sel: selectors.DefaultSelector, sock: socket.socket, oks_received: set, coord_received: set) -> Optional[str]:
        sender = self.resolve_socket(sock)
        if not sender:
            logging.error("At listen_for_oks_aux: Invalid Socket")
            sel.unregister(sock)
            self.close_socket(sock)
            return
        
        try:
            received = recv_bytes(sock, INT_ENCODING_LENGTH)
        except Exception as e:
            logging.error(f"at listen_for_oks_aux: {e}")
            sel.unregister(sock)
            self.close_socket(sock)
            return
        
        if not received:
            self.close_socket(sock)
            sel.unregister(sock)
            return
        
        if is_type(received, Message.OK):
            oks_received.add(sender)
        
        elif is_type(received, Message.COORDINATOR):
            coord_received.add(sender)
        
        else:
            logging.error(
                f"at listen_for_oks: Unexpected message received from {sender}: {received}"
            )
    #endregion

    def close_socket(self, sock: socket.socket):
        
        with self._connections_lock:
            for id in self._connections:
                if self._connections[id] == sock:
                    try:
                        sock.shutdown(socket.SHUT_RDWR)
                    except:
                        pass
            
                    sock.close()
                    del self._connections[id]
                    logging.info(f"Connection closed: {id}")    
                    return

    def greatest_id(self, medic_ids: Iterable[str]) -> str:
        if not medic_ids:
            raise ValueError("No medics to choose from")

        number_to_id = dict()

        for id in medic_ids:
            number = id[len("medic") :]
            number_to_id[int(number)] = id

        max_key = max(number_to_id.keys())
        return number_to_id[max_key]

    def revive_controller(self, controller_id: str):
        logging.info(f"🩺   Reviving controllers: {controller_id}")
        container = self.docker_client.containers.get(controller_id)
        try:  # KILL IF NOT DEAD
            container.kill()
        except:
            pass
        container.start()  # REVIVE

    def check_on_controllers(self, controller_ids: Iterable[str]):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        


        last_check = time.time()
        dead_controllers = set()

        while not self._shutting_down:
            if (time.time() - last_check) >= (HEALTHCHECK_TIMEOUT // 2):
                for id in controller_ids:
                    if not self._last_contact_timestamp.get(id):
                        logging.info(f"{id} id not here")
                        dead_controllers.add(id)
                        continue
                    elif (
                        time.time() - self._last_contact_timestamp[id]
                    ) >= HEALTHCHECK_TIMEOUT:
                        logging.info(f"{id} timed out")
                        dead_controllers.add(id)

                logging.info(f"Dead Controllers: {len(dead_controllers)}")

                for id in dead_controllers:
                    self.revive_controller(id)
                    self._last_contact_timestamp[id] = time.time() + REVIVE_TIME

                last_check = time.time()
                dead_controllers.clear()

            for id in controller_ids:
                try:
                    sock.sendto(
                        healthcheck_msg(self._id), (id, CONNECTION_PORT)
                    )
                except socket.gaierror:
                    continue

            if self._transfering_leader:
                return

            with self._transfering_leader_condvar:
                if self._transfering_leader_condvar.wait(
                    timeout=HEALTHCHECK_INTERVAL_SECONDS
                ):
                    return

def config_logging(level: str):

    level = getattr(logging, level)

    # Filter logging
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main():

    required = {
        "LOGGING_LEVEL": str,
        "MEDIC_NUMBER": int,
        "NUMBER_OF_MEDICS": int,
    }

    config = Configuration.from_file(required, "config.ini")
    config.update_from_env()
    config.validate()

    config_logging(config.get("LOGGING_LEVEL"))
    logging.info(config)

    medic = Medic(config=config, controllers_to_check={
        "title_filter1": "title_filter1",
        "title_filter2": "title_filter2",
        "title_filter_proxy": "title_filter_proxy",
        "category_filter1": "category_filter1",
        "category_filter_proxy": "category_filter_proxy",
        "date_filter1": "date_filter1",
        "date_filter2": "date_filter2",
        "date_filter_proxy": "date_filter_proxy",
        "review_counter_proxy": "review_counter_proxy",
        "review_counter1": "review_counter1",
        "decade_counter1": "decade_counter1",
        "decade_counter2": "decade_counter2",
        "decade_counter3": "decade_counter3",
        "decade_counter_proxy": "decade_counter_proxy",
        "sentiment_analyzer_proxy": "sentiment_analyzer_proxy",
        "sentiment_analyzer1": "sentiment_analyzer1",
        "sentiment_analyzer2": "sentiment_analyzer2",
        "sentiment_analyzer3": "sentiment_analyzer3",
        "sentiment_analyzer4": "sentiment_analyzer4",
        "sentiment_analyzer5": "sentiment_analyzer5",
        "sentiment_analyzer6": "sentiment_analyzer6",
        "review_joiner_proxy": "review_joiner_proxy",
        "review_joiner1": "review_joiner1",
        "review_joiner2": "review_joiner2",
        "review_joiner3": "review_joiner3",
        "sentiment_average_reducer1": "sentiment_average_reducer1",
        "sentiment_average_reducer2": "sentiment_average_reducer2",
        "sentiment_averager_proxy": "sentiment_averager_proxy",
    }
        
    )
    signal.signal(signal.SIGTERM, lambda *_: medic.shutdown())
    # "title_filter1": "title_filter1", "title_filter2": "title_filter2",
    #                                              "title_filter_proxy": "title_filter_proxy", "date_filter1": "date_filter1",
    #                                                "date_filter2": "date_filter2", "date_filter_proxy": "date_filter_proxy",
    #                                                "category_filter_proxy": "category_filter_proxy", "category_filter1": "category_filter1"}

    medic.start()


if __name__ == "__main__":
    main()
