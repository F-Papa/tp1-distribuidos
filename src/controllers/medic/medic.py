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


HEALTHCHECK_REQUEST_CODE = 1
HEALTHCHECK_RESPONSE_CODE = 2

CONNECTION_PORT = 12345
INT_ENCODING_LENGTH = 1
CONNECTION_RETRIES = 3
RETRY_INTERVAL = 2

CONNECTION_TIMEOUT = 2 #5
SETUP_TIMEOUT = 5 #15
TIMEOUT = 4
OK_TIMEOUT = 1 #
ELECTION_TIMEOUT = 1 #4
RESOLUTION_APROX_TIMEOUT = 4
LEADER_TIMEOUT = 30
REVIVE_TIME = 15
COORDINATOR_TIMEOUT = LEADER_TIMEOUT
COORDINATOR_OK_TIMEOUT = 15 #15

VOTING_DURATION = 8
LEADER_ANNOUNCEMENT_DURATION = 8
HEALTHCHECK_INTERVAL_SECONDS = 5
HEALTHCHECK_TIMEOUT = 25
VOTING_COOLDOWN = 6

MSG_REDUNDANCY = 1


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

def connect_msg(medic_id: str) -> bytes:
    return (
        b""
        + Message.CONNECT.value.to_bytes(INT_ENCODING_LENGTH, "big")
        + len(medic_id).to_bytes(INT_ENCODING_LENGTH, "big")
        + medic_id.encode("utf-8")
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

def free_socket(sock: socket.socket):
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except:
        pass

    sock.close()

def is_socket_open(sock: socket.socket):
    try:
        sock.fileno()
    except socket.error:
        return False
    return True

def recv_bytes(sock: socket.socket, length: int):
    received = b""
    while len(received) < length:
        received += sock.recv(length - len(received))
        if len(received) == 0:
            return received
    return received

def send_bytes(sock: socket.socket, data: bytes):
    bytes_sent = 0
    while bytes_sent < len(data):
        bytes_sent += sock.send(data[bytes_sent:])

class Medic:
    CONTROLLER_TYPE = "medic"

    def __init__(self, controllers_to_check: dict, config: Configuration):
        # dict{nombre_controller: address_controller}
        self._number_of_medics = config.get("NUMBER_OF_MEDICS")
        self._medic_number = config.get("MEDIC_NUMBER")
        self._other_medics = {}
        self._greater_medics = {}
        self._first_election_done = False
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
        self._connections_lock = threading.Lock()
        self._connections = {}
        self._threads = {}
        self._check_thread: Optional[threading.Thread] = None
        self._transfering_leader_condvar = threading.Condition()
        self._transfering_leader = False

        self._ok_received = set()
        self._ok_received_lock = threading.Lock()
        self._udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._last_im_alive_timestamp = dict()

    def shutdown(self):
        for _, conn in self._connections.items():
            conn.shutdown(socket.SHUT_RDWR)
            conn.close()

    # region: Connection Setup

    def new_connection(self, id: str):
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

    def handshake_initiation(self, sock: socket.socket, target_id: str):
        try:
            send_bytes(sock, connect_msg(self._id))
        except:
            logging.error("Unexpected error sending Handshake to {id}: {e}")
            free_socket(sock)
            return 
        try:
            sock.settimeout(CONNECTION_TIMEOUT) 
            received = recv_bytes(sock, INT_ENCODING_LENGTH)

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
        # Conect via TCP
        conn = self.new_connection(target_id)
        if not conn:
            return
        
        # App Layer Handshake: Say who I am
        self.handshake_initiation(conn, target_id)
       
    def accept_connection(self, sock: socket.socket):
        sock.settimeout(SETUP_TIMEOUT)
        conn, addr = sock.accept()
        try:
            #TODO: que lo primero sea el length
            received = recv_bytes(conn, INT_ENCODING_LENGTH)
        except OSError as e:
            if e.errno == errno.ETIMEDOUT:
                # If one times out, elevate the exception to break the loop
                raise e
            logging.error(f"Unexpected error receiving Handshake from {addr}: {e}")
            return
            
        if is_type(received, Message.CONNECT):
            received += recv_bytes(conn, len("YmedicX"))
            received_id = sender_id(received)
            try:
                send_bytes(conn, connected_msg())
                with self._connections_lock:
                    self._connections[received_id] = conn
            except Exception as e:
                logging.error(f"Unexpected error sending Handshake response to {received_id}: {e}")
                return
        else:
            logging.info(f"at accept_connection: Received unexpected message from {addr}: {received}")
    
    def accept_connection_from_smaller_medics(self):
        try:
            for _ in range(len(self._smaller_medics)):
                self.accept_connection(self._listen_socket)
        except socket.timeout:
            pass
    #endregion: Connection

    def loop(self):
        sel = selectors.DefaultSelector()
        with self._connections_lock:
            for stored_id in self._other_medics:
                if conn := self._connections.get(stored_id):
                    conn.setblocking(False)
                    sel.register(conn, selectors.EVENT_READ)

        self._udp_sock.setblocking(False)
        self._listen_socket.setblocking(False)
        sel.register(self._udp_sock, selectors.EVENT_READ)
        sel.register(self._listen_socket, selectors.EVENT_READ)

        # logging.info("selecting...")
        events = sel.select(timeout=LEADER_TIMEOUT + 2*self._medic_number)

        if not events:
            logging.error(f"Selector timed out")
            last_contact_with_leader = self._last_im_alive_timestamp.get(self._leader)

            if last_contact_with_leader and time.time() - last_contact_with_leader < LEADER_TIMEOUT:
                return

            logging.error(f"💀  Leader {self._leader} is dead, raising election")
            self.send_election()
            self.answer_to_elections(initiator=None)
            leader, oks_received = self.listen_for_oks()

            if leader or oks_received:  # I lost
                if not leader:
                    leader = self.listen_for_coordinator()

                if self._check_thread:
                    self._check_thread.join()
                    logging.info(f"⭐   X Coordinator Transfered: {leader}")

                else:
                    logging.info(f"⭐   X Coordinator: {leader}")

                self._leader = leader
                self._is_leader = False

                with self._connections_lock:
                    conn = self._connections[leader]
                    send_bytes(conn, coordinator_ok_msg())

            else:  # I Won
                if not self._check_thread:  # If i wasn't already the leader
                    check_thread = threading.Thread(
                        target=self.check_on_controllers,
                        args=(self._other_medics,),
                    )
                    self._check_thread = check_thread
                    check_thread.start()

                self._leader = self._id
                self._is_leader = True
                logging.info(f"🌟   Coordinator: {self._leader}")
                self.send_coordinator()
                self.listen_for_coordinator_oks()     
            return           

        for key, _ in events:
            
            sock: socket.socket = key.fileobj  # type: ignore

            if sock == self._listen_socket:
                self.accept_connection(sock)
                # logging.info("New connection accepted")
                # Election?
                return

            elif sock == self._udp_sock:
                received, addr = sock.recvfrom(1024)
                if is_type(received, Message.HELLO):
                    number = decode_int(received[INT_ENCODING_LENGTH:])
                    id = f"medic{number}"
                    logging.info(f"📣   HELLO from {id}")
                    self.connect_to(id)
                    logging.info(f"📶  Connected to {id}")

                elif is_type(received, Message.HEALTHCHECK):
                    id = sender_id(received)
                    if not id:
                        return
                    
                    sock.sendto(
                        im_alive_msg(self._id),
                        (id, CONNECTION_PORT),
                    )
                    self._last_im_alive_timestamp[id] = time.time()

                elif is_type(received, Message.IM_ALIVE):
                    id = sender_id(received)
                    if not id:
                        return
                    logging.info(f"💓   Received IM ALIVE from {id}")
                    self._last_im_alive_timestamp[id] = time.time()
                else:
                    logging.error(f"Unkown UDP message received: {received}")
                return

            id = None
            with self._connections_lock:
                for stored_id in self._other_medics:
                    if conn := self._connections.get(stored_id):
                        if sock == conn:
                            id = stored_id
                            break
            try:
                received = recv_bytes(sock, INT_ENCODING_LENGTH)
            except Exception as e:
                logging.error(f"Exception {e} happened in: {id}")
                continue

            if is_type(received, Message.COORDINATOR):
                if not self._is_leader:
                    with self._connections_lock:
                        conn = self._connections[id]
                        send_bytes(conn, coordinator_ok_msg())
                    logging.info(f"⭐   Z COORDINATOR: {id}")
                else:

                    if self._check_thread:
                        self._transfering_leader = True
                        with self._transfering_leader_condvar:
                            self._transfering_leader_condvar.notify()
                        self._check_thread.join()
                    
                    with self._connections_lock:
                        conn = self._connections[id]
                        send_bytes(conn, coordinator_ok_msg())
                    self._transfering_leader = False
                    logging.info(f"⭐   Z COORDINATION TRANSFERED: {id}")

                self._is_leader = False
                self._leader = id

            elif is_type(received, Message.ELECTION):
                logging.info(f"Election received from: {id}")
                try:
                    send_bytes(sock, ok_msg())
                
                except Exception as e:
                    logging.error(f"at loop with {id}: {e}")
                    self.close_socket(sock)
                    pass

                self.send_election()
                self.answer_to_elections(initiator=id)
                leader, oks_received = self.listen_for_oks()

                if leader or oks_received:  # I lost
                    if not leader:
                        leader = self.listen_for_coordinator()

                    if self._check_thread:
                        self._transfering_leader = True
                        with self._transfering_leader_condvar:
                            self._transfering_leader_condvar.notify()
                        self._check_thread.join()
                        logging.info(f"⭐   Y Coordinator Transfered: {leader}")

                    else:
                        logging.info(f"⭐   Y Coordinator: {leader}")

                    self._leader = leader
                    self._is_leader = False

                    with self._connections_lock:
                        conn = self._connections[leader]
                        send_bytes(conn, coordinator_ok_msg())

                else:  # I Won
                    self._leader = self._id
                    self._is_leader = True
                    self.send_coordinator()
                    self.listen_for_coordinator_oks()
                    logging.info(f"🌟   Y Coordinator: {self._leader}")
                    
                    if not self._check_thread:  # If i wasn't already the leader
                            check_thread = threading.Thread(
                                target=self.check_on_controllers,
                                args=(self._other_medics,),
                            )
                            self._check_thread = check_thread
                            check_thread.start()

            elif not received:
                logging.info(f"at loop: received null from {id}")
                self.close_socket(sock)
                return
            else:
                logging.info(f"at loop: unexpected message from {id}: {received}")

    def send_hello_to_smaller_medics(self):
        """Send a UDP datagram for to a smaller medic in case"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        for medic in self._smaller_medics:
            try:
                sock.sendto(hello_msg(self._medic_number), (medic, CONNECTION_PORT))
            except:
                pass

    def start(self):
        # Connect to greater medics
        self._listen_socket.bind(("0.0.0.0", CONNECTION_PORT))
        self._listen_socket.listen(self._number_of_medics)

        time.sleep(3)

        # Fork Join: Connect to greater medics and accept connections 
        # and at the same time accept connections from smaller medics
        connection_threads = []
        for id in self._greater_medics:
            thread = threading.Thread(target=self.connect_to, args=(id,))
            thread.start()
            connection_threads.append(thread)

        self.send_hello_to_smaller_medics()
        self.accept_connection_from_smaller_medics()

        for thread in connection_threads:
            thread.join()

        with self._connections_lock:
            connected_to = list(self._connections.keys())
            connected_to.sort()
            logging.info(f"📶   Connected to: {connected_to}")

        self.send_election()
        self.answer_to_elections(initiator=None)
        leader, oks_received = self.listen_for_oks()

        if leader:
            self._leader = leader
            self._is_leader = False
            with self._connections_lock:
                conn = self._connections[self._leader]
                send_bytes(conn, coordinator_ok_msg())
            logging.info(f"⭐   Coordinator: {self._leader}")

        elif oks_received:
            self._leader = self.listen_for_coordinator()
            self._is_leader = False
            with self._connections_lock:
                conn = self._connections[self._leader]
                send_bytes(conn, coordinator_ok_msg())
            logging.info(f"⭐   Coordinator: {self._leader}")

        else:
            self._leader = self._id
            self._is_leader = True
            self.send_coordinator()
            self.listen_for_coordinator_oks()
            logging.info(f"🌟   Coordinator: {self._leader}")
            threading.Thread(
                target=self.check_on_controllers, args=(self._other_medics,)
            ).start()

        self._udp_sock.bind(("0.0.0.0", CONNECTION_PORT))
        
        while True:
            self.loop()

    def send_election(self):
        logging.info("Sending elections")
        for id in self._greater_medics:
            with self._connections_lock:
                if conn := self._connections.get(id):
                    # logging.info(f"Sending Election to: {id}")
                    send_bytes(conn, election_msg())

    def answer_to_elections(self, initiator: Optional[str]):
        if initiator:
            logging.info(f"Answering to elections initiated by {initiator}")
        else:
            logging.info("Answering to elections")

        sel = selectors.DefaultSelector()
        for id in self._smaller_medics:
            if id == initiator:
                continue
            with self._connections_lock:
                if conn := self._connections.get(id):
                    conn.setblocking(False)
                    sel.register(conn, selectors.EVENT_READ)

        elections_received = 1 if initiator is not None else 0

        while elections_received < len(self._smaller_medics):
            events = sel.select(timeout=ELECTION_TIMEOUT)

            if not events:
                break

            for key, _ in events:
                sock: socket.socket = key.fileobj  # type: ignore

                id = None
                with self._connections_lock:
                    for conn_id in self._connections:
                        if sock == self._connections[conn_id]:
                            id = conn_id
                            break

                received = recv_bytes(sock, INT_ENCODING_LENGTH)

                if id is None:
                    logging.info("Received message from unknown sender")
                    continue

                if is_type(received, Message.ELECTION):
                    elections_received += 1
                    send_bytes(sock, ok_msg())

                elif not received:
                    logging.error(
                        f"at answer_to_elections: Received null from {id}"
                    )
                    self.close_socket(sock)
                else:
                    logging.error(
                        f"at answer_to_elections: Unexpected message received from {id}: {received}"
                    )

        logging.info(
            f"{elections_received}/{len(self._smaller_medics)} Elections received"
        )

    def listen_for_coordinator_oks(self) -> Optional[str]:
        coord_oks_received = set()
        while len(coord_oks_received) < len(self._smaller_medics):
            sel = selectors.DefaultSelector()
            for id in self._smaller_medics:
                with self._connections_lock:
                    if conn := self._connections.get(id):
                        conn.setblocking(False)
                        sel.register(conn, selectors.EVENT_READ)

            events = sel.select(timeout=COORDINATOR_OK_TIMEOUT)

            if not events:
                break

            for key, _ in events:
                sock: socket.socket = key.fileobj  # type: ignore
                try:
                    received = recv_bytes(sock, INT_ENCODING_LENGTH)
                except Exception as e:
                    logging.error(f"at listen_for_coordinator_oks: {e}")
                    self.close_socket(sock)
                    continue

                if is_type(received, Message.COORDINATOR_OK):
                    with self._connections_lock:
                        for id in self._connections:
                            if self._connections[id] == sock:
                                coord_oks_received.add(id)
                                break
                if is_type(received, Message.ELECTION):
                    send_bytes(sock, ok_msg())
                    send_bytes(sock, coord_msg())
                elif not received:
                    logging.error(
                        f"at answer_to_elections: Received null from {id}"
                    )
                    self.close_socket(sock)
                else:
                    logging.error(
                        f"at listen_for_coordinator_oks: Unexpected message received from {id}: {received}"
                    )

        if len(coord_oks_received) < len(self._smaller_medics):
            logging.info(f"({len(coord_oks_received)}/{len(self._smaller_medics)}) COORDINATOR OKs received: {list(coord_oks_received)}")
        else:
            logging.info(f"({len(coord_oks_received)}/{len(self._smaller_medics)}) COORDINATOR OKs received")

    def listen_for_coordinator(self) -> Optional[str]:
        while True:
            sel = selectors.DefaultSelector()
            for id in self._greater_medics:
                with self._connections_lock:
                    if conn := self._connections.get(id):
                        conn.setblocking(False)
                        sel.register(conn, selectors.EVENT_READ)

            events = sel.select(timeout=COORDINATOR_TIMEOUT)
            if not events:
                logging.error("No coordinator received")
                return None

            for key, _ in events:
                sock: socket.socket = key.fileobj  # type: ignore
                try:
                    received = recv_bytes(sock, INT_ENCODING_LENGTH)
                except Exception as e:
                    logging.error(f"at listen_for_coordinator: {e}")
                    self.close_socket(sock)
                    continue
            
                if is_type(received, Message.COORDINATOR):
                    with self._connections_lock:
                        for id in self._connections:
                            if (
                                self._connections[id] == sock
                            ):  # and id in self._greater_medics:
                                logging.info(f"Received coordinator from {id}")
                                return id
                else:
                    logging.error(
                        f"at listen_for_coordinator: Unexpected message received from {id}: {received}"
                    )

    def send_coordinator(self):
        with self._connections_lock:
            for id in self._smaller_medics:
                if conn := self._connections.get(id):
                    # logging.info(f"Sent coord to: {id}")
                    send_bytes(conn, coord_msg())

    def listen_for_oks(self):
        sel = selectors.DefaultSelector()
        for id in self._greater_medics:
            with self._connections_lock:
                if conn := self._connections.get(id):
                    conn.setblocking(False)
                    sel.register(conn, selectors.EVENT_READ)

        oks_received = set()
        leader = None

        while len(oks_received) < len(self._greater_medics):
            events = sel.select(timeout=OK_TIMEOUT * 4)
            if not events:
                break

            for key, _ in events:
                sock: socket.socket = key.fileobj  # type: ignore

                try:
                    received = recv_bytes(sock, INT_ENCODING_LENGTH)
                except Exception as e:
                    logging.error(f"at answer_to_elections: {e}")
                    self.close_socket(sock)
                
                if is_type(received, Message.OK):
                    with self._connections_lock:
                        for id in self._connections:
                            if (
                                self._connections[id] == sock
                            ):  # and id in self._greater_medics:
                                oks_received.add(id)
                                break
                elif is_type(received, Message.COORDINATOR):
                    for id in self._connections:
                        if (
                            self._connections[id] == sock
                        ):  # and id in self._greater_medics:
                            leader = id
                            break
                elif not received:
                    logging.error(
                        f"at answer_to_elections: Received null from {id}"
                    )
                    self.close_socket(sock)
                else:
                    logging.error(
                        f"at listen_for_oks: Unexpected message received from {id}: {received}"
                    )

        logging.info(
            f"{len(oks_received)}/{len(self._greater_medics)} Oks received: {list(oks_received)}"
        )

        return leader, oks_received

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

    def greatest_id(self, medic_ids: list[str]) -> str:
        if not medic_ids:
            raise ValueError("No medics to choose from")

        number_to_id = dict()

        for id in medic_ids:
            number = id[len("medic") :]
            number_to_id[int(number)] = id

        max_key = max(number_to_id.keys())
        return number_to_id[max_key]

    def revive_controller(self, controller_id: str):
        logging.info(f"REVIVIENDO CONTROLADOR: {controller_id}")
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

        while True:
            if (time.time() - last_check) >= (HEALTHCHECK_TIMEOUT // 2):
                for id in controller_ids:
                    if not self._last_im_alive_timestamp.get(id):
                        logging.info(f"{id} id not here")
                        dead_controllers.add(id)
                        continue
                    elif (
                        time.time() - self._last_im_alive_timestamp[id]
                    ) >= HEALTHCHECK_TIMEOUT:
                        logging.info(f"{id} timed out")
                        dead_controllers.add(id)

                logging.info(f"Dead Controllers: {len(dead_controllers)}")

                for id in dead_controllers:
                    self.revive_controller(id)
                    self._last_im_alive_timestamp[id] = time.time() + REVIVE_TIME

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

    medic = Medic(config=config, controllers_to_check={})
    signal.signal(signal.SIGTERM, lambda *_: medic.shutdown())
    # "title_filter1": "title_filter1", "title_filter2": "title_filter2",
    #                                              "title_filter_proxy": "title_filter_proxy", "date_filter1": "date_filter1",
    #                                                "date_filter2": "date_filter2", "date_filter_proxy": "date_filter_proxy",
    #                                                "category_filter_proxy": "category_filter_proxy", "category_filter1": "category_filter1"}

    medic.start()


if __name__ == "__main__":
    main()
