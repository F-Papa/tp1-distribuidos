import selectors
from collections import defaultdict
import errno
import random
import signal
import socket
import time
import logging
from typing import Optional
import docker
from enum import Enum
import os
import logging
import threading
from src.utils.config_loader import Configuration


HEALTHCHECK_REQUEST_CODE = 1
HEALTHCHECK_RESPONSE_CODE = 2


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
    DEAD_LEADER = 10


def decode_int(bytes: bytes) -> int:
    return int.from_bytes(bytes, "big")


def is_type(data: bytes, msg_type: Message) -> bool:
    return decode_int(data[:INT_ENCODING_LENGTH]) == msg_type.value


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


def connected_msg() -> bytes:
    return Message.CONNECTED.value.to_bytes(INT_ENCODING_LENGTH, "big")


def election_msg() -> bytes:
    return Message.ELECTION.value.to_bytes(INT_ENCODING_LENGTH, "big")


def ok_msg() -> bytes:
    return Message.OK.value.to_bytes(INT_ENCODING_LENGTH, "big")


def coord_msg() -> bytes:
    return Message.COORDINATOR.value.to_bytes(INT_ENCODING_LENGTH, "big")


CONNECTION_PORT = 12345
INT_ENCODING_LENGTH = 1
CONNECTION_RETRIES = 5
RETRY_INTERVAL = 2

CONNECTION_TIMEOUT = 5
SETUP_TIMEOUT = 15
TIMEOUT = 4
OK_TIMEOUT = 4
COORDINATOR_TIMEOUT = 30
ELECTION_TIMEOUT = 4
RESOLUTION_APROX_TIMEOUT = 4

VOTING_DURATION = 8
LEADER_ANNOUNCEMENT_DURATION = 8
HEALTH_CHECK_INTERVAL_SECONDS = 15
VOTING_COOLDOWN = 6

MSG_REDUNDANCY = 1


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

        self._ok_received = set()
        self._ok_received_lock = threading.Lock()
        self._udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def shutdown(self):
        for _, conn in self._connections.items():
            conn.shutdown(socket.SHUT_RDWR)
            conn.close()

    def connect_to_other(self, id: str):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(CONNECTION_TIMEOUT)

        for _ in range(CONNECTION_RETRIES):
            try:
                # logging.info(f"Trying to connect to {id}...")
                sock.connect((id, CONNECTION_PORT))
                self.send_bytes(sock, connect_msg(self._medic_number))
                received = self.recv_bytes(sock, INT_ENCODING_LENGTH)
                if is_type(received, Message.CONNECTED):
                    # logging.info(f"Connected to {id}")
                    with self._connections_lock:
                        self._connections[id] = sock
                        return
                else:
                    logging.info(
                        f"at connect_to_other: Received unexpected message from {id}: {received}"
                    )
            except Exception as e:
                logging.error(f"at connect_to_other ({id}): {e}")
                pass

    def accept_connection(self, sock: socket.socket):
        # logging.info("Listening...")

        conn, addr = sock.accept()
        received = self.recv_bytes(conn, INT_ENCODING_LENGTH * 2)

        if is_type(received, Message.CONNECT):
            number = decode_int(received[INT_ENCODING_LENGTH:])
            id = f"medic{number}"
            self.send_bytes(conn, connected_msg())
            with self._connections_lock:
                self._connections[id] = conn
                # logging.info(f"Connected to {id}")
        else:
            logging.info(
                f"at accept_connection: Received unexpected message from {addr}: {received}"
            )


    def loop(self):
        while True:
            sel = selectors.DefaultSelector()
            with self._connections_lock:
                for stored_id in self._other_medics:
                    if conn := self._connections.get(stored_id):
                        sel.register(conn, selectors.EVENT_READ)
            
            
            sel.register(self._udp_sock, selectors.EVENT_READ)
            sel.register(self._listen_socket, selectors.EVENT_READ)
        
            events = sel.select()

            for key, _ in events:
                sock: socket.socket = key.fileobj # type: ignore

                if sock == self._listen_socket:
                    self.accept_connection(sock)
                    # Election?
                    return
                
                if sock == self._udp_sock:
                    received = sock.recv(2*INT_ENCODING_LENGTH)
                    if is_type(received, Message.HELLO):
                        number = decode_int(received[INT_ENCODING_LENGTH:])
                        id = f"medic{number}"
                        logging.info(f"📣   HELLO from {id}")
                        self.connect_to_other(id)
                        logging.info(f"📶  Connected to {id}")
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
                received = self.recv_bytes(sock, INT_ENCODING_LENGTH)
                if is_type(received, Message.COORDINATOR):
                    logging.info(f"⭐   COORDINATOR: {id}")
                    self._is_leader = False
                    self._leader = id
                elif is_type(received, Message.ELECTION):
                    self.send_bytes(sock, ok_msg())
                    self.send_election()
                    self.answer_to_elections(initiator=id)
                    leader, oks_received = self.listen_for_oks()
                    if leader:
                        self._leader = leader
                        self._is_leader = False
                        logging.info(f"⭐   Coordinator: {self._leader}")
                    elif oks_received:
                        self._leader = self.listen_for_coordinator()
                        self._is_leader = False
                        logging.info(f"⭐   Coordinator: {self._leader}")
                    else:
                        self._leader = self._id
                        self._is_leader = True
                        self.send_coordinator()
                        logging.info(f"🌟   Coordinator: {self._leader}")

    def send_hello(self):
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
        self._listen_socket.settimeout(SETUP_TIMEOUT)

        self.send_hello()

        time.sleep(3)
        setup_threads = []
        for id in self._greater_medics:
            thread = threading.Thread(target=self.connect_to_other, args=(id,))
            thread.start()
            setup_threads.append(thread)

        try:
            for _ in range(len(self._smaller_medics)):
                self.accept_connection(self._listen_socket)
        except socket.timeout:
            pass

        for thread in setup_threads:
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
            logging.info(f"⭐   Coordinator: {self._leader}")
        elif oks_received:
            self._leader = self.listen_for_coordinator()
            self._is_leader = False
            logging.info(f"⭐   Coordinator: {self._leader}")
        else:
            self._leader = self._id
            self._is_leader = True
            self.send_coordinator()
            logging.info(f"🌟   Coordinator: {self._leader}")

        self._udp_sock.bind(("0.0.0.0", CONNECTION_PORT))
        while True:
            self.loop()

    def send_election(self):
        logging.info("Sending elections")
        for id in self._greater_medics:
            with self._connections_lock:
                if conn := self._connections.get(id):
                    # logging.info(f"Sending Election to: {id}")
                    self.send_bytes(conn, election_msg())

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
                
                received = self.recv_bytes(sock, INT_ENCODING_LENGTH)
                
                if id is None:
                    logging.info("Received message from unknown sender")
                    continue

                if is_type(received, Message.ELECTION):
                    elections_received += 1
                    self.send_bytes(sock, ok_msg())

                elif len(received) == 0:
                    with self._connections_lock:
                        for id in self._connections:
                            if self._connections[id] == sock:
                                logging.error(f"at answer_to_elections: Received null from {id}")
                                sel.unregister(sock)
                                del self._connections[id]
                                break
                else:
                    logging.error(
                        f"at answer_to_elections: Unexpected message received from {id}: {received}"
                    )
                    
        logging.info(
            f"{elections_received}/{len(self._smaller_medics)} Elections received"
        )

    def listen_for_coordinator(self) -> Optional[str]:
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

            received = self.recv_bytes(sock, INT_ENCODING_LENGTH)
            if is_type(received, Message.COORDINATOR):
                with self._connections_lock:
                    for id in self._connections:
                        if self._connections[id] == sock:# and id in self._greater_medics:
                            logging.info(f"Received coordinator from {id}")
                            return id
            else:
                logging.error(
                    f"at listen_for_coordinator: Unexpected message received from {id}: {received}"
                )
                return None
    
    def send_coordinator(self):
        with self._connections_lock:
            for id in self._smaller_medics:
                if conn := self._connections.get(id):
                    logging.info(f"Sent coord to: {id}")
                    self.send_bytes(conn, coord_msg())

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
            events = sel.select(timeout=OK_TIMEOUT*4)
            if not events:
                break

            for key, _ in events:
                sock: socket.socket = key.fileobj  # type: ignore

                received = self.recv_bytes(sock, INT_ENCODING_LENGTH)
                if is_type(received, Message.OK):
                    with self._connections_lock:
                        for id in self._connections:
                            if self._connections[id] == sock: # and id in self._greater_medics:
                                oks_received.add(id)
                                break
                elif is_type(received, Message.COORDINATOR):
                    for id in self._connections:
                            if self._connections[id] == sock: # and id in self._greater_medics:
                                leader = id
                                break
                elif len(received) == 0:
                    with self._connections_lock:
                        for id in self._connections:
                            if self._connections[id] == sock:
                                logging.error(f"at answer_to_elections: Received null from {id}")
                                sel.unregister(sock)
                                del self._connections[id]
                                break   
                else:
                    logging.error(
                        f"at listen_for_oks: Unexpected message received from {id}: {received}"
                    )

        logging.info(
            f"{len(oks_received)}/{len(self._greater_medics)} Oks received: {list(oks_received)}"
        )

        return leader, oks_received

    def recv_bytes(self, sock: socket.socket, size: int):
        received = b""
        while len(received) < size:
            received += sock.recv(size - len(received))
            if len(received) == 0:
                return received
        return received

    def send_bytes(self, sock: socket.socket, data: bytes):
        bytes_sent = 0
        while bytes_sent < len(data):
            bytes_sent += sock.send(data[bytes_sent:])

    def greatest_id(self, medic_ids: list[str]) -> str:
        if not medic_ids:
            raise ValueError("No medics to choose from")

        number_to_id = dict()

        for id in medic_ids:
            number = id[len("medic") :]
            number_to_id[int(number)] = id

        max_key = max(number_to_id.keys())
        return number_to_id[max_key]

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
    # logging.info(config)

    medic = Medic(config=config, controllers_to_check={})
    signal.signal(signal.SIGTERM, lambda *_: medic.shutdown())
    # "title_filter1": "title_filter1", "title_filter2": "title_filter2",
    #                                              "title_filter_proxy": "title_filter_proxy", "date_filter1": "date_filter1",
    #                                                "date_filter2": "date_filter2", "date_filter_proxy": "date_filter_proxy",
    #                                                "category_filter_proxy": "category_filter_proxy", "category_filter1": "category_filter1"}

    medic.start()


if __name__ == "__main__":
    main()
