from collections import defaultdict
import errno
import random
import signal
import socket
import time
import logging
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


def decode_int(bytes: bytes) -> int:
    return int.from_bytes(bytes, "big")


def is_type(data: bytes, msg_type: Message) -> bool:
    return decode_int(data[:CONNECTION_MSG_LENGTH]) == msg_type.value


def connect_msg(medic_number: int) -> bytes:
    return (
        b""
        + Message.CONNECT.value.to_bytes(CONNECTION_MSG_LENGTH, "big")
        + medic_number.to_bytes(CONNECTION_MSG_LENGTH, "big")
    )

def connected_msg() -> bytes:
    return Message.CONNECTED.value.to_bytes(CONNECTION_MSG_LENGTH, "big")


def election_msg() -> bytes:
    return Message.ELECTION.value.to_bytes(CONNECTION_MSG_LENGTH, "big")


def ok_msg() -> bytes:
    return Message.OK.value.to_bytes(CONNECTION_MSG_LENGTH, "big")


def coord_msg() -> bytes:
    return Message.COORDINATOR.value.to_bytes(CONNECTION_MSG_LENGTH, "big")


CONNECTION_PORT = 12345
CONNECTION_MSG_LENGTH = 1
CONNECTION_RETRIES = 5
RETRY_INTERVAL = 2

CONNECTION_TIMEOUT = 4
SETUP_TIMEOUT = 15
TIMEOUT = 4
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

        self._id = f"{self.CONTROLLER_TYPE}{self._medic_number}"
        self._is_leader = False
        self._leader = None
        self.docker_client = docker.DockerClient(base_url="unix://var/run/docker.sock")
        self._connections_lock = threading.Lock()
        self._connections = {}
        self._threads = {}

        self._ok_received = set()
        self._ok_received_lock = threading.Lock()

        self._voting = False
        self._voting_condvar = threading.Condition()

    def shutdown(self):
        for _, conn in self._connections.items():
            conn.shutdown(socket.SHUT_RDWR)
            conn.close()

    def accept_conn_from_smaller_medics(self, elections_barrier: threading.Barrier):
        udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        for medic in self._smaller_medics:
            msg = b"" + Message.HELLO.value.to_bytes(CONNECTION_MSG_LENGTH, "big") + self._medic_number.to_bytes(CONNECTION_MSG_LENGTH, "big") 
            logging.info(f"Sent Hello to {medic}")
            udp_sock.sendto(msg, (medic, CONNECTION_PORT))

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("0.0.0.0", CONNECTION_PORT))
        sock.listen(self._medic_number - 1)

        while True:
            conn, _ = sock.accept()
            received = self.recv_bytes(conn, CONNECTION_MSG_LENGTH * 2)
            medic_number = decode_int(received[CONNECTION_MSG_LENGTH:])
            medic_id = f"medic{medic_number}"

            if is_type(received, Message.CONNECT):
                with self._voting_condvar:
                    if old_conn := self._connections.get(medic_id):
                        old_conn.shutdown(socket.SHUT_RDWR)
                        old_conn.close()
                    self._voting_condvar.notify_all()

                if old_thread := self._threads.get(medic_id):
                    logging.info("Joining old thread")
                    old_thread.join()

                thread = threading.Thread(
                    target=self.handle_connection_lower_id, args=(medic_id, conn, elections_barrier)
                )

                thread.start()
                self._threads[medic_id] = thread

    def handle_connection_lower_id(self, medic_id: str, sock: socket.socket, elections_barriers: list[threading.Barrier]):
        logging.info("Handling connection with " + medic_id)
        with self._connections_lock:
            self._connections[medic_id] = sock

        self.send_bytes(sock, connected_msg())
        logging.info(f"Connected to {medic_id}")
        sock.settimeout(TIMEOUT)
        
        # ---------------------------------------------
        while True:
            try:
                received = self.recv_bytes(sock, CONNECTION_MSG_LENGTH)
                if len(received) == 0:
                    return
            except OSError as e:
                if e.errno == errno.EBADF or e.errno == errno.EBADFD:
                    return
            
            if is_type(received, Message.ELECTION):
                self._voting = True
                self.send_bytes(sock, ok_msg())

            elections_barriers[0].wait() # Wait for all OKs to be received or Timed out

            elections_barriers[1].wait() # Wait for decision on leadership
            
            if self._is_leader:
                msg = coord_msg()
                logging.info("Sending Coordinator")
                self.send_bytes(sock, msg)
            
            elections_barriers[2].wait() # Wait for all COORDINATORS to be sent.

            with self._voting_condvar:
                self._voting_condvar.wait()

    def macuca(self, elections_barriers):
        udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_sock.bind(("0.0.0.0", CONNECTION_PORT))

        while True:
            received = udp_sock.recv(1024)
            if decode_int(received[:CONNECTION_MSG_LENGTH]) == Message.HELLO.value:
                medic_num = decode_int(received[CONNECTION_MSG_LENGTH:])
                medic_id = f"medic{medic_num}"

                if not self._first_election_done:
                    logging.info("Ignoring HELLO")
                    continue
                if conn := self._connections.get(medic_id):
                    logging.info(f"Received HELLO from {medic_id}")
                    conn.shutdown(socket.SHUT_RDWR)
                    conn.close()
                    with self._voting_condvar:
                        self._voting_condvar.notify_all()
                    self._threads[medic_id].join()
                    thread = threading.Thread(target=self.handle_connection_greater_id, args=(medic_id, elections_barriers,))
                    thread.start()
                    self._threads[medic_id] = thread


    def connect_to_greater_medics(self, elections_barriers: list[threading.Barrier]):
        for medic in self._greater_medics:
            thread = threading.Thread(
                target=self.handle_connection_greater_id, args=(medic, elections_barriers)
            )
            thread.start()
            self._threads[medic] = thread

    def handle_connection_greater_id(self, medic_id: str, elections_barriers: list[threading.Barrier]):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # try:
        self.handle_connection_greater_id_aux(medic_id, sock, elections_barriers)
        # except OSError as e:
            # logging.error(f"Error handling connection {medic_id}: {e}")
        sock.close()

    def handle_connection_greater_id_aux(self, connected_id: str, sock: socket.socket, elections_barriers: list[threading.Barrier]):
        # Connect to medic via TCP and retry if failed
        #self.try_to_connect_to_medic(connected_id, sock)
        for i in range(CONNECTION_RETRIES):
            try:
                sock.connect((connected_id, CONNECTION_PORT))
                break
            except Exception as e:
                if i + 1 == CONNECTION_RETRIES:
                    logging.error(f"Exception conneting to {connected_id}: {e}")
                    # logging.info("Waiting 0") 
                    elections_barriers[0].wait() # Wait for all OKs to be received or Timed out
                    # logging.info("Waiting 1") 
                    elections_barriers[1].wait() # Wait for decision on leadership
                    # logging.info("Waiting 2") 
                    elections_barriers[2].wait() # Wait for all COORDINATORS to be sent.
                    return
                else:
                    time.sleep(RETRY_INTERVAL)

        # Send CONNECT
        self.send_bytes(sock, connect_msg(self._medic_number))
        
        received = self.recv_bytes(sock, CONNECTION_MSG_LENGTH)
        if is_type(received, Message.CONNECTED):
            logging.info(f"Connected to {connected_id}")
            with self._connections_lock:
                self._connections[connected_id] = sock
        
        # Connection established ---------------------------------------------------------------
        while True:
            self._voting = True
            try:
                self.send_bytes(sock, election_msg())
            except OSError as e:
                if e.errno == errno.EBADF or e.errno == errno.EBADFD:
                    return
                else:
                    raise e
                
            sock.settimeout(TIMEOUT)
            try:    
                received = self.recv_bytes(sock, CONNECTION_MSG_LENGTH)
            except socket.timeout:
                logging.info("TIMEOUT WAIT1")
            
            if is_type(received, Message.OK):
                with self._ok_received_lock:
                    self._ok_received.add(connected_id)
            
            # logging.info("Waiting 0") # Wait for all OKs to be received or Timed out
            elections_barriers[0].wait()   # ALL THREADS SYNC TO WAIT FOR ALL OKs

            # logging.info("Waiting 1") 
            elections_barriers[1].wait() # Wait for decision on leadership

            if not self._is_leader:
                with self._ok_received_lock:
                    logging.info(f"{self.greatest_id(list(self._ok_received))} == {connected_id}?")
                    if connected_id == self.greatest_id(list(self._ok_received)):
                        received = self.recv_bytes(sock, CONNECTION_MSG_LENGTH)
                        if is_type(received, Message.COORDINATOR):
                            logging.info(f"RECEIVED COORDINATOR FROM {connected_id}")
                            self._leader = connected_id
                            self._voting = False
                # logging.info("Waiting 2") 

            elections_barriers[2].wait() # Wait for coordinator to be sent if not leader
            
            with self._voting_condvar:
                self._voting_condvar.wait()

    def start(self):
        elections_barriers = []
        for i in range(3):
            elections_barriers.append(
                threading.Barrier(self._number_of_medics)
            )
        

        listen_thread = threading.Thread(
            target=self.accept_conn_from_smaller_medics, args=(elections_barriers,)
        )
        listen_thread.start()
        self.connect_to_greater_medics(elections_barriers)
        
        threading.Thread(target=self.macuca, args=(elections_barriers,)).start()

        while True:
            elections_barriers[0].wait() # Wait for all OKs to be received or Timed out
            with self._ok_received_lock:
                if not self._ok_received:
                    logging.info(f"IM NOW THE LEADER")
                    self._is_leader = True
                    self._leader = self._id
                    self._voting = False
            
            elections_barriers[1].wait() # Wait for decision on leadership        
            elections_barriers[2].wait() # Wait for coordinator to be sent if leader
            
            logging.info("Election finished")
            self._first_election_done = True
            with self._voting_condvar:
                self._voting_condvar.wait()

        map(lambda thread: thread.join(), self._threads.values())

        # Acá tenes a todos los threads joineados y las conexiones exitosas guardadas.
        

    def recv_bytes(self, sock: socket.socket, size: int):
        received = b""
        while len(received) < size:
            received += sock.recv(size - len(received))
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
