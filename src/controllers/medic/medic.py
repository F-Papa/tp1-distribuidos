from collections import defaultdict
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


CONNECTION_PORT = 12345
CONNECTION_MSG_LENGTH = 1

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
        self._bigger_medics = {}
        self._smaller_medics = {}

        for i in range(1, self._number_of_medics + 1):
            if i > self._medic_number:
                self._bigger_medics[f"medic{i}"] = f"medic{i}"
            if i != self._medic_number:
                self._other_medics[f"medic{i}"] = f"medic{i}"
            if i < self._medic_number:
                self._smaller_medics[f"medic{i}"] = f"medic{i}"

        self.controllers_to_check = controllers_to_check.copy()
        self.controllers_to_check.update(self._other_medics)

        self._seq_num = 0
        self._id = f"{self.CONTROLLER_TYPE}{self._medic_number}"
        self._is_leader = False
        self._leader = None
        self.docker_client = docker.DockerClient(base_url="unix://var/run/docker.sock")
        self._connections_lock = threading.Lock()
        self._connections = {}

        # TODO
        self.alive_flags = [True, False]

    def accept_conn_from_smaller_medics(self, election_condvar: threading.Condition):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("0.0.0.0", CONNECTION_PORT))
        # sock.settimeout(
        #     CONNECTION_TIMEOUT * (self._number_of_medics - self._medic_number) * 2
        # )
        sock.listen(self._number_of_medics * 5)

        connections_accepted = 0
        sock.settimeout(SETUP_TIMEOUT)

        while True:
            if connections_accepted >= self._medic_number - 1:
                with election_condvar:
                    election_condvar.notify()
                logging.info("Notified")
            try:
                conn, addr = sock.accept()
            except socket.timeout:
                sock.settimeout(None)
                logging.info("Notified due to timeout")
                with election_condvar:
                    election_condvar.notify()
                continue

            received = conn.recv(CONNECTION_MSG_LENGTH * 2)
            if (
                int.from_bytes(received[:CONNECTION_MSG_LENGTH], "big")
                != Message.CONNECT.value
            ):
                logging.error(f"Unknown message from {addr}: ", received)
                continue

            medic_number = int.from_bytes(received[CONNECTION_MSG_LENGTH:], "big")

            msg = Message.CONNECTED.value.to_bytes(CONNECTION_MSG_LENGTH, "big")
            logging.info(f"Sending Connected to medic {medic_number}")
            self.send_bytes(conn, msg)

            connections_accepted += 1
            with self._connections_lock:
                self._connections[f"medic{medic_number}"] = conn

    def connect_to_geater_medics(self):
        for medic in self._bigger_medics:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # sock.settimeout(CONNECTION_TIMEOUT)
            try:
                start = time.time()
                sock.connect((medic, CONNECTION_PORT))
            except socket.gaierror:
                logging.info(f"Resolution failed after: {time.time() -  start}")
                continue
            msg_code = Message.CONNECT.value.to_bytes(CONNECTION_MSG_LENGTH, "big")
            medic_number = self._medic_number.to_bytes(CONNECTION_MSG_LENGTH, "big")
            msg = b"" + msg_code + medic_number

            logging.info(f"Sending Connect to {medic}")
            self.send_bytes(sock, msg)

            received = self.recv_bytes(sock, CONNECTION_MSG_LENGTH)

            if int.from_bytes(received, "big") == Message.CONNECTED.value:
                with self._connections_lock:
                    self._connections[medic] = sock
            else:
                logging.error(f"Unknown response from {medic}: {received}")

    def start(self):
        election_condvar = threading.Condition()
        listen_thread = threading.Thread(
            target=self.accept_conn_from_smaller_medics, args=(election_condvar,)
        )
        listen_thread.start()

        time.sleep(2)

        self.connect_to_geater_medics()

        while True:
            with election_condvar:
                election_condvar.wait()
            self.start_election()

        listen_thread.join()

    def start_election(self):
        oks_received = []

        # Send ELECTION Messages
        logging.info("MACAROONES")
        for medic in self._bigger_medics:
            msg = Message.ELECTION.value.to_bytes(CONNECTION_MSG_LENGTH, "big")
            with self._connections_lock:
                if conn := self._connections.get(medic):
                    logging.info(f"Sending Election to {medic}")
                    self.send_bytes(conn, msg)
                else:
                    continue

        logging.info("PISTACHOS")
        # Receive ELECTION and send OK Messages 
        for medic in self._smaller_medics:
            with self._connections_lock:
                if conn := self._connections.get(medic):
                    conn.settimeout(RESOLUTION_APROX_TIMEOUT*2*(self._number_of_medics-1)) # If all resolutions_fail for new node
                    try:
                        received = self.recv_bytes(conn, CONNECTION_MSG_LENGTH)
                    except socket.timeout:
                        logging.error(f"Timed out waitig for {medic}'s ELECTION")
                        continue

                    if int.from_bytes(received, "big") == Message.ELECTION.value:
                        logging.info(f"Sending Ok to {medic}")
                        ok_msg = Message.OK.value.to_bytes(CONNECTION_MSG_LENGTH, "big")
                        self.send_bytes(conn, ok_msg)
                    else:
                        logging.info(f"Received frula: {received}")

        # Receive OK Messages
        logging.info("SALMOMNELLA")
        for medic in self._bigger_medics:
            with self._connections_lock:
                if conn := self._connections.get(medic):
                    conn.settimeout(TIMEOUT)
                    try:
                        received = self.recv_bytes(conn, CONNECTION_MSG_LENGTH)
                    except socket.timeout:
                        logging.error(f"Timed out waiting for {medic}'s OK")
                        continue
                    if int.from_bytes(received, "big") == Message.OK.value:
                        oks_received.append(medic)

        #Send COORDINATOR Messages
        if oks_received:
            greatest_medic = self.greatest_id(oks_received)

            with self._connections_lock:
                conn = self._connections.get(greatest_medic)

            if conn:
                conn.settimeout(TIMEOUT)
                try:
                    received = self.recv_bytes(conn, CONNECTION_MSG_LENGTH)
                except socket.timeout:
                    logging.error(f"Timed out waiting for {greatest_medic}'s COORDINATOR")
                    return

                if int.from_bytes(received, "big") == Message.COORDINATOR.value:
                    logging.info(f"Received COORDINATOR: {greatest_medic}")
                    # UPDATEAR COORDI
        
        #Receive COORDINATOR Messages
        else:
            for medic in self._other_medics:
                msg = Message.COORDINATOR.value.to_bytes(CONNECTION_MSG_LENGTH, "big")
                with self._connections_lock:
                    if conn := self._connections.get(medic):
                        logging.info(f"Sending Coordinator to {medic}")
                        self.send_bytes(conn, msg)

    def greatest_id(self, medic_ids: list[str]) -> str:
        if not medic_ids:
            raise ValueError("No medics to choose from")

        number_to_id = dict()

        for id in medic_ids:
            number = id[len("medic") :]
            number_to_id[int(number)] = id

        max_key = max(number_to_id.keys())
        return number_to_id[max_key]

    def recv_bytes(self, sock: socket.socket, size: int):
        received = b""
        while len(received) < size:
            received += sock.recv(size - len(received))
        return received

    def send_bytes(self, sock: socket.socket, data: bytes):
        bytes_sent = 0
        while bytes_sent < len(data):
            bytes_sent += sock.send(data[bytes_sent:])


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

    # "title_filter1": "title_filter1", "title_filter2": "title_filter2",
    #                                              "title_filter_proxy": "title_filter_proxy", "date_filter1": "date_filter1",
    #                                                "date_filter2": "date_filter2", "date_filter_proxy": "date_filter_proxy",
    #                                                "category_filter_proxy": "category_filter_proxy", "category_filter1": "category_filter1"}

    medic.start()


if __name__ == "__main__":
    main()
