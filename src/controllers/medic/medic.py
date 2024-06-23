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
    DEAD_LEADER = 10


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
OK_TIMEOUT = 4
COORDINATOR_TIMEOUT = 30
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

        self._udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._udp_sock.bind(("0.0.0.0", CONNECTION_PORT))

        self._voting = False
        self._voting_condvar = threading.Condition()

    def shutdown(self):
        for _, conn in self._connections.items():
            conn.shutdown(socket.SHUT_RDWR)
            conn.close()

    def accept_conn_from_smaller_medics(self, elections_barriers: list[threading.Barrier]):
        tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_sock.bind(("0.0.0.0", CONNECTION_PORT))
        tcp_sock.listen(self._medic_number - 1)

        udp_sock = self._udp_sock
        for medic in self._smaller_medics:
            msg = (
                b""
                + Message.HELLO.value.to_bytes(CONNECTION_MSG_LENGTH, "big")
                + self._medic_number.to_bytes(CONNECTION_MSG_LENGTH, "big")
            )
            try:
                udp_sock.sendto(msg, (medic, CONNECTION_PORT))
                logging.info(f"Sent Hello to {medic}")
            except socket.gaierror:
                logging.error(f"Failed to send hello to {medic}")
                pass


        while True:
            conn, _ = tcp_sock.accept()
            received = self.recv_bytes(conn, CONNECTION_MSG_LENGTH * 2)

            if len(received) == 0:
                thread = threading.Thread(target=self.dummy_thread, args=(elections_barriers,))
                thread.start()
                self._threads[medic_id] = thread
                continue

            medic_number = decode_int(received[CONNECTION_MSG_LENGTH:])
            medic_id = f"medic{medic_number}"
            if is_type(received, Message.CONNECT):
                with self._voting_condvar:
                    if old_conn := self._connections.get(medic_id):
                        old_conn.shutdown(socket.SHUT_RDWR)
                        old_conn.close()
                    
                    self._voting_condvar.notify_all()

                if old_thread := self._threads.get(medic_id):
                    logging.info(f"Joining old thread: {medic_id}")
                    old_thread.join()
                    logging.info("Joined!")

                thread = threading.Thread(
                    target=self.handle_connection_lower_id,
                    args=(medic_id, conn, elections_barriers),
                )

                thread.start()
                self._threads[medic_id] = thread

    def dummy_thread(self, barriers: list[threading.Barrier]):
        barriers[0].wait()
        barriers[1].wait()
        barriers[2].wait()
        logging.info("Dummy thread finished")

    def handle_connection_lower_id(
        self,
        medic_id: str,
        sock: socket.socket,
        elections_barriers: list[threading.Barrier],
    ):
        # logging.info("Handling connection with " + medic_id)
        with self._connections_lock:
            self._connections[medic_id] = sock
        
        try:
            self.send_bytes(sock, connected_msg())
        except Exception as e:
            logging.error(f"Exception gh554: {e} en medic {medic_id}")
            with self._connections_lock:
                del self._connections[medic_id] 

            elections_barriers[0].wait()
            elections_barriers[1].wait()
            elections_barriers[2].wait()
            return
        # logging.info(f"Connected to {medic_id}")

        # ---------------------------------------------

        while True:
            logging.info(f"Started {medic_id}")
            try:
                sock.settimeout(TIMEOUT)
                received = self.recv_bytes(sock, CONNECTION_MSG_LENGTH)
                if len(received) == 0:
                    (f"received LEN CERO {medic_id}")
                    elections_barriers[0].wait()
                    elections_barriers[1].wait()
                    elections_barriers[2].wait()
                    return
                
                if is_type(received, Message.ELECTION):
                    logging.info(f"😱 Received Election from {medic_id}")
                    self._voting = True
                    logging.info(f"Sending Ok to {medic_id}")
                    self.send_bytes(sock, ok_msg())
                
                else:
                    logging.error(f"Sidra 4 quesos {medic_id}")
            except OSError as e:
                if e.errno == errno.EBADF or e.errno == errno.EBADFD:
                    logging.error("Maria se escapa")
                    with self._connections_lock:
                        del self._connections[medic_id] 
                    return

            except Exception as e:
                logging.error(f"Exception maria cebce ({medic_id}): {e}")
                elections_barriers[0].wait()
                elections_barriers[1].wait()
                elections_barriers[2].wait()
                return
        
            elections_barriers[0].wait()  # Wait for all OKs to be received or Timed out
            elections_barriers[1].wait()  # Wait for decision on leadership

            if self._is_leader:
                msg = coord_msg()
                logging.info(f"Sending Coordinator to {medic_id}")
                try:
                    self.send_bytes(sock, msg)
                except Exception as e:
                    logging.error("Exception gran hermano 2015: {e}")
                    with self._connections_lock:
                        del self._connections[medic_id] 
                    elections_barriers[2].wait()
                    return
            
            elections_barriers[2].wait()  # Wait for all COORDINATORS to be sent.

            with self._voting_condvar:
                self._voting_condvar.wait()

    def call_dead_leader(self):
        for other in self._other_medics:
            dead_leader = Message.DEAD_LEADER.value.to_bytes(CONNECTION_MSG_LENGTH, "big")
            self._udp_sock.sendto(dead_leader, (other, CONNECTION_PORT))

        logging.info("Calling dead leader")
        self._voting_condvar.notify_all()


    def macuca(self, elections_barriers):
        udp_sock = self._udp_sock

        while True:
            logging.info("Macuca Recevigin")
            received = udp_sock.recv(1024)
            code = decode_int(received[:CONNECTION_MSG_LENGTH])

            if code == Message.HELLO.value:
                medic_num = decode_int(received[CONNECTION_MSG_LENGTH:])
                medic_id = f"medic{medic_num}"
                logging.info(f"Macuca is Hello from {medic_id}")

                if not self._first_election_done:
                    logging.info("Ignoring HELLO")
                    continue

                if conn := self._connections.get(medic_id):
                    logging.info("Había connection")
                    try:
                        conn.shutdown(socket.SHUT_RDWR)
                        conn.close()
                        logging.info("La cerré")
                    except:
                        logging.info("No pude cerrarla")
                        pass
                    
                with self._voting_condvar:
                    logging.info("I notify all")
                    self._voting_condvar.notify_all()
                
                # if thread := self._threads[medic_id]:
                #     logging.info("Había thread")
                #     thread.join()
                #     logging.info("Lo Joineé")

                # thread = threading.Thread(
                #     target=self.handle_connection_greater_id,
                #     args=(
                #         medic_id,
                #         elections_barriers,
                #     ),
                # )
                # thread.start()
                # self._threads[medic_id] = thread

                for medic_id in self._other_medics:
                    if medic_id not in self._threads:
                        logging.info(f"Starting dummy for: {medic_id}")
                        dummy = threading.Thread(target=self.dummy_thread, args=(
                        elections_barriers,))
                        dummy.start()
                        self._threads[medic_id] = dummy

            elif code == Message.DEAD_LEADER.value:
                if self._voting:
                    pass
                    # logging.info("Ignoring DEAD_LEADER")
                else:
                    # logging.info("Starting election due to DEAD_LEADER")
                    time.sleep(3)
                    with self._voting_condvar:
                        logging.info("I notify due to DEAD LEADER")
                        self._voting_condvar.notify_all()

    def connect_to_greater_medics(self, elections_barriers: list[threading.Barrier]):
        for medic in self._greater_medics:
            thread = threading.Thread(
                target=self.handle_connection_greater_id,
                args=(medic, elections_barriers),
            )
            thread.start()
            self._threads[medic] = thread

    def handle_connection_greater_id(self, medic_id, elections_barriers: list[threading.Barrier]
    ):
        # try:
        while True:
            instant_retry = self.handle_connection_greater_id_aux(medic_id, elections_barriers)
            if not instant_retry:
                with self._voting_condvar:
                    self._voting_condvar.wait()

        logging.info(f"========== Aux returned: {medic_id} ==========")
        # except OSError as e:
        # logging.error(f"Error handling connection {medic_id}: {e}")
        sock.close()

    def handle_connection_greater_id_aux(
        self,
        medic_id: str,
        elections_barriers: list[threading.Barrier],
    ):
        logging.info(f"Nuevo thread para mas grande: {medic_id}")
        # Connect to medic via TCP and retry if failed
        # self.try_to_connect_to_medic(connected_id, sock)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        for i in range(CONNECTION_RETRIES):
            try:
                sock.connect((medic_id, CONNECTION_PORT))
                break
            except socket.gaierror:
                logging.error(f"Could not resolve: {medic_id}")
                elections_barriers[0].wait()  # Wait for all OKs to be received or Timed out
                elections_barriers[1].wait()  # Wait for decision on leadership
                elections_barriers[2].wait()  # Wait for all COORDINATORS to be sent.
                return False             
            except Exception as e:
                if i + 1 == CONNECTION_RETRIES:
                    logging.error(f"Exception masss! conneting to {medic_id}: {e}")
                    # logging.info("Waiting 0")
                    elections_barriers[0].wait()  # Wait for all OKs to be received or Timed out
                    # logging.info("Waiting 1")
                    elections_barriers[1].wait()  # Wait for decision on leadership
                    # logging.info("Waiting 2")
                    elections_barriers[2].wait()  # Wait for all COORDINATORS to be sent.
                    return False
                else:
                    time.sleep(RETRY_INTERVAL)

        # Send CONNECT
        try:
            self.send_bytes(sock, connect_msg(self._medic_number))
            received = self.recv_bytes(sock, CONNECTION_MSG_LENGTH)

            if len(received) == 0:
                logging.error(f"Received zero from: {medic_id}")   
                elections_barriers[0].wait()
                elections_barriers[1].wait()
                elections_barriers[2].wait()
                return False
                
        except Exception as e:
            logging.error(f"El doblete ({medic_id}): {e}")   
            elections_barriers[0].wait()
            elections_barriers[1].wait()
            elections_barriers[2].wait()
            return False

        if is_type(received, Message.CONNECTED):
            # logging.info(f"Connected to {connected_id}")
            with self._connections_lock:
                self._connections[medic_id] = sock

        first_iteration = True
        # Connection established ---------------------------------------------------------------
        while True:
            self._voting = True
            try:
                logging.info(f"Sending Election to {medic_id}")
                sock.settimeout(OK_TIMEOUT)
                self.send_bytes(sock, election_msg())
                received = self.recv_bytes(sock, CONNECTION_MSG_LENGTH)

                if len(received) == 0:
                    if first_iteration:
                        elections_barriers[0].wait()
                        elections_barriers[1].wait()
                        elections_barriers[2].wait()
                        logging.error("No answer received in first iteration, should not retry until next votation")
                        return False
                    else:
                        logging.error("No answer in subsequent iteration, should try to reconnect")
                        return True

                #received = self.recv_bytes(sock, CONNECTION_MSG_LENGTH)
            except socket.timeout:
                # logging.info("TIMEOUT DE MIRIAM")
                pass
            
            except OSError as e:
                if e.errno == errno.EBADF or e.errno == errno.EBADFD:
                    logging.info(f"Terreneitor {medic_id}: {e}")
                    with self._connections_lock:
                        del self._connections[medic_id]    
                    return True
                else:
                    logging.info(f"Delfin acua {medic_id}: {e}")

            except Exception as e:
                logging.info(f"Exception GENERICA RICOTERA {e}")
                with self._connections_lock:
                    del self._connections[medic_id]

                elections_barriers[0].wait()  # Wait for all OKs to be received or Timed out
                elections_barriers[1].wait()  # Wait for decision on leadership
                elections_barriers[2].wait()  # Wait for all COORDINATORS to be sent.
                return False

            if is_type(received, Message.OK):
                with self._ok_received_lock:
                    self._ok_received.add(medic_id)
            
            elections_barriers[0].wait()  # ALL THREADS SYNC TO WAIT FOR ALL OKs
            elections_barriers[1].wait()  # Wait for decision on leadership

            if not self._is_leader:
                with self._ok_received_lock:
                    if medic_id == self.greatest_id(list(self._ok_received)):
                        sock.settimeout(COORDINATOR_TIMEOUT)
                        try:
                            received = self.recv_bytes(sock, CONNECTION_MSG_LENGTH)
                            if len(received) == 0:
                                logging.error(f"RECIBI 0 ESPERANDO COORDINATOR de {medic_id}")
                                elections_barriers[2].wait()
                                return False
                        except socket.timeout:
                            logging.error(f"Timed out waiting for {medic_id}")
                        if is_type(received, Message.COORDINATOR):
                            logging.info(f"🤩 RECEIVED COORDINATOR FROM {medic_id}")
                            self._leader = medic_id
            
            elections_barriers[2].wait()  # Wait for coordinator to be sent if not leader

            first_iteration = False
            with self._voting_condvar:
                self._voting_condvar.wait()

    def start(self):
        elections_barriers = []
        for i in range(3):
            elections_barriers.append(threading.Barrier(self._number_of_medics))

        listen_thread = threading.Thread(
            target=self.accept_conn_from_smaller_medics, args=(elections_barriers,)
        )
        listen_thread.start()
        self.connect_to_greater_medics(elections_barriers)

        threading.Thread(target=self.macuca, args=(elections_barriers,)).start()

        while True:
            self._leader = None
            self._is_leader = False
            
            elections_barriers[0].wait()  # Wait for all OKs to be received or Timed out
            with self._ok_received_lock:
                if not self._ok_received:
                    logging.info(f"IM NOW THE LEADER 😎")
                    self._is_leader = True
                    self._leader = self._id
                else:
                    logging.info(f"Received OKS from: {self._ok_received}")

            elections_barriers[1].wait()  # Wait for decision on leadership


            elections_barriers[2].wait()  # Wait for coordinator to be sent if leader
            logging.info(f"Clearing OKS: {self._ok_received}")
            self._ok_received.clear()
            self._voting = False
            # logging.info("Election finished")
            self._first_election_done = True
            logging.info("Wait Condvar")
            with self._voting_condvar:
                self._voting_condvar.wait()

        map(lambda thread: thread.join(), self._threads.values())

        # Acá tenes a todos los threads joineados y las conexiones exitosas guardadas.

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
