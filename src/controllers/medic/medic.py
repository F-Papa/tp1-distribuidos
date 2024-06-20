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
    ELECTION = 3
    OK = 4
    COORDINATOR = 5
    HEALTHCHECK = 6
    IM_ALIVE = 7


PORT_1 = 12347
PORT_2 = 12348



TIMEOUT = 2
VOTING_DURATION = 8
LEADER_ANNOUNCEMENT_DURATION = 8
HEALTH_CHECK_INTERVAL_SECONDS = 15
MSG_REDUNDANCY = 1


class Medic:
    CONTROLLER_TYPE = "medic"

    def __init__(self, controllers_to_check: dict, config: Configuration):
        # dict{nombre_controller: address_controller}
        self._number_of_medics = config.get("NUMBER_OF_MEDICS")
        self._medic_number = config.get("MEDIC_NUMBER")
        self._other_medics = {}
        self._bigger_medics = {}
        for i in range(1, self._number_of_medics + 1):
            if i > self._medic_number:
                self._bigger_medics[f"medic{i}"] = f"medic{i}"
            if i != self._medic_number:
                self._other_medics[f"medic{i}"] = f"medic{i}"
    
        self.other_controllers = controllers_to_check.copy()
        self.controllers_to_check = {}
        
        self._seq_num = 0
        self._id = f"{self.CONTROLLER_TYPE}{self._medic_number}"
        self._is_leader = False
        self._leader = None
        self.docker_client = docker.DockerClient(base_url="unix://var/run/docker.sock")

        self._sock_1: socket.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock_2: socket.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock_1.bind(("0.0.0.0", PORT_1))
        self._sock_2.bind(("0.0.0.0", PORT_2))

    def is_leader(self):
        return self._is_leader

    def hostname(self, receiver_id: str):
        return receiver_id

    def send_message(self, message: str, receiver_id: str, port: int, sock: socket.socket):
        logging.info(f"Sending to {receiver_id}: {message}")
        for _ in range(MSG_REDUNDANCY):
            sock.sendto(message.encode(), (self.hostname(receiver_id), port))

    def recv_message(self, sock: socket.socket) -> tuple[int, str, Message]:
        received, address = sock.recvfrom(1024)
        seq_num, sender_id, message_code = received.decode().split(",")
        return int(seq_num), sender_id, Message(int(message_code))

    # def send_healthchecks(self):
    #     for controller_id, address in self.controllers_to_check.items():
    #         logging.info(
    #             f"Medic {self.medic_number} checking {controller_id} at {address}"
    #         )
    #         message = f"{self._seq_num},{self._id},{Message.HEALTHCHECK.value}$"
    #         for _ in range(MSG_REDUNDANCY):
    #             sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) 
    #             try:
    #                 sock.sendto(message.encode(), (address, CONTROL_PORT))
    #             except Exception as e:

    #                 logging.error(f"EXCEPTION: {e}")

    #     self.increase_seq_number()

    def revive_controller(self, controller_name):
        logging.info(f"REVIVING CONTROLLER {controller_name}")
        container = self.docker_client.containers.get(controller_name)
        try:
            container.kill()
        except:
            pass
        container.start()

    # RECIBE "ELECTIONS" POR PORT 2, CONTESTA "OK" AL PORT 1
    def listen_thread(self):
        while True:
            (seq_num, sender_id, msg) = self.recv_message(self._sock_2)
            if msg == Message.ELECTION:
                ok = f"{self._seq_num},{self._id},{Message.OK.value}"
                self.send_message(ok, sender_id, PORT_1, self._sock_2)

    # MANDA "ELECTION" A PORT 2, ESPERA "OK" POR PORT 1
    def elect_leader(self):
        logging.info("Starting election")
        for bigger_medic in self._bigger_medics:
            election = f"{self._seq_num},{self._id},{Message.ELECTION.value}"
            self.send_message(election, bigger_medic, PORT_2, self._sock_1)
            self._seq_num += 1

        oks_received = set()
        end_of_voting = time.time() + VOTING_DURATION

        logging.info("Listening for OKs")

        while time.time() < end_of_voting:
            timeout = end_of_voting - time.time()
            self._sock_1.settimeout(timeout)
            try:
                (seq_num, sender_id, msg) = self.recv_message(self._sock_1)
                if msg == Message.OK:
                    oks_received.add(sender_id)
                elif msg == Message.COORDINATOR:
                    if sender_id in self._bigger_medics:
                        self.set_other_leader(sender_id)
                        return
                    else:
                        logging.error(f"Received coordinator from {sender_id}")
            
            except socket.timeout:
                logging.info(f"Done listening for OKS, received: {oks_received}")

        if oks_received:
            end_of_announcement = time.time() + LEADER_ANNOUNCEMENT_DURATION
            while time.time() < end_of_announcement:
                timeout = end_of_announcement - time.time()
                self._sock_1.settimeout(timeout) 
                (seq_num, sender_id, msg) = self.recv_message(self._sock_1)
                if msg == Message.COORDINATOR:
                    if sender_id in self._bigger_medics and sender_id in oks_received:
                        self.set_other_leader(sender_id)
                        return
                    else:
                        logging.error(f"Received coordinator from {sender_id} | Bigger: {sender_id in self._bigger_medics}, OK Received: {sender_id in oks_received}")
        else:
            self._leader = self._id
            self._is_leader = True
            self.announce_leader()

    def announce_leader(self):
        logging.info(f"I'm the leader ({self._id})")
        for medic in self._other_medics:
            coord = f"{self._seq_num},{self._id},{Message.COORDINATOR.value}"
            self.send_message(coord, medic, PORT_1, self._sock_1)

    def set_other_leader(self, other: str):
        self._leader = other
        logging.info(f"The leader is ({other})")

    def start(self):
        threading.Thread(target=self.listen_thread, args=()).start()
        time.sleep(2)
        self.elect_leader()

        #    THREAD 1: ESCUCHA ELECTIONS Y CONTESTA OKS
        #    THREAD 2: MANDA ELECTIONS ESPERA OKS




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

    medic = Medic(
        config=config, controllers_to_check={}
    )

# "title_filter1": "title_filter1", "title_filter2": "title_filter2",
#                                              "title_filter_proxy": "title_filter_proxy", "date_filter1": "date_filter1",
#                                                "date_filter2": "date_filter2", "date_filter_proxy": "date_filter_proxy",
#                                                "category_filter_proxy": "category_filter_proxy", "category_filter1": "category_filter1"}

    medic.start()


if __name__ == "__main__":
    main()
