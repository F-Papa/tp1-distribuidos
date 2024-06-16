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


REQUEST_PORT = 12345
RESPONSE_PORT = 12346
CONTROL_PORT = 12347
TIMEOUT = 2
VOTING_DURATION = 8
HEALTH_CHECK_INTERVAL_SECONDS = 30
MSG_REDUNDANCY = 1


class Medic:
    CONTROLLER_TYPE = "medic"

    def __init__(self, controllers_to_check: dict, config: Configuration):
        # dict{nombre_controller: address_controller}
        self.controllers_to_check = controllers_to_check

        self.other_medics = {}
        self.number_of_medics = config.get("NUMBER_OF_MEDICS")
        self.medic_number = config.get("MEDIC_NUMBER")
        self.sequence_number = 0
        self.controller_id = f"{self.CONTROLLER_TYPE}{self.medic_number}"
        self._is_leader = False
        self._leader = None
        self.docker_client = docker.DockerClient(base_url="unix://var/run/docker.sock")
        self.OKS_received = []
        self.IM_ALIVE_received = {}

        for i in range(self.number_of_medics):
            if i + 1 == self.medic_number:
                continue
            self.other_medics.update({f"medic{i+1}": f"medic{i+1}"})

    def increase_seq_number(self):
        self.sequence_number += 1

    def is_leader(self):
        return self._is_leader

    def send_healthchecks(self, controllers_to_check: dict):
        for controller_id, address in controllers_to_check.items():
            logging.info(
                f"Medic {self.medic_number} checking {controller_id} at {address}"
            )
            message = f"{self.sequence_number},{self.controller_id},{Message.HEALTHCHECK.value}$"
            for _ in range(MSG_REDUNDANCY):
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # UDP
                sock.sendto(message.encode(), (address, CONTROL_PORT))

        self.increase_seq_number()

    def check_controllers(self):
        to_check = self.controllers_to_check.copy()
        to_check.update(self.other_medics)
        self.send_healthchecks(to_check)
        self.handle_healthcheck_responses(to_check)
        logging.info(f"Medic {self.medic_number} finished checking")

    def check_leader_medic(self):
        self.send_healthchecks({self._leader: self._leader})
        self.handle_healthcheck_responses({self._leader: self._leader})

    def handle_healthcheck_responses(self, controllers_to_check: dict):
        for alias, address in controllers_to_check.items():
            if (
                alias in self.IM_ALIVE_received
                and self.IM_ALIVE_received[alias] - time.time()
                < HEALTH_CHECK_INTERVAL_SECONDS
            ):
                logging.info(f"Controller {alias} is alive")
            else:
                logging.info(f"Controller {alias} is dead")

    def revive_controller(self, controller_name):
        logging.info(f"REVIVING CONTROLLER {controller_name}")
        container = self.docker_client.containers.get(controller_name)
        container.kill(signal.Signals.SIGTERM)
        container.start()

    def send_healthcheck_response(self, address):
        message = (
            f"{self.sequence_number},{self.controller_id},{Message.IM_ALIVE.value}$"
        )
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        logging.info(f"IM ALIVE to {address}")

        for _ in range(MSG_REDUNDANCY):
            sock.sendto(message.encode(), (address, CONTROL_PORT))

    def elect_leader(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        for i in range(self.medic_number, self.number_of_medics):
            election_msg = (
                f"{self.sequence_number},{self.controller_id},{Message.ELECTION.value}$"
            )
            for _ in range(MSG_REDUNDANCY):
                sock.sendto(election_msg.encode(), (f"medic{i+1}", CONTROL_PORT))

        time.sleep(VOTING_DURATION)

        if len(self.OKS_received) == 0:
            self._is_leader = True
            for _ in range(self.number_of_medics):
                coordinator_msg = f"{self.sequence_number},{self.controller_id},{Message.COORDINATOR.value}$"
                for i in range(self.number_of_medics):
                    if i + 1 == self.medic_number:
                        continue

                    sock.sendto(coordinator_msg.encode(), (f"medic{i+1}", CONTROL_PORT))

            self.increase_seq_number()
            logging.info(f"Leader: medic{self.medic_number}")
        else:
            self.OKS_received.clear()

    def listen_thread(self):
        logging.info("Listening thread started")
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("0.0.0.0", CONTROL_PORT))
        terminator_bytes = bytes("$", "utf-8")[0]

        while True:
            data = b""
            while len(data) == 0 or data[-1] != terminator_bytes:
                try:
                    recieved, _ = sock.recvfrom(1024)
                    data += recieved
                except socket.timeout:
                    break

            data = data.decode()
            _, controller_id, response_code = data[:-1].split(",")
            response_code = int(response_code)
            if response_code == Message.OK.value:
                self.OKS_received.append(controller_id)
            elif response_code == Message.ELECTION.value:
                election_msg = (
                    f"{self.sequence_number},{self.controller_id},{Message.OK.value}$"
                )
                for _ in range(MSG_REDUNDANCY):
                    sock.sendto(election_msg.encode(), (controller_id, CONTROL_PORT))
            elif response_code == Message.COORDINATOR.value:
                self._leader = controller_id
                self._is_leader = False
                logging.info(f"Leader: {controller_id}")
            elif response_code == Message.HEALTHCHECK.value:
                self.send_healthcheck_response(controller_id)
            elif response_code == Message.IM_ALIVE.value:
                self.IM_ALIVE_received[controller_id] = time.time()

    def start(self):
        threading.Thread(target=self.listen_thread, args=()).start()
        self.elect_leader()

        # # prueba
        # self.revive_controller("title_filter1")

        while True:
            if self.is_leader():
                self.check_controllers()
            else:
                self.check_leader_medic()
            time.sleep(HEALTH_CHECK_INTERVAL_SECONDS)


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
        config=config, controllers_to_check={}  # "title_filter1": "title_filter1"},
    )
    medic.start()


if __name__ == "__main__":
    main()
