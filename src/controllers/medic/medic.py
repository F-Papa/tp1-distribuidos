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


CONTROL_PORT = 12347
SOCK_TIMEOUT_SECONDS = 2
VOTING_DURATION_SECONDS = 4
HEALTH_CHECK_INTERVAL_SECONDS = 15
MSG_REDUNDANCY = 3


def crash_maybe():
    import random

    if random.random() < 0.0001:
        logging.error("Crashing...")
        os._exit(1)


class Medic:
    CONTROLLER_TYPE = "medic"

    def shutdown(self):
        logging.info("SIGTERM received. Initiating Graceful Shutdown.")
        self._shutting_down = True

    def __init__(self, controllers_to_check: dict, config: Configuration):
        self._shutting_down = False
        self.number_of_medics = config.get("NUMBER_OF_MEDICS")
        self.medic_number = config.get("MEDIC_NUMBER")
        self.other_medics = {}
        for i in range(self.number_of_medics):
            if i + 1 == self.medic_number:
                continue
            self.other_medics.update({f"medic{i+1}": f"medic{i+1}"})

        self.other_controllers = controllers_to_check.copy()
        self.controllers_to_check = {}

        self.sequence_number = 0
        self.controller_id = f"{self.CONTROLLER_TYPE}{self.medic_number}"
        self._is_leader = False
        self._leader = None
        self.docker_client = docker.DockerClient(base_url="unix://var/run/docker.sock")
        self.OKS_received = []
        self.IM_ALIVE_received = {}

        self.IM_ALIVE_SYNCER_DICT = {}
        self.healthcheck_syncer = threading.Condition()

    def increase_seq_number(self):
        self.sequence_number += 1

    def is_leader(self):
        return self._is_leader

    def send_healthchecks(self):
        for _, address in self.controllers_to_check.items():
            message = f"{self.sequence_number},{self.controller_id},{Message.HEALTHCHECK.value}$"
            for _ in range(MSG_REDUNDANCY):
                crash_maybe()
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # UDP
                try:
                    sock.sendto(message.encode(), (address, CONTROL_PORT))
                except Exception as e:
                    logging.error(f"EXCEPTION: {e}")

        self.increase_seq_number()

    def set_other_leader(self, controller_id, address):
        self._leader = controller_id
        self._is_leader = False
        self.controllers_to_check = {controller_id: address}
        logging.info(f"Leader: {controller_id} at {address}")

    def set_self_leader(self):
        self._is_leader = True
        self._leader = None
        self.controllers_to_check = self.other_controllers.copy()
        self.controllers_to_check.update(self.other_medics)
        logging.info(f"Leader: {self.controller_id} at localhost")

    def check_controllers(self):
        self.send_healthchecks()
        with self.healthcheck_syncer:
            self.healthcheck_syncer.wait()
        self.handle_healthcheck_responses()

    def check_leader_medic(self):
        self.send_healthchecks()
        with self.healthcheck_syncer:
            self.healthcheck_syncer.wait()
        self.handle_healthcheck_responses()

    def handle_healthcheck_responses(self):
        for alias in self.controllers_to_check.keys():
            crash_maybe()
            if (
                alias in self.IM_ALIVE_received
                and time.time() - self.IM_ALIVE_received[alias]
                < HEALTH_CHECK_INTERVAL_SECONDS
            ):
                logging.info(f"Controller {alias} is alive")
            else:
                logging.info(f"Controller {alias} is dead")
                if self.is_leader():
                    self.revive_controller(alias)
                else:
                    self.elect_leader()

    def revive_controller(self, controller_id):
        logging.info(f"Reving controller: {controller_id}")
        container = self.docker_client.containers.get(controller_id)
        container.start()

    def send_healthcheck_response(self, address, controller_id):
        message = (
            f"{self.sequence_number},{self.controller_id},{Message.IM_ALIVE.value}$"
        )
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        logging.info(f"Sending IM ALIVE to: {address} ({controller_id})")

        for _ in range(MSG_REDUNDANCY):
            # logging.info(f"MADNANDO {message}")
            sock.sendto(message.encode(), (address, CONTROL_PORT))

    def elect_leader(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        for i in range(self.medic_number, self.number_of_medics):
            election_msg = (
                f"{self.sequence_number},{self.controller_id},{Message.ELECTION.value}$"
            )
            for _ in range(MSG_REDUNDANCY):
                crash_maybe()
                try:
                    sock.sendto(election_msg.encode(), (f"medic{i+1}", CONTROL_PORT))
                except Exception as e:
                    logging.error(f"Exception at sendto: {e}")

        time.sleep(VOTING_DURATION_SECONDS)

        if len(self.OKS_received) == 0:
            self.set_self_leader()
            for _ in range(self.number_of_medics):
                coordinator_msg = f"{self.sequence_number},{self.controller_id},{Message.COORDINATOR.value}$"
                for i in range(self.number_of_medics):
                    crash_maybe()
                    if i + 1 == self.medic_number:
                        continue
                    try:
                        for _ in range(MSG_REDUNDANCY):
                            sock.sendto(
                                coordinator_msg.encode(), (f"medic{i+1}", CONTROL_PORT)
                            )
                    except Exception as e:
                        logging.error(f"Exception at sendto: {e}")

            self.increase_seq_number()
        else:
            self.OKS_received.clear()

    def listen_thread(self):
        logging.info("Listening thread started")
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.settimeout(SOCK_TIMEOUT_SECONDS)
        sock.bind(("0.0.0.0", CONTROL_PORT))
        terminator_bytes = bytes("$", "utf-8")[0]

        while not self._shutting_down:
            data = b""
            while len(data) == 0 or data[-1] != terminator_bytes:
                try:
                    crash_maybe()
                    recieved, address = sock.recvfrom(1024)
                    data += recieved
                except socket.timeout:
                    with self.healthcheck_syncer:
                        self.healthcheck_syncer.notify()
                        break

            if not data:
                continue
            data = data.decode()
            _, controller_id, response_code = data[:-1].split(",")
            response_code = int(response_code)
            if response_code == Message.OK.value:
                self.OKS_received.append(controller_id)
            elif response_code == Message.ELECTION.value:
                ok_msg = (
                    f"{self.sequence_number},{self.controller_id},{Message.OK.value}$"
                )
                for _ in range(MSG_REDUNDANCY):
                    try:
                        crash_maybe()
                        sock.sendto(ok_msg.encode(), (controller_id, CONTROL_PORT))
                    except Exception as e:
                        logging.error(f"Exception at sendto: {e}")
            elif response_code == Message.COORDINATOR.value:
                self.set_other_leader(controller_id, address[0])
            elif response_code == Message.HEALTHCHECK.value:
                self.send_healthcheck_response(
                    address=address[0], controller_id=controller_id
                )
            elif response_code == Message.IM_ALIVE.value:
                self.IM_ALIVE_received[controller_id] = time.time()
                self.IM_ALIVE_SYNCER_DICT[controller_id] = True
                all_in = all(
                    controller_id in self.IM_ALIVE_SYNCER_DICT
                    for controller_id in self.controllers_to_check
                )
                crash_maybe()
                if all_in:
                    with self.healthcheck_syncer:
                        self.IM_ALIVE_SYNCER_DICT.clear()
                        self.healthcheck_syncer.notify()

    def start(self):
        threading.Thread(target=self.listen_thread, args=()).start()
        time.sleep(2)
        self.elect_leader()

        # # prueba
        # self.revive_controller("title_filter1")

        while not self._shutting_down:
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
        config=config,
        controllers_to_check={
            "title_filter_proxy": "title_filter_proxy",
            "title_filter1": "title_filter1",
            "title_filter2": "title_filter2",
            "date_filter_proxy": "date_filter_proxy",
            "date_filter1": "date_filter1",
            "date_filter2": "date_filter2",
            "category_filter_proxy": "category_filter_proxy",
            "category_filter1": "category_filter1",
            "review_counter_proxy": "review_counter_proxy",
            "review_counter1": "review_counter1",
            "decade_counter_proxy": "decade_counter_proxy",
            "decade_counter1": "decade_counter1",
            "decade_counter2": "decade_counter2",
            "decade_counter3": "decade_counter3",
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
            "sentiment_averager_proxy": "sentiment_averager_proxy",
            "sentiment_average_reducer1": "sentiment_average_reducer1",
            "sentiment_average_reducer2": "sentiment_average_reducer2",
        },
    )

    signal.signal(signal.SIGTERM, lambda sig, frame: medic.shutdown())
    medic.start()


if __name__ == "__main__":
    main()
