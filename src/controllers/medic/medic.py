from collections import defaultdict
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


class Message(Enum):
    ELECTION = 3
    OK = 4
    COORDINATOR = 5
    HEALTHCHECK = 6
    IM_ALIVE = 7


CONTROL_PORT = 12347
HEALTHCHECK_RESPONSE_PORT = 12346
SOCK_TIMEOUT_SECONDS = 2
VOTING_DURATION_SECONDS = 6
HEALTH_CHECK_INTERVAL_SECONDS = 20
MSG_REDUNDANCY = 3


def crash_maybe():
    import random

    if random.random() < 0.00001:
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

        self.sequence_number = 1
        self.controller_id = f"{self.CONTROLLER_TYPE}{self.medic_number}"
        self._is_leader = False
        self._leader = None
        self.docker_client = docker.DockerClient(base_url="unix://var/run/docker.sock")
        self.OKS_received = []
        self.lock = threading.Lock()
        self.control_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.control_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.control_sock.bind(("0.0.0.0", CONTROL_PORT))
        self.healthcheck_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.healthcheck_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.healthcheck_sock.bind(("0.0.0.0", HEALTHCHECK_RESPONSE_PORT))
        self.healthcheck_syncer = threading.Condition()
        self.healthcheck_start = None
        self.just_revived_controllers = set()

    def increase_seq_number(self):
        self.sequence_number += 1

    def is_leader(self):
        return self._is_leader

    def send_healthchecks(self):
        self.healthcheck_start = time.time()
        for alias, address in self.controllers_to_check.items():
            message = f"{self.sequence_number},{self.controller_id},{Message.HEALTHCHECK.value}$"
            try:
                for _ in range(MSG_REDUNDANCY):
                    crash_maybe()
                    self.healthcheck_sock.sendto(
                        message.encode(), (address, CONTROL_PORT)
                    )
                    # logging.info(f"Sent healthcheck to {alias}, {address}")
            except socket.gaierror:
                pass
            except Exception as e:
                logging.error(
                    f"EXCEPTION at sendto sending HEALTHCHECK: ({type(e)}) {e}"
                )
                break
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
        received_healthchecks = self.listen_healthcheck_responses()

        # logging.info(f"Received healthchecks:")
        # for k, v in received_healthchecks.items():
        #     logging.info(f"{k}: {v}")
        controllers_to_revive = []
        for controller_id in self.controllers_to_check:
            # Skip if controller was just revived, give it some time
            if controller_id in self.just_revived_controllers:
                logging.info(f"Skipping {controller_id} as it was just revived")
                continue
            if controller_id not in received_healthchecks:
                controllers_to_revive.append(controller_id)
        self.just_revived_controllers.clear()

        if len(controllers_to_revive) == 0:
            logging.info("All controllers are healthy")
            return
        else:
            logging.info(f"Controllers not responding: {controllers_to_revive}")

        if self._is_leader:
            for controller_id in controllers_to_revive:
                self.revive_controller(controller_id)
                self.just_revived_controllers.add(controller_id)
        else:
            self.elect_leader()
            if self._is_leader:
                for controller_id in controllers_to_revive:
                    self.revive_controller(controller_id)

    def revive_controller(self, controller_id):
        logging.info(f"Reving controller: {controller_id}")
        container = self.docker_client.containers.get(controller_id)
        try:
            container.kill()
        except:
            pass
        container.start()

    def send_healthcheck_response(self, address, controller_id, seq_num):
        message = f"{seq_num},{self.controller_id},{Message.IM_ALIVE.value}$"
        # logging.info(f"Sending healthcheck response to {controller_id} at {address}")
        try:
            for _ in range(MSG_REDUNDANCY):
                self.control_sock.sendto(
                    message.encode(), (address, HEALTHCHECK_RESPONSE_PORT)
                )
        except socket.gaierror:
            pass
        except Exception as e:
            logging.error(f"EXCEPTION at sendto sending IM_ALIVE: ({type(e)}) {e}")

    def elect_leader(self):
        logging.info("Starting election")

        for i in range(self.medic_number, self.number_of_medics):

            election_msg = (
                f"{self.sequence_number},{self.controller_id},{Message.ELECTION.value}$"
            )
            try:
                for _ in range(MSG_REDUNDANCY):
                    crash_maybe()
                    self.control_sock.sendto(
                        election_msg.encode(),
                        (f"medic{i+1}", CONTROL_PORT),
                    )
            except socket.gaierror:
                continue
            except Exception as e:
                logging.error(f"Exception at sendto sending ELECTION: ({type(e)}) {e}")

        time.sleep(VOTING_DURATION_SECONDS)

        if len(self.OKS_received) == 0:
            self.set_self_leader()
            for _ in range(self.number_of_medics):
                coordinator_msg = f"{self.sequence_number},{self.controller_id},{Message.COORDINATOR.value}$"
                for i in range(0, self.medic_number):
                    crash_maybe()
                    if i + 1 == self.medic_number:
                        continue
                    try:
                        for _ in range(MSG_REDUNDANCY):
                            self.control_sock.sendto(
                                coordinator_msg.encode(), (f"medic{i+1}", CONTROL_PORT)
                            )
                    except socket.gaierror:
                        continue
                    except Exception as e:
                        logging.error(
                            f"Exception at sendto sending COORDINATOR: ({type(e)}) {e}"
                        )

            self.increase_seq_number()
        else:
            self.OKS_received.clear()

    def control_listen_thread(self):
        logging.info("Control Listening thread started")
        terminator_bytes = bytes("$", "utf-8")[0]
        while not self._shutting_down:
            data = b""
            while len(data) == 0 or data[-1] != terminator_bytes:
                crash_maybe()
                recieved, address = self.control_sock.recvfrom(1024)
                data += recieved

            if not data:
                continue
            data = data.decode()
            seq_num, controller_id, response_code = data[:-1].split(",")

            response_code = int(response_code)
            if response_code == Message.OK.value:
                self.OKS_received.append(controller_id)
            elif response_code == Message.HEALTHCHECK.value:
                self.send_healthcheck_response(
                    address=address[0], controller_id=controller_id, seq_num=seq_num
                )
            elif response_code == Message.ELECTION.value:
                ok_msg = (
                    f"{self.sequence_number},{self.controller_id},{Message.OK.value}$"
                )
                try:
                    for _ in range(MSG_REDUNDANCY):
                        crash_maybe()
                        self.control_sock.sendto(
                            ok_msg.encode(), (controller_id, CONTROL_PORT)
                        )
                except socket.gaierror:
                    pass
                except Exception as e:
                    logging.error(f"Exception at sendto sending OK: ({type(e)}) {e}")
            elif response_code == Message.COORDINATOR.value:
                # Dont set leader if already set
                if self._is_leader or self._leader != controller_id:
                    self.set_other_leader(controller_id, address[0])

    def listen_healthcheck_responses(self) -> dict:

        terminator_bytes = bytes("$", "utf-8")[0]
        healthcheck_round_start = time.time()
        new_timeout = HEALTH_CHECK_INTERVAL_SECONDS

        received_healthchecks = defaultdict(lambda: 0)

        while not self._shutting_down:
            new_timeout = max(
                HEALTH_CHECK_INTERVAL_SECONDS - (time.time() - healthcheck_round_start),
                1,
            )
            self.healthcheck_sock.settimeout(new_timeout)

            data = b""
            try:
                while len(data) == 0 or data[-1] != terminator_bytes:
                    crash_maybe()
                    recieved, address = self.healthcheck_sock.recvfrom(1024)
                    data += recieved
            except socket.timeout:
                break

            if not data:
                continue

            data = data.decode()
            seq_num, controller_id, response_code = data[:-1].split(",")
            seq_num = int(seq_num)

            response_code = int(response_code)
            if response_code == Message.IM_ALIVE.value:
                received_healthchecks[controller_id] += 1
            else:
                logging.error(f"Unexpected message received: {data}")

        return received_healthchecks

    def start(self):
        control_handle = threading.Thread(target=self.control_listen_thread, args=())
        control_handle.start()
        time.sleep(5)
        self.elect_leader()
        while not self._shutting_down:
            self.check_controllers()
            time.sleep(HEALTH_CHECK_INTERVAL_SECONDS)

        self.control_sock.close()
        control_handle.join()


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
            # "title_filter2": "title_filter2",
            # "date_filter_proxy": "date_filter_proxy",
            # "date_filter1": "date_filter1",
            # "date_filter2": "date_filter2",
            # "category_filter_proxy": "category_filter_proxy",
            # "category_filter1": "category_filter1",
            # "review_counter_proxy": "review_counter_proxy",
            # "review_counter1": "review_counter1",
            # "decade_counter_proxy": "decade_counter_proxy",
            # "decade_counter1": "decade_counter1",
            # "decade_counter2": "decade_counter2",
            # "decade_counter3": "decade_counter3",
            # "sentiment_analyzer_proxy": "sentiment_analyzer_proxy",
            # "sentiment_analyzer1": "sentiment_analyzer1",
            # "sentiment_analyzer2": "sentiment_analyzer2",
            # "sentiment_analyzer3": "sentiment_analyzer3",
            # "sentiment_analyzer4": "sentiment_analyzer4",
            # "sentiment_analyzer5": "sentiment_analyzer5",
            # "sentiment_analyzer6": "sentiment_analyzer6",
            # "review_joiner_proxy": "review_joiner_proxy",
            # "review_joiner1": "review_joiner1",
            # "review_joiner2": "review_joiner2",
            # "review_joiner3": "review_joiner3",
            # "sentiment_averager_proxy": "sentiment_averager_proxy",
            # "sentiment_averager1": "sentiment_averager1",
            # "sentiment_averager2": "sentiment_averager2",
        },
    )

    signal.signal(signal.SIGTERM, lambda sig, frame: medic.shutdown())
    medic.start()


if __name__ == "__main__":
    main()
