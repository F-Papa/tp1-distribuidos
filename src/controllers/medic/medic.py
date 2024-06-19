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


MSG_REDUNDANCY = 3
BYTES_SIZE = 2

PORT = 12347

VOTING_DURATION_SECONDS = 6
HEALTH_CHECK_INTERVAL_SECONDS = 30


def crash_maybe():
    import random

    if random.random() < 0.01:
        logging.error("Crashing...")
        os._exit(1)


class Medic:
    CONTROLLER_TYPE = "medic"

    def shutdown(self):
        logging.info("SIGTERM received. Initiating Graceful Shutdown.")
        self._shutting_down = True
        self._control_socket.close()

    def __init__(self, system_controllers: dict, config: Configuration):
        self._resolved_controllers = {}
        self._control_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._control_socket.bind(("0.0.0.0", PORT))
        self._system_controllers = system_controllers
        self._other_medics = {}
        self._bigger_medics = {}
        self._shutting_down = False
        self._number_of_medics = config.get("NUMBER_OF_MEDICS")
        self._medic_number = config.get("MEDIC_NUMBER")
        self._is_leader = False
        self._leader = None
        self._seq_num = 1
        self._id = self.CONTROLLER_TYPE + str(config.get("MEDIC_NUMBER"))
        self._ok_received = False
        self._leader_lock = threading.Lock()
        self._im_alive_lock = threading.Lock()
        self._im_alive_received = set()
        self._just_revived = set()
        self._own_heartbeat = time.time()

        for i in range(1, self._number_of_medics + 1):
            if i > self._medic_number:
                self._bigger_medics[f"medic{i}"] = f"medic{i}"
            if i != self._medic_number:
                self._other_medics[f"medic{i}"] = f"medic{i}"

        self._docker_client = docker.from_env()
        self._ongoing_election = False

    def hostname(self, controller_id: str) -> str:
        if controller_id in self._resolved_controllers:
            return self._resolved_controllers[controller_id]
        else:
            return controller_id

    def send_message(
        self, message: Message, recv_id: str, recv_port: int, sock: socket.socket
    ):
        message_str = f"{self._seq_num},{self._id},{message.value}"

        length = len(message_str) + BYTES_SIZE
        encoded_msg = length.to_bytes(BYTES_SIZE, byteorder="big")
        encoded_msg += message_str.encode()

        for _ in range(MSG_REDUNDANCY):
            bytes_sent = 0
            while bytes_sent < len(message_str):
                try:
                    bytes_sent += sock.sendto(
                        encoded_msg[bytes_sent:], (self.hostname(recv_id), recv_port)
                    )
                except socket.gaierror:
                    break

    def receive_message(self, sock: socket.socket) -> tuple[str, Message]:
        """Controller_id, message_code"""
        data = bytes()

        while len(data) < BYTES_SIZE:
            received, address = sock.recvfrom(BYTES_SIZE - len(data))
            data += received

        size_to_read = int.from_bytes(data, "big")
        data = bytes()

        while len(data) < size_to_read:
            crash_maybe()
            received, address = sock.recvfrom(size_to_read - len(data))
            data += received

        seq_num, controller_id, response_code = data[BYTES_SIZE:].decode().split(",")
        if controller_id not in self._resolved_controllers:
            self._resolved_controllers[controller_id] = address[0]

        seq_num = int(seq_num)
        response_code = int(response_code)

        return controller_id, Message(response_code)

    def handle_messages(self):
        while not self._shutting_down:
            sender_id, message_code = self.receive_message(self._control_socket)
            crash_maybe()

            if self._own_heartbeat + 2 * HEALTH_CHECK_INTERVAL_SECONDS < time.time():
                logging.error("💥   No heartbeat received in time")
                os._exit(1)

            if message_code == Message.ELECTION:
                if self._is_leader:
                    self.send_message(
                        Message.COORDINATOR, sender_id, PORT, self._control_socket
                    )
                else:
                    self.send_message(Message.OK, sender_id, PORT, self._control_socket)

            elif message_code == Message.OK:
                with self._leader_lock:
                    self._ok_received = True

            elif message_code == Message.COORDINATOR:
                logging.info(f"📣   LEADER: {sender_id} ({self.hostname(sender_id)})")
                with self._leader_lock:
                    self._is_leader = False
                    self._leader = sender_id

            elif message_code == Message.HEALTHCHECK:
                self.send_message(
                    Message.IM_ALIVE, sender_id, PORT, self._control_socket
                )

            elif message_code == Message.IM_ALIVE:
                with self._im_alive_lock:
                    self._im_alive_received.add(sender_id)

    def election(self):
        logging.info("🗳️   Starting election")
        for medic_id in self._bigger_medics:
            crash_maybe()
            self.send_message(Message.ELECTION, medic_id, PORT, self._control_socket)

        time.sleep(VOTING_DURATION_SECONDS)
        crash_maybe()

        with self._leader_lock:
            if not self._ok_received:
                self._is_leader = True
                self._leader = self._id
                self.announce_leader()
            self._ok_received = False

        time.sleep(2)

    def announce_leader(self):
        logging.info(f"📣   LEADER: {self._id} (localhost)")
        for medic_id in self._other_medics:
            crash_maybe()
            self.send_message(
                Message.COORDINATOR,
                medic_id,
                PORT,
                self._control_socket,
            )

    def revive_controller(self, controller_id):
        logging.info(f"🔄   Reving controller: {controller_id}")
        container = self._docker_client.containers.get(controller_id)
        try:
            container.kill()
        except:
            pass
        container.start()
        self._just_revived.add(controller_id)

    def check_on_other_controllers(self):
        to_check = list(self._other_medics.keys())

        with self._leader_lock:
            self._im_alive_received.clear()

        for medic_id in to_check:
            crash_maybe()
            self.send_message(
                Message.HEALTHCHECK, medic_id, PORT, self._control_socket  # type: ignore
            )

        time.sleep(HEALTH_CHECK_INTERVAL_SECONDS)

        with self._im_alive_lock:
            if len(self._im_alive_received) == len(to_check):
                logging.info("✅   All controllers are alive")

            for controller_id in to_check:
                crash_maybe()
                if controller_id not in self._im_alive_received:
                    if controller_id in self._just_revived:
                        self._just_revived.remove(controller_id)
                        continue
                    self.revive_controller(controller_id)

    def check_on_leader(self):
        leader = self._leader

        with self._im_alive_lock:
            self._im_alive_received.clear()

        self.send_message(
            Message.HEALTHCHECK, leader, PORT, self._control_socket  # type: ignore
        )

        crash_maybe()
        time.sleep(HEALTH_CHECK_INTERVAL_SECONDS)

        with self._im_alive_lock:
            if len(self._im_alive_received) == 0:
                logging.info(f"❌   Leader ({leader}) is dead")
                logging.info("Heartbeat 💓")
                self._own_heartbeat = time.time()
                self.election()
                if self._is_leader:
                    self.announce_leader()
                    self.revive_controller(leader)
            else:
                logging.info(f"✅   Leader ({leader}) is alive")

    def start(self):
        handle_msg_thread = threading.Thread(target=self.handle_messages, args=())
        handle_msg_thread.start()

        time.sleep(5)

        while not self._shutting_down and not self._leader and not self._is_leader:
            self.election()
            logging.info("Heartbeat 💓")
            self._own_heartbeat = time.time()

        if self._is_leader:
            logging.info("🚀   Starting leader loop")
            while not self._shutting_down:
                self.check_on_other_controllers()
                logging.info("Heartbeat 💓")
                self._own_heartbeat = time.time()
        else:
            logging.info("🩺   Starting follower loop")
            if self._leader is None:
                logging.error("💥   No leader to check on")
            while not self._shutting_down:
                self.check_on_leader()
                logging.info("Heartbeat 💓")
                self._own_heartbeat = time.time()

        handle_msg_thread.join()


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
        system_controllers={
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
