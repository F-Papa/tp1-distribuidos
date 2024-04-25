from os import environ
from typing import Any
import logging

from messaging.message import Message
from messaging.goutong import Goutong


class BarrierConfig:
    required = ["FILTER_COUNT", "LOGGING_LEVEL"]

    def __init__(
        self,
        filter_count: int,
        logging_level: str,
        filter_type: str,
    ):
        self.properties = {
            "FILTER_COUNT": filter_count,
            "LOGGING_LEVEL": logging_level,
            "FILTER_TYPE": filter_type,
        }

    def get(self, key) -> Any:
        value = self.properties.get(key)
        if not value:
            raise ValueError(f"Invalid property: {key}")
        return value

    def update(self, key, value):
        if key not in self.properties:
            raise ValueError(f"Invalid property: {key}")
        self.properties[key] = value

    def validate(self):
        for k in self.required:
            if self.properties.get(k) is None:
                raise ValueError(f"Missing required property: {k}")

    @classmethod
    def from_env(cls):
        logging_level = environ.get("LOGGING_LEVEL")
        filter_count = environ.get("FILTER_COUNT")
        filter_type = environ.get("FILTER_TYPE")

        if logging_level is None or filter_count is None or filter_type is None:
            raise ValueError("Missing required environment variables")

        return BarrierConfig(
            filter_count=int(filter_count),
            logging_level=logging_level,
            filter_type=filter_type,
        )

    def __str__(self) -> str:
        formatted = ", ".join([f"{k}={v}" for k, v in self.properties.items()])
        return f"FilterConfig({formatted})"


class Barrier:
    def __init__(self, barrier_config: BarrierConfig, messaging: Goutong):
        self.barrier_config = barrier_config
        self.current_queue = 0
        self.eof_count = 0
        self.messaging = messaging

        self.eof_queue_name = barrier_config.get("FILTER_TYPE") + "_eof"
        self.input_queue_name = barrier_config.get("FILTER_TYPE") + "_queue"
        self.broadcast_group_name = barrier_config.get("FILTER_TYPE") + "_broadcast"

        self.filter_queues = []
        for i in range(1, barrier_config.get("FILTER_COUNT") + 1):
            queue_name = barrier_config.get("FILTER_TYPE") + str(i)
            self.filter_queues.append(queue_name)

        # Add queues and broadcast group
        self.messaging.add_queues(self.eof_queue_name)
        self.messaging.add_queues(self.input_queue_name)
        self.messaging.add_broadcast_group(
            self.broadcast_group_name, self.filter_queues
        )

        # Set callbacks
        self.messaging.set_callback(self.input_queue_name, self.distribute_data)
        self.messaging.set_callback(self.eof_queue_name, self.eof_received)

    def start(self):
        self.messaging.listen()

    def eof_received(self, _messaging: Goutong, msg: Message):
        self.eof_count += 1
        logging.debug(f"Received EOF number {self.eof_count}: {msg.marshal()}")
        if self.eof_count == self.barrier_config.get("FILTER_COUNT"):
            # Forward EOF
            logging.debug("Forwarding EOF")
            next_queue = "category_filter_queue"
            self.messaging.send_to_queue(next_queue, msg)
            self.eof_count = 0

    def increase_current_queue_index(self):
        self.current_queue = (self.current_queue + 1) % self.barrier_config.get(
            "FILTER_COUNT"
        )

    def distribute_data(self, _messaging: Goutong, msg: Message):
        logging.debug(f"Received: {msg.marshal()}")

        if msg.has_key("EOF"):
            self.messaging.broadcast_to_group(self.broadcast_group_name, msg)
            return

        # round-robin data
        self.messaging.send_to_queue(self.filter_queues[self.current_queue], msg)
        self.increase_current_queue_index()


def main():
    messaging = Goutong()

    barrier_config = BarrierConfig.from_env()
    barrier_config.validate()
    barrier = Barrier(barrier_config, messaging)
    logging.info(barrier_config)
    barrier.start()


if __name__ == "__main__":
    main()
