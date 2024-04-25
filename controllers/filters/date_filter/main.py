from os import environ
from messaging.goutong import Goutong
from messaging.message import Message
from typing import Any
import configparser
import logging

INPUT_QUEUE = "date_filter_queue"
OUTPUT_QUEUE = "category_filter_queue"


class FilterConfig:
    required = ["LOWER_BOUND", "UPPER_BOUND", "LOGGING_LEVEL", "ITEMS_PER_BATCH"]

    def __init__(
        self,
        lower_bound: int,
        upper_bound: int,
        logging_level: str,
        items_per_batch: int,
    ):
        self.properties = {
            "LOWER_BOUND": lower_bound,
            "UPPER_BOUND": upper_bound,
            "LOGGING_LEVEL": logging_level,
            "ITEMS_PER_BATCH": items_per_batch,
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

    def update_from_env(self):
        for key in FilterConfig.required:
            value = environ.get(key)
            if value is not None:
                self.update(key, environ.get(key))

    @classmethod
    def from_file(cls, path: str):
        config = configparser.ConfigParser()
        config.read(path)
        return FilterConfig(
            lower_bound=int(config["FILTER"]["LOWER_BOUND"]),
            upper_bound=int(config["FILTER"]["UPPER_BOUND"]),
            logging_level=config["FILTER"]["LOGGING_LEVEL"],
            items_per_batch=int(config["FILTER"]["ITEMS_PER_BATCH"]),
        )

    def __str__(self) -> str:
        formatted = ", ".join([f"{k}={v}" for k, v in self.properties.items()])
        return f"FilterConfig({formatted})"


def config_logging(filter_config: FilterConfig):
    # Filter logging
    level = filter_config.get("LOGGING_LEVEL")
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Hide pika logs
    pika_logger = logging.getLogger("pika")
    pika_logger.setLevel(logging.ERROR)


def main():
    filter_config = FilterConfig.from_file("config.ini")
    filter_config.update_from_env()
    filter_config.validate()
    config_logging(filter_config)

    logging.info(filter_config)

    messaging = Goutong()
    messaging.add_queues(INPUT_QUEUE, OUTPUT_QUEUE)
    messaging.set_callback(INPUT_QUEUE, callback_filter, (filter_config,))
    messaging.listen()


def _send_batch(messaging: Goutong, batch: list):
    msg_content = {"data": batch}
    msg = Message(msg_content)
    messaging.send_to_queue(OUTPUT_QUEUE, msg)
    logging.debug(f"Passed: {msg.marshal()}")


def callback_filter(messaging: Goutong, msg: Message, config: FilterConfig):
    logging.debug(f"Received: {msg.marshal()}")

    # Forward EOF and Keep Consuming
    if msg.has_key("EOF"):
        messaging.send_to_queue(OUTPUT_QUEUE, msg)
        return

    books = msg.get("data")
    batch = []

    for book in books:
        year = book.get("year")
        if config.get("LOWER_BOUND") <= year <= config.get("UPPER_BOUND"):
            if len(batch) < config.get("ITEMS_PER_BATCH"):
                batch.append(book)
            else:
                _send_batch(messaging, batch)
                batch = []
    if len(batch) > 0:
        _send_batch(messaging, batch)


if __name__ == "__main__":
    main()
