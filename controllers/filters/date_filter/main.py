from os import environ
from messaging.goutong import Goutong
from messaging.message import Message
from typing import Any
import configparser
import logging

FILTER_TYPE = "date_filter"
OUTPUT_QUEUE = "category_filter_queue"
EOF_QUEUE = "date_filter_eof"


class FilterConfig:
    required = {
        "FILTER_NUMBER": int,
        "LOWER_BOUND": int,
        "UPPER_BOUND": int,
        "LOGGING_LEVEL": str,
        "ITEMS_PER_BATCH": int,
    }

    def __init__(self, config: dict):
        self.properties = {}
        for key, value_type in self.required.items():
            if key in config:
                self.properties[key] = value_type(config[key])

    def get(self, key) -> Any:
        value = self.properties.get(key)
        if not value:
            raise ValueError(f"Invalid property: {key}")
        return value

    def update(self, key, value):
        if key not in self.required:
            raise ValueError(f"Invalid property: {key}")

        value_type = self.required[key]
        self.properties[key] = value_type(value)

    def validate(self):
        for key, value_type in self.required.items():
            if not isinstance(self.properties.get(key), value_type):
                raise ValueError(f"Missing or invalid property: {key}")

    def update_from_env(self):
        for key in FilterConfig.required:
            value = environ.get(key)
            if value is not None:
                self.update(key, value)

    @classmethod
    def from_file(cls, path: str):
        config = configparser.ConfigParser()
        config.read(path)
        config_dict = {k.upper(): v for k, v in config["FILTER"].items()}
        return FilterConfig(config=config_dict)

    def __str__(self) -> str:
        formatted = ", ".join([f"{k}={v}" for k, v in self.properties.items()])
        return f"FilterConfig({formatted})"


def config_logging(level: str):
    # Filter logging
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Hide pika logs
    pika_logger = logging.getLogger("pika")
    pika_logger.setLevel(logging.ERROR)


def main():
    config_logging("DEBUG")
    logging.info("Loading Config...")
    filter_config = FilterConfig.from_file("config.ini")
    filter_config.update_from_env()
    filter_config.validate()
    # config_logging(filter_config.get("LOGGING_LEVEL"))

    logging.info(filter_config)

    messaging = Goutong()
    input_queue_name = FILTER_TYPE + str(filter_config.get("FILTER_NUMBER"))
    messaging.add_queues(input_queue_name, OUTPUT_QUEUE)
    messaging.set_callback(input_queue_name, callback_filter, (filter_config,))
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
        messaging.send_to_queue(EOF_QUEUE, msg)
        return

    books = msg.get("data")
    batch = []

    for book in books:
        year = book.get("year")
        lower_bound = config.get("LOWER_BOUND")
        upper_bound = config.get("UPPER_BOUND")
        if lower_bound <= year <= upper_bound:
            if len(batch) < config.get("ITEMS_PER_BATCH"):
                batch.append(book)
            else:
                _send_batch(messaging, batch)
                batch = []
    if len(batch) > 0:
        _send_batch(messaging, batch)


if __name__ == "__main__":
    main()
