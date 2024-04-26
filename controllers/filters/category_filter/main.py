from os import environ
import configparser
from messaging.goutong import Goutong
from typing import Any
import logging

from messaging.message import Message

FILTER_TYPE = "category_filter"
EOF_QUEUE = "category_filter_eof"


class FilterConfig:
    required = {
        "FILTER_NUMBER": int,
        "CATEGORY": str,
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
    config_logging(filter_config.get("LOGGING_LEVEL"))

    logging.info(filter_config)

    messaging = Goutong()
    input_queue_name = FILTER_TYPE + str(filter_config.get("FILTER_NUMBER"))
    messaging.add_queues(input_queue_name)
    messaging.set_callback(input_queue_name, callback_filter, (filter_config,))
    messaging.listen()


def _send_batch(messaging: Goutong, batch: list, route: list):
    msg_content = {"data": batch, "route": route}
    msg = Message(msg_content)
    messaging.send_to_queue(route[0], msg)
    logging.debug(f"Sent Data to: {route[0]}")


def _send_EOF(messaging: Goutong, route: list):
    msg = Message({"EOF": True, "route": route})
    messaging.send_to_queue(EOF_QUEUE, msg)
    logging.debug(f"Sent EOF to: {EOF_QUEUE}")


def callback_filter(messaging: Goutong, msg: Message, config: FilterConfig):
    # logging.debug(f"Received: {msg.marshal()}")

    route = msg.get("route")
    route.pop(0)

    if msg.has_key("EOF"):
        # Forward EOF and Keep Consuming
        _send_EOF(messaging, route)
        return

    books = msg.get("data")
    batch = []

    for book in books:
        categories = book.get("categories")
        if config.get("CATEGORY") in categories:
            if len(batch) < config.get("ITEMS_PER_BATCH"):
                batch.append(book)
            else:
                _send_batch(messaging, batch, route)
                batch = []
    if len(batch) > 0:
        _send_batch(messaging, batch, route)


if __name__ == "__main__":
    main()
