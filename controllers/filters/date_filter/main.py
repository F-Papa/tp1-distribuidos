from os import environ
from messaging.goutong import Goutong
from messaging.message import Message
import json
import logging

INPUT_QUEUE = "date_filter_queue"
OUTPUT_QUEUE = "category_filter_queue"
DEFAULT_ITEMS_PER_BATCH = 50


class FilterConfig:
    def __init__(
        self,
        lower_bound: int,
        upper_bound: int,
        logging_level: str,
        items_per_batch: int,
    ):
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound
        self.logging_level = logging_level
        self.items_per_batch = items_per_batch


def get_config_from_env() -> FilterConfig:
    required = ["LOWER_BOUND", "UPPER_BOUND"]

    for key in required:
        if not environ.get(key):
            raise ValueError(f"Missing required environment variable: {key}")

    if not environ.get("LOGGING_LEVEL"):
        logging.warning("No logging level specified, defaulting to ERROR")

    if not environ.get("ITEMS_PER_BATCH"):
        logging.warning("No items per batch specified, defaulting to ")

    return FilterConfig(
        lower_bound=int(environ.get("LOWER_BOUND", 0)),
        upper_bound=int(environ.get("UPPER_BOUND", 0)),
        logging_level=environ.get("LOGGING_LEVEL", "ERROR"),
        items_per_batch=int(environ.get("ITEMS_PER_BATCH", DEFAULT_ITEMS_PER_BATCH)),
    )


def config_logging(filter_config: FilterConfig):
    # Filter logging
    level = getattr(logging, filter_config.logging_level)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Hide pika logs
    pika_logger = logging.getLogger("pika")
    pika_logger.setLevel(logging.ERROR)


def main():
    filter_config = get_config_from_env()
    config_logging(filter_config)
    logging.info("Filter is up and running!")

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
        if config.lower_bound <= year <= config.upper_bound:
            if len(batch) < config.items_per_batch:
                batch.append(book)
            else:
                _send_batch(messaging, batch)
                batch = []
    if len(batch) > 0:
        _send_batch(messaging, batch)


if __name__ == "__main__":
    main()
