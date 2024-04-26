from messaging.goutong import Goutong
from messaging.message import Message
import logging

from utils.config_loader import Configuration

FILTER_TYPE = "date_filter"
EOF_QUEUE = "date_filter_eof"


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
    required = {
        "FILTER_NUMBER": int,
        "LOWER_BOUND": int,
        "UPPER_BOUND": int,
        "LOGGING_LEVEL": str,
        "ITEMS_PER_BATCH": int,
    }
    filter_config = Configuration.from_file(required, "config.ini")
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


def callback_filter(messaging: Goutong, msg: Message, config: Configuration):
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
        year = book.get("year")
        lower_bound = config.get("LOWER_BOUND")
        upper_bound = config.get("UPPER_BOUND")
        if lower_bound <= year <= upper_bound:
            if len(batch) < config.get("ITEMS_PER_BATCH"):
                batch.append(book)
            else:
                _send_batch(messaging, batch, route)
                batch = []
    if len(batch) > 0:
        _send_batch(messaging, batch, route)


if __name__ == "__main__":
    main()
