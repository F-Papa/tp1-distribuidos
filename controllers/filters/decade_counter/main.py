from messaging.goutong import Goutong
from messaging.message import Message
import logging
import signal

from utils.config_loader import Configuration
from exceptions.shutting_down import ShuttingDown

INPUT_QUEUE = "decade_counter_queue"
FILTER_TYPE = "decade_counter"
EOF_QUEUE = "results_queue"
CONTROL_GROUP = "CONTROL"

OUTPUT_Q2 = "results_queue"

shutting_down = False


# Graceful Shutdown
def sigterm_handler(messaging: Goutong):
    global shutting_down
    logging.info("SIGTERM received. Initiating Graceful Shutdown.")
    shutting_down = True
    msg = Message({"ShutDown": True})
    # messaging.broadcast_to_group(CONTROL_GROUP, msg)


def config_logging(level: str):

    level = getattr(logging, level)

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
    required = {
        "LOGGING_LEVEL": str,
    }
    filter_config = Configuration.from_file(required, "config.ini")
    filter_config.update_from_env()
    filter_config.validate()

    config_logging(filter_config.get("LOGGING_LEVEL"))
    logging.info(filter_config)

    messaging = Goutong()

    # Set up the queues
    control_queue_name = FILTER_TYPE + "_control"
    own_queues = [INPUT_QUEUE, control_queue_name, EOF_QUEUE]
    messaging.add_queues(*own_queues)
    messaging.add_queues(OUTPUT_Q2)

    messaging.add_broadcast_group(CONTROL_GROUP, [control_queue_name])
    messaging.set_callback(control_queue_name, callback_control, ())

    decades_per_author = {}
    messaging.set_callback(
        INPUT_QUEUE, callback_filter, (filter_config, decades_per_author)
    )

    signal.signal(signal.SIGTERM, lambda sig, frame: sigterm_handler(messaging))

    # Start listening
    if not shutting_down:
        try:
            messaging.listen()
        except ShuttingDown:
            logging.debug("Shutdown Message Received via Control Broadcast")

    messaging.close()
    logging.info("Shutting Down.")


def callback_control(messaging: Goutong, msg: Message):
    global shutting_down
    if msg.has_key("ShutDown"):
        shutting_down = True
        raise ShuttingDown


def _send_EOF(messaging: Goutong):
    msg = Message({"EOF": True})
    messaging.send_to_queue(EOF_QUEUE, msg)
    logging.debug(f"Sent EOF to: {EOF_QUEUE}")


def callback_filter(
    messaging: Goutong, msg: Message, config: Configuration, decades_per_author: dict
):
    # logging.debug(f"Received: {msg.marshal()}")

    if msg.has_key("EOF"):
        # Forward EOF and Keep Consuming
        _send_results_q2(messaging, decades_per_author)
        _send_EOF(messaging)
        return

    books = msg.get("data")
    logging.debug(f"Received {len(books)} books")
    for book in books:
        decade = book.get("decade")

        # Query 2 flow
        for author in book.get("authors"):
            if author not in decades_per_author.keys():
                decades_per_author[author] = set()
            decades_per_author[author].add(decade)

    logging.debug(f"Authors: {len(decades_per_author.keys())}")


def _send_results_q2(messaging: Goutong, batch: dict):
    valid_author = lambda author: repr(author) != "''" and not author.isspace()

    ten_decade_authors = list(
        filter(lambda author: (len(batch[author]) >= 10) and valid_author(author), batch.keys())
    )

    msg = Message({"query": 2, "data": ten_decade_authors})
    messaging.send_to_queue(OUTPUT_Q2, msg)
    logging.debug(f"Sent Data to: {OUTPUT_Q2}")


if __name__ == "__main__":
    main()
