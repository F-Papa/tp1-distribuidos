from messaging.goutong import Goutong
from messaging.message import Message
import logging
import signal

from utils.config_loader import Configuration
from exceptions.shutting_down import ShuttingDown

FILTER_TYPE = "date_filter"
EOF_QUEUE = "date_filter_eof"
CONTROL_GROUP = "CONTROL"


UPPER_Q1 = 2023
LOWER_Q1 = 2000

UPPER_Q3_4 = 1999
LOWER_Q3_4 = 1990

OUTPUT_Q1 = "title_filter_queue"
OUTPUT_Q3_4 = "joiner_books_queue"

shutting_down = False


# Graceful Shutdown
def sigterm_handler(messaging: Goutong):
    global shutting_down
    logging.info("SIGTERM received. Initiating Graceful Shutdown.")
    shutting_down = True
    msg = Message({"ShutDown": True})
    messaging.broadcast_to_group(CONTROL_GROUP, msg)


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
        "FILTER_NUMBER": int,
        "LOGGING_LEVEL": str,
        "ITEMS_PER_BATCH": int,
    }
    filter_config = Configuration.from_file(required, "config.ini")
    filter_config.update_from_env()
    filter_config.validate()

    config_logging(filter_config.get("LOGGING_LEVEL"))
    logging.info(filter_config)

    messaging = Goutong()

    # Set up the queues
    control_queue_name = (
        FILTER_TYPE + str(filter_config.get("FILTER_NUMBER")) + "_control"
    )
    input_queue_name = FILTER_TYPE + str(filter_config.get("FILTER_NUMBER"))

    own_queues = [input_queue_name, control_queue_name, EOF_QUEUE]
    messaging.add_queues(*own_queues)
    output_queues = [OUTPUT_Q1, OUTPUT_Q3_4]
    messaging.add_queues(*output_queues)

    messaging.add_broadcast_group(CONTROL_GROUP, [control_queue_name])
    messaging.set_callback(control_queue_name, callback_control, ())
    messaging.set_callback(input_queue_name, callback_filter, (filter_config,))

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


def _columns_for_query1(book: dict) -> dict:
    return {
        "title": book["title"],
        "authors": book["authors"],
        "publisher": book["publisher"],
        "categories": book["categories"],
    }


def _columns_for_query3_4(book: dict) -> dict:
    return {
        "title": book["title"],
        "authors": book["authors"],
    }


def _send_batch_q1(messaging: Goutong, batch: list):
    data = list(map(_columns_for_query1, batch))
    msg = Message({"query": 1, "data": data})
    messaging.send_to_queue(OUTPUT_Q1, msg)
    # logging.debug(f"Sent Data to: {OUTPUT_Q1}")


def _send_batch_q3_4(messaging: Goutong, batch: list):
    data = list(map(_columns_for_query3_4, batch))
    msg = Message({"query": [3, 4], "data": data})
    messaging.send_to_queue(OUTPUT_Q3_4, msg)
    # logging.debug(f"Sent Data to: {OUTPUT_Q3_4}")


def _send_EOF(messaging: Goutong):
    msg = Message({"EOF": True, "forward_to": [OUTPUT_Q1, OUTPUT_Q3_4]})
    messaging.send_to_queue(EOF_QUEUE, msg)
    logging.debug(f"Sent EOF to: {EOF_QUEUE}")


def callback_filter(messaging: Goutong, msg: Message, config: Configuration):
    # logging.debug(f"Received: {msg.marshal()}")

    if msg.has_key("EOF"):
        # Forward EOF and Keep Consuming
        _send_EOF(messaging)
        return

    books = msg.get("data")
    batch_q1 = []
    batch_q3_4 = []

    for book in books:
        year = book.get("year")

        # Query 1 flow
        if LOWER_Q1 <= year <= UPPER_Q1:
            batch_q1.append(book)
            if len(batch_q1) >= config.get("ITEMS_PER_BATCH"):
                _send_batch_q1(messaging, batch_q1)
                batch_q1.clear()

        # Queries 3 and 4 flow
        if LOWER_Q3_4 <= year <= UPPER_Q3_4:
            batch_q3_4.append(book)
            if len(batch_q3_4) >= config.get("ITEMS_PER_BATCH"):
                _send_batch_q3_4(messaging, batch_q3_4)
                batch_q3_4.clear()

    # Send remaining
    if batch_q1:
        _send_batch_q1(messaging, batch_q1)
    if batch_q3_4:
        _send_batch_q3_4(messaging, batch_q3_4)


if __name__ == "__main__":
    main()
