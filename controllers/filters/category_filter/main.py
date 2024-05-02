from messaging.goutong import Goutong
from utils.config_loader import Configuration
import logging
import signal

from messaging.message import Message
from exceptions.shutting_down import ShuttingDown

FILTER_TYPE = "category_filter"
EOF_QUEUE = "category_filter_eof"
CONTROL_GROUP = "CONTROL"

CATEGORY_Q1 = "Computers"
CATEGORY_Q5 = "Fiction"

OUTPUT_Q1 = "results_queue"
OUTPUT_Q5 = "joiner_books_queue"

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
        "LOGGING_LEVEL": str,
        "ITEMS_PER_BATCH": int,
        "FILTER_NUMBER": int,
    }

    filter_config = Configuration.from_file(required, "config.ini")
    filter_config.update_from_env()
    filter_config.validate()

    config_logging(filter_config.get("LOGGING_LEVEL"))
    logging.info(filter_config)

    messaging = Goutong()

    # Set up queues
    control_queue_name = (
        FILTER_TYPE + str(filter_config.get("FILTER_NUMBER")) + "_control"
    )
    input_queue_name = FILTER_TYPE + str(filter_config.get("FILTER_NUMBER"))

    own_queues = [input_queue_name, control_queue_name, EOF_QUEUE]
    messaging.add_queues(*own_queues)
    output_queues = [OUTPUT_Q1, OUTPUT_Q5]
    messaging.add_queues(*output_queues)

    messaging.add_broadcast_group(CONTROL_GROUP, [control_queue_name])
    messaging.set_callback(control_queue_name, callback_control, ())
    messaging.set_callback(input_queue_name, callback_filter, (filter_config,))

    signal.signal(signal.SIGTERM, lambda sig, frame: sigterm_handler(messaging))

    # Start Listening
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
        "title": book.get("title"),
        "authors": book["authors"],
        "publisher": book["publisher"],
    }


def _columns_for_query5(book: dict) -> dict:
    return {
        "title": book.get("title"),
    }


def _send_batch_q1(messaging: Goutong, batch: list):
    data = list(map(_columns_for_query1, batch))
    msg = Message({"query": 1, "data": data})
    messaging.send_to_queue(OUTPUT_Q1, msg)
    logging.debug(f"Sent Data to: {OUTPUT_Q1}")


def _send_batch_q5(messaging: Goutong, batch: list):
    data = list(map(_columns_for_query5, batch))
    msg = Message({"query": [5], "data": data})
    messaging.send_to_queue(OUTPUT_Q5, msg)
    logging.debug(f"Sent Data to: {OUTPUT_Q5}")


def _send_EOF(messaging: Goutong, eof_queue: str):
    msg = Message({"EOF": True, "forward_to": [eof_queue]})
    messaging.send_to_queue(EOF_QUEUE, msg)
    logging.debug(f"Sent EOF to: {EOF_QUEUE}")


def callback_filter(messaging: Goutong, msg: Message, config: Configuration):
    # logging.debug(f"Received: {msg.marshal()}")

    if msg.has_key("EOF"):
        # Forward EOF and Keep Consuming
        if msg.get("query") == 1:
            _send_EOF(messaging, OUTPUT_Q1)
        elif msg.get("query") == 5:
            _send_EOF(messaging, OUTPUT_Q5)
        else:
            logging.info("ESTO ESMA SDIHDHDD:V")
        
        return

    query_id = msg.get("query")
    books = msg.get("data")
    batch_q1 = []
    batch_q5 = []

    for book in books:
        categories = book.get("categories")

        # Query 1 Flow
        if CATEGORY_Q1 in categories and query_id == 1:
            batch_q1.append(book)
            if len(batch_q1) >= config.get("ITEMS_PER_BATCH"):
                _send_batch_q1(messaging, batch_q1)
                batch_q1.clear()

        # Query 5 Flow
        if CATEGORY_Q5 in categories and query_id == 5:
            batch_q5.append(book)
            if len(batch_q5) >= config.get("ITEMS_PER_BATCH"):
                _send_batch_q5(messaging, batch_q5)
                batch_q5.clear()

    # Send Remaining
    if batch_q1:
        _send_batch_q1(messaging, batch_q1)

    if batch_q5:
        _send_batch_q5(messaging, batch_q5)
        logging.debug(f"MANDO A JOINER {batch_q5}")


if __name__ == "__main__":
    main()
