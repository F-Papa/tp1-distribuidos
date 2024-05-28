from collections import defaultdict
from src.messaging.goutong import Goutong
from src.utils.config_loader import Configuration
import logging
import signal

from src.messaging.message import Message
from src.exceptions.shutting_down import ShuttingDown

FILTER_TYPE = "category_filter"
EOF_QUEUE = "category_filter_eof"
CONTROL_GROUP = "CONTROL"

CATEGORY_Q1 = "Computers"
CATEGORY_Q5 = "Fiction"

OUTPUT_Q1_PREFIX = "results_"
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
    output_queues = [OUTPUT_Q5]
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

def _send_batch_q1(messaging: Goutong, batch: list, connection_id: int):
    data = list(map(_columns_for_query1, batch))
    msg = Message({"conn_id": connection_id, "queries": [1], "data": data})
    output_queue = OUTPUT_Q1_PREFIX + str(connection_id)
    messaging.send_to_queue(output_queue, msg)
    logging.debug(f"Sent Data to: {output_queue}")


def _send_batch_q5(messaging: Goutong, batch: list, connection_id: int):
    data = list(map(_columns_for_query5, batch))
    msg = Message({"conn_id": connection_id, "queries": [5], "data": data})
    messaging.send_to_queue(OUTPUT_Q5, msg)
    logging.debug(f"Sent Data to: {OUTPUT_Q5}")


def _send_EOF(messaging: Goutong, forward_to: str, connection_id: int, queries: list):
    msg = Message({"conn_id": connection_id, "EOF": True, "forward_to": [forward_to], "queries": queries})
    messaging.send_to_queue(EOF_QUEUE, msg)
    logging.debug(f"Sent EOF to: {EOF_QUEUE}")


def callback_filter(messaging: Goutong, msg: Message, config: Configuration):
    # logging.debug(f"Received: {msg.marshal()}")
    connection_id = msg.get("conn_id")
    queries = msg.get("queries")
    
    if msg.has_key("EOF"):
        # Forward EOF and Keep Consuming
        if 1 in queries:
            output_queue = OUTPUT_Q1_PREFIX + str(connection_id)
            _send_EOF(messaging, output_queue, connection_id, [1])
        elif 5 in queries:
            _send_EOF(messaging, OUTPUT_Q5, connection_id, [5])
        return

    books = msg.get("data")
    batch_q1 = defaultdict(list)
    batch_q5 = defaultdict(list)

    for book in books:
        categories = book.get("categories")

        # Query 1 Flow
        if 1 in queries and CATEGORY_Q1 in categories:
            batch_q1[connection_id].append(book)
            if len(batch_q1[connection_id]) >= config.get("ITEMS_PER_BATCH"):
                _send_batch_q1(messaging, batch_q1[connection_id], connection_id)
                batch_q1[connection_id].clear()

        # Query 5 Flow
        if 5 in queries and CATEGORY_Q5 in categories:
            batch_q5[connection_id].append(book)
            if len(batch_q5[connection_id]) >= config.get("ITEMS_PER_BATCH"):
                _send_batch_q5(messaging, batch_q5[connection_id], connection_id)
                batch_q5[connection_id].clear()

    # Send Remaining
    if batch_q1[connection_id]:
        _send_batch_q1(messaging, batch_q1[connection_id], connection_id)

    if batch_q5[connection_id]:
        _send_batch_q5(messaging, batch_q5[connection_id], connection_id)


if __name__ == "__main__":
    main()
