from messaging.goutong import Goutong
from utils.config_loader import Configuration
import logging
import signal

from messaging.message import Message
from exceptions.shutting_down import ShuttingDown

FILTER_TYPE = "sentiment_analyzer"
CONTROL_GROUP = "CONTROL"

OUTPUT_QUEUE = "sentiment_average_reducer"

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
        "ITEMS_PER_BATCH": int,
    }

    filter_config = Configuration.from_file(required, "config.ini")
    filter_config.update_from_env()
    filter_config.validate()

    config_logging(filter_config.get("LOGGING_LEVEL"))
    logging.info(filter_config)

    messaging = Goutong()

    # Set up queues
    control_queue_name = "sentiment_analyzer_control"
    input_queue_name = FILTER_TYPE + "_queue"

    own_queues = [input_queue_name, control_queue_name]
    messaging.add_queues(*own_queues)
    messaging.add_queues(OUTPUT_QUEUE)

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
    # messaging.send_to_queue(OUTPUT_Q5, msg)
    logging.debug(f"Sent Data to: {OUTPUT_Q5}")


def _send_EOF(messaging: Goutong):
    msg = Message({"EOF": True, "forward_to": [OUTPUT_QUEUE]})
    messaging.send_to_queue(OUTPUT_QUEUE, msg)
    logging.debug(f"Sent EOF to: {OUTPUT_QUEUE}")


def callback_filter(messaging: Goutong, msg: Message, config: Configuration):
    # logging.debug(f"Received: {msg.marshal()}")

    if msg.has_key("EOF"):
        # Forward EOF and Keep Consuming
        _send_EOF(messaging)
        return

    query_id = msg.get("query")
    reviews = msg.get("data")
    output_batch = []

    for review in reviews:
        categories = review.get("categories")
        

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


if __name__ == "__main__":
    main()
