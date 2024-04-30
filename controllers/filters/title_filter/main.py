from messaging.goutong import Goutong
from exceptions.shutting_down import ShuttingDown
import logging
import signal

from messaging.message import Message
from utils.config_loader import Configuration
from exceptions.shutting_down import ShuttingDown

FILTER_TYPE = "title_filter"
EOF_QUEUE = "title_filter_eof"
CONTROL_GROUP = "CONTROL"

KEYWORD_Q1 = "distributed"
OUTPUT_Q1 = "category_filter_queue"

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

    # Set up queues
    control_queue_name = (
        FILTER_TYPE + str(filter_config.get("FILTER_NUMBER")) + "_control"
    )
    input_queue_name = FILTER_TYPE + str(filter_config.get("FILTER_NUMBER"))

    own_queues = [control_queue_name, input_queue_name, EOF_QUEUE]
    messaging.add_queues(*own_queues)
    output_queue = OUTPUT_Q1
    messaging.add_queues(output_queue)

    messaging.add_broadcast_group(CONTROL_GROUP, [control_queue_name])
    messaging.set_callback(input_queue_name, callback_filter, (filter_config,))
    messaging.set_callback(control_queue_name, callback_control, ())

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
        "categories": book["categories"],
    }


def _send_batch_q1(messaging: Goutong, batch: list):
    data = list(map(_columns_for_query1, batch))
    msg_content = {"query": 1, "data": batch}
    msg = Message(msg_content)
    messaging.send_to_queue(OUTPUT_Q1, msg)
    logging.debug(f"Sent Data to: {OUTPUT_Q1}")


def _send_EOF(messaging: Goutong):
    msg = Message({"EOF": True, "forward_to": [OUTPUT_Q1]})
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

    for book in books:
        title = book.get("title")

        # Query 1 (Only) Flow
        if KEYWORD_Q1.lower() in title.lower():
            batch_q1.append(book)
            if len(batch_q1) >= config.get("ITEMS_PER_BATCH"):
                _send_batch_q1(messaging, batch_q1)
                batch_q1.clear()
    
    # Send remaining items
    if batch_q1:
        _send_batch_q1(messaging, batch_q1)


if __name__ == "__main__":
    main()
