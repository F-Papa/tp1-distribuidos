from src.messaging.goutong import Goutong
from src.utils.config_loader import Configuration
import logging
import signal
from textblob import TextBlob

from src.messaging.message import Message
from src.exceptions.shutting_down import ShuttingDown


FILTER_TYPE = "sentiment_analyzer"
EOF_QUEUE = "sentiment_analyzer_eof"
CONTROL_GROUP = "CONTROL"

OUTPUT_QUEUE = "sentiment_average_queue"

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

    # Set up queues
    control_queue_name = (
        FILTER_TYPE + str(filter_config.get("FILTER_NUMBER")) + "_control"
    )
    input_queue_name = FILTER_TYPE + str(filter_config.get("FILTER_NUMBER"))

    own_queues = [input_queue_name, control_queue_name, EOF_QUEUE]
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


def _send_EOF(messaging: Goutong, conn_id: int):
    msg = Message({"conn_id": conn_id, "queries": [5], "EOF": True, "forward_to": [OUTPUT_QUEUE]})
    messaging.send_to_queue(EOF_QUEUE, msg)
    logging.debug(f"Sent EOF to: {EOF_QUEUE}")


def _analyze_sentiment(text: str):
    blob = TextBlob(text)
    sentiment = blob.sentiment.polarity # type: ignore
    return sentiment


def callback_filter(messaging: Goutong, msg: Message, config: Configuration):
    # logging.debug(f"Received: {msg.marshal()}")
    conn_id = msg.get("conn_id")
    queries = msg.get("queries")

    if msg.has_key("EOF"):
        # Forward EOF and Keep Consuming
        _send_EOF(messaging, conn_id)
        return

    reviews = msg.get("data")
    output_batch = []

    for review in reviews:
        review_text = review["review/text"]
        sentiment = _analyze_sentiment(review_text)

        output_batch.append({"title": review["title"], "sentiment": sentiment})
        if len(output_batch) >= config.get("ITEMS_PER_BATCH"):
            _send_batch(messaging, output_batch, conn_id)
            output_batch.clear()

    # Send Remaining
    if len(output_batch) > 0:
        _send_batch(messaging, output_batch, conn_id)


def _send_batch(messaging: Goutong, batch: list, conn_id: int):
    msg = Message({"conn_id": conn_id, "queries": [5], "data": batch})
    messaging.send_to_queue(OUTPUT_QUEUE, msg)
    logging.debug(f"Sent Data to: {OUTPUT_QUEUE}")


if __name__ == "__main__":
    main()
