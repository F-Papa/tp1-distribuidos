from typing import Any
from messaging.goutong import Goutong
from messaging.message import Message
import logging
import signal

from utils.config_loader import Configuration
from exceptions.shutting_down import ShuttingDown

INPUT_QUEUE = "review_counter_queue"
FILTER_TYPE = "review_counter"
EOF_QUEUE = "results_queue"  # ?
CONTROL_GROUP = "CONTROL"

OUTPUT_Q3 = "results_queue"
OUTPUT_Q4 = "rating_average_queue"

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
    own_queues = [INPUT_QUEUE, control_queue_name]
    messaging.add_queues(*own_queues)
    messaging.add_queues(OUTPUT_Q3, OUTPUT_Q4)

    messaging.add_broadcast_group(CONTROL_GROUP, [control_queue_name])
    messaging.set_callback(control_queue_name, callback_control, ())

    reviews_per_title = {}
    titles_already_sent_to_q3_output = set()
    messaging.set_callback(
        INPUT_QUEUE,
        callback_filter,
        (filter_config, reviews_per_title, titles_already_sent_to_q3_output),
    )

    signal.signal(signal.SIGTERM, lambda sig, frame: sigterm_handler(messaging))

    # Start listening
    if not shutting_down:
        try:
            logging.info("Listening for Messages")
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
    messaging: Goutong,
    msg: Message,
    config: Configuration,
    reviews_per_title: dict,
    titles_already_sent_to_q3_output: set,
):

    if msg.has_key("EOF"):
        # Forward EOF and Keep Consuming
        _send_reviews_over_500(
            messaging, reviews_per_title, titles_already_sent_to_q3_output
        )
        reviews_per_title = {}
        titles_already_sent_to_q3_output = set()
        _send_EOF(messaging)
        return

    msg_reviews = msg.get("data")
    for review in msg_reviews:
        title = review.get("title")
        if title not in reviews_per_title.keys():
            reviews_per_title[title] = []
        reviews_per_title[title].append(review)

    _send_reviews_over_500(
        messaging, reviews_per_title, titles_already_sent_to_q3_output
    )


def _send_reviews_over_500(
    messaging: Goutong, reviews_per_title: dict, titles_already_sent_to_q3_output: set
):
    for title, reviews in reviews_per_title.items():
        if len(reviews) >= 500 and title not in titles_already_sent_to_q3_output:
            authors = reviews[0].get("authors")
            _send_results_q3(messaging, title, authors)
            reviews_per_title[title] = []
            titles_already_sent_to_q3_output.add(title)


def _send_results_q3(messaging: Goutong, title: str, authors: Any):
    data = [{"title": title, "authors": authors}]
    msg = Message({"query": 3, "data": data})
    messaging.send_to_queue(OUTPUT_Q3, msg)
    logging.debug(f"Sent Data to: {OUTPUT_Q3}")


if __name__ == "__main__":
    main()
