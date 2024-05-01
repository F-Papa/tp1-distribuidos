from typing import Any
from messaging.goutong import Goutong
from messaging.message import Message
import logging
import signal

from utils.config_loader import Configuration
from exceptions.shutting_down import ShuttingDown

from collections import defaultdict, Counter


shutting_down = False


class RatingReducer:

    INPUT_QUEUE = "rating_average_queue"
    FILTER_TYPE = "rating_average_reducer"
    EOF_QUEUE = "results_queue"  # ?
    CONTROL_GROUP = "CONTROL"

    OUTPUT_QUEUE = "results_queue"

    def __init__(self, items_per_batch: int):
        self.shutting_down = False
        self.items_per_batch = items_per_batch
        self.averages_per_title = defaultdict(lambda: {"average": 0, "count": 0})
        self.output_batch = []
        self.output_batch_size = 0

        self._init_messaging()

    def update_average(self, title, average):
        self.averages_per_title[title]["average"] = average

    def calculate_new_average(self, title, rating):
        old_count = self.averages_per_title[title]["count"]
        old_total = self.averages_per_title[title]["average"] * old_count
        new_count = old_count + 1
        new_total = old_total + rating
        new_average = new_total / new_count

        self.averages_per_title[title]["count"] = new_count
        if new_average > 5:
            logging.debug(
                f"rating: {rating} old_count: {old_count} old_average {self.averages_per_title[title]['average']} old_total {old_total} new_total {new_total} new_average {new_average}"
            )
        return new_average

    def _init_messaging(self):
        self.messaging = Goutong()

        # Set up the queues
        control_queue_name = self.FILTER_TYPE + "_control"
        own_queues = [self.INPUT_QUEUE, control_queue_name]
        self.messaging.add_queues(*own_queues)
        self.messaging.add_queues(self.OUTPUT_QUEUE)

        self.messaging.add_broadcast_group(self.CONTROL_GROUP, [control_queue_name])
        self.messaging.set_callback(control_queue_name, self.callback_control, ())

        self.messaging.set_callback(self.INPUT_QUEUE, self.callback_filter)

    def listen(self):
        try:
            self.messaging.listen()
        except ShuttingDown:
            logging.debug("Stopped Listening")

    def shutdown(self):
        logging.info("Initiating Graceful Shutdown")
        self.shutting_down = True
        self.messaging.close()
        raise ShuttingDown

    def callback_control(self, messaging: Goutong, msg: Message):
        if msg.has_key("ShutDown"):
            self.shutting_down = True
            raise ShuttingDown

    def _send_EOF(self):
        msg = Message({"EOF": True})
        self.messaging.send_to_queue(self.EOF_QUEUE, msg)
        logging.debug(f"Sent EOF to: {self.EOF_QUEUE}")

    def _reset_state(self):
        self.averages_per_title = defaultdict(lambda: {"average": 0, "count": 0})
        self.output_batch = []
        self.output_batch_size = 0

    def send_top_10_average_books(self):
        top_10_titles_and_averages = sorted(
            self.averages_per_title.items(), key=lambda x: x[1]["average"], reverse=True
        )[:10]
        data = list(map(lambda x: {"title": x[0]}, top_10_titles_and_averages))
        # top_10_titles = heapq.nlargest(10, self.averages_per_title, key=sel)
        msg = Message({"query": 4, "data": data})
        self.messaging.send_to_queue(self.OUTPUT_QUEUE, msg)
        logging.debug(f"MANDE {msg.marshal()}")

    def callback_filter(
        self,
        messaging: Goutong,
        msg: Message,
    ):

        if msg.has_key("EOF"):
            logging.debug(f"EMPIEZO A CALCULAR")
            self.send_top_10_average_books()
            self._reset_state()
            self._send_EOF()
            return

        msg_reviews = msg.get("data")
        for review in msg_reviews:
            title = review["title"]
            score = review["review/score"]
            new_average = self.calculate_new_average(title, float(score))
            self.update_average(title, new_average)


# Graceful Shutdown
def sigterm_handler(counter: RatingReducer):
    logging.info("SIGTERM received. Initiating Graceful Shutdown.")
    counter.shutdown()


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
    required = {"LOGGING_LEVEL": str, "ITEMS_PER_BATCH": int}
    filter_config = Configuration.from_file(required, "config.ini")
    filter_config.update_from_env()
    filter_config.validate()

    config_logging(filter_config.get("LOGGING_LEVEL"))
    logging.info(filter_config)

    reducer = RatingReducer(items_per_batch=filter_config.get("ITEMS_PER_BATCH"))
    signal.signal(signal.SIGTERM, lambda sig, frame: sigterm_handler(reducer))
    reducer.listen()

    logging.info("Shutting Down.")


if __name__ == "__main__":
    main()
