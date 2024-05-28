from typing import Any, Union
from src.messaging.goutong import Goutong
from src.messaging.message import Message
import logging
import signal
import json
import os


from src.utils.config_loader import Configuration
from src.exceptions.shutting_down import ShuttingDown
from src.data_access.data_access import DataAccess

from collections import defaultdict


shutting_down = False


class InvalidCacheState(Exception):
    def __init__(self):
        pass


class ReviewCounter:
    THRESHOLD = 500

    INPUT_QUEUE = "review_counter_queue"
    FILTER_TYPE = "review_counter"
    CONTROL_GROUP = "CONTROL"

    OUTPUT_QUEUE_PREFIX = "results_"

    DEBUG_FREQ = 500

    def __init__(self, items_per_batch: int, reviews: DataAccess):
        self.reviews = reviews
        self.shutting_down = False
        self.review_counts = defaultdict(int)
        self.titles_over_thresh = set()
        self.msg_received = 0

        self.items_per_batch = items_per_batch
        self.titles_in_last_msg = dict()

        self.q3_output_batch = []
        self.q3_output_batch_size = 0

        self.q4_output_batch = []
        self.q4_output_batch_size = 0

        self.sent_to_q3 = 0
        self.sent_to_q4 = 0

        self._init_messaging()

    def _init_messaging(self):
        self.messaging = Goutong()

        # Set up the queues
        control_queue_name = self.FILTER_TYPE + "_control"
        own_queues = [self.INPUT_QUEUE, control_queue_name]
        self.messaging.add_queues(*own_queues)

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
        msg = Message({"ShutDown": True})
        self.messaging.broadcast_to_group(self.CONTROL_GROUP, msg)
        self.messaging.close()
        raise ShuttingDown

    def callback_control(self, messaging: Goutong, msg: Message):
        if msg.has_key("ShutDown"):
            self.shutting_down = True
            raise ShuttingDown

    def _send_EOF(self, conn_id: int):

        msg = Message({"conn_id": conn_id, "queries": [3,4], "EOF": True})
        output_q3 = self.OUTPUT_QUEUE_PREFIX + str(conn_id)
        self.messaging.send_to_queue(output_q3, msg)
        logging.debug(f"Sent EOF to: {output_q3}")

    def _reset_state(self):
        self.reviews.clear()
        self.review_counts = defaultdict(int)
        self.titles_over_thresh = set()
        self.titles_in_last_msg = dict()
        self.q3_output_batch = []
        self.q3_output_batch_size = 0

        self.sent_to_q3 = 0
        self.sent_to_q4 = 0

    def callback_filter(
        self,
        messaging: Goutong,
        msg: Message,
    ):
        self.msg_received += 1

        conn_id = msg.get("conn_id")
        queries = msg.get("queries")

        if msg.has_key("EOF"):
            if self.q3_output_batch_size > 0:
                self._send_q3_batch(check_full=False, conn_id=conn_id)

            self._send_q4_batch(conn_id=conn_id)

            logging.info(f"EN TOTAL FUERON {self.reviews.cached_entries}")
            logging.info(f"MSGS RECIBIDOS {self.msg_received}")
            self._reset_state()
            self._send_EOF(conn_id)
            return

        msg_reviews = msg.get("data")
        for review in msg_reviews:
            title = review.get("title")
            score_to_sum = float(review.get("review/score"))
            self.reviews.add(title, [score_to_sum, 1.0])
            if title not in self.titles_in_last_msg:
                self.titles_in_last_msg.update({title: review.get("authors")})

        self._load_q3_batch()
        self._send_q3_batch(check_full=True, conn_id=conn_id)

    def _send_q4_batch(self, conn_id: int):
        to_sort = list(
            map(
                lambda title: {title: self.reviews.get(title).value[0] / self.reviews.get(title).value[1]},  # type: ignore
                self.titles_over_thresh,
            )
        )
        to_sort.sort(key=lambda x: list(x.values())[0], reverse=True)
        data = list(map(lambda x: {"title": list(x.keys())[0]}, to_sort[:10]))

        msg = Message({"conn_id": conn_id, "queries": [4], "data": data})
        output_queue = self.OUTPUT_QUEUE_PREFIX + str(conn_id)
        self.messaging.send_to_queue(output_queue, msg)

    def _send_q3_batch(self, check_full: bool, conn_id: int):

        while (self.q3_output_batch_size >= self.items_per_batch) or not check_full:
            data = self.q3_output_batch[: self.items_per_batch]
            self.q3_output_batch = self.q3_output_batch[self.items_per_batch :]
            self.q3_output_batch_size -= self.items_per_batch

            msg = Message({"conn_id": conn_id, "queries": [3], "data": data})
            output_queue = self.OUTPUT_QUEUE_PREFIX + str(conn_id)
            self.messaging.send_to_queue(output_queue, msg)
            check_full = True  # Avoid infinite loop

            # Used for debugging
            self.sent_to_q3 += len(data)
            if self.sent_to_q3 >= self.DEBUG_FREQ:
                logging.debug(f"{self.sent_to_q3} more titles sent to Q3")
                self.sent_to_q3 = 0

    def _load_q3_batch(self):
        for title, authors in self.titles_in_last_msg.items():
            entry: Union[DataAccess.DataEntry, None] = self.reviews.get(title)

            if entry is None:
                logging.error(f"Title not found: {title}")
                raise InvalidCacheState

            score_sum, n_reviews = entry.value
            if n_reviews >= self.THRESHOLD and title not in self.titles_over_thresh:
                self.q3_output_batch.append({"title": title, "authors": authors})
                logging.debug(
                    f"Title has exceeded threshold: {title} with {n_reviews} reviews."
                )
                self.titles_over_thresh.add(title)
                self.q3_output_batch_size += 1

        self.titles_in_last_msg.clear()


# Graceful Shutdown
def sigterm_handler(counter: ReviewCounter):
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
    required = {
        "N_PARTITIONS": int,
        "LOGGING_LEVEL": str,
        "CACHE_VACANTS": int,
        "ITEMS_PER_BATCH": int,
    }
    filter_config = Configuration.from_file(required, "config.ini")
    filter_config.update_from_env()
    filter_config.validate()

    config_logging(filter_config.get("LOGGING_LEVEL"))
    logging.info(filter_config)

    def sum_and_increment(
        old_value: list[float], new_value: list[float]
    ) -> list[float]:
        return [old_value[0] + new_value[0], old_value[1] + new_value[1]]

    reviews = DataAccess(
        str,
        list,
        filter_config.get("CACHE_VACANTS"),
        filter_config.get("N_PARTITIONS"),
        "q2_reviews",
        merge_function=sum_and_increment,
    )

    counter = ReviewCounter(
        items_per_batch=filter_config.get("ITEMS_PER_BATCH"), reviews=reviews
    )

    signal.signal(signal.SIGTERM, lambda sig, frame: sigterm_handler(counter))
    counter.listen()

    logging.info("Shutting Down.")


if __name__ == "__main__":
    main()
