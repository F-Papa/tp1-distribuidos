from typing import Any, Union
from messaging.goutong import Goutong
from messaging.message import Message
import logging
import signal
import json
import os


from utils.config_loader import Configuration
from exceptions.shutting_down import ShuttingDown
from data_access.data_access import DataAccess

from collections import defaultdict


shutting_down = False


class InvalidCacheState(Exception):
    def __init__(self):
        pass


# class ReviewCache:
#     FILE_PREFIX = "reviews"
#     FILE_SUFFIX = ".json"
#     N_PARTITIONS = 10
#     KEY_VALUE_SEPARATOR = "%%%"
#     DEBUG_FREQ = 2500

#     def __init__(self, cache_vacants: int) -> None:
#         self.cache = {}
#         self.cache_vacants = cache_vacants
#         self.cached_entries = 0
#         self.entries_in_files = 0

#         # create files from scratch
#         for i in range(self.N_PARTITIONS):
#             file = self.FILE_PREFIX + str(i) + self.FILE_SUFFIX
#             with open(file, "w") as f:
#                 pass

#     def n_elements_in_cache(self) -> int:
#         return self.cached_entries

#     def _write_oldest_to_disk(self):
#         if len(self.cache) == 0:
#             raise ValueError("Cache is empty")

#         title: str = list(self.cache.keys())[0]
#         first_title_reviews = list(self.cache.pop(title))

#         partition = hash(title) % self.N_PARTITIONS
#         file_name = self.FILE_PREFIX + str(partition) + self.FILE_SUFFIX

#         entry = f"{title}{self.KEY_VALUE_SEPARATOR}{json.dumps(first_title_reviews)}\n"
#         with open(file_name, "a") as f:
#             f.write(entry)

#         # logging.debug(f"Entry saved in file {file_name}")
#         self.entries_in_files += 1
#         self.cached_entries -= 1

#     def _pop_from_disk(self, title: str) -> list[float]:
#         temp_file_name = "temp.json"

#         partition = hash(title) % self.N_PARTITIONS
#         file_name = self.FILE_PREFIX + str(partition) + self.FILE_SUFFIX
#         value_from_disk = [0.0, 0.0]

#         with open(file_name, "r") as original_file, open(
#             temp_file_name, "w"
#         ) as temp_file:
#             for line in original_file:
#                 line_title, data = line.split(self.KEY_VALUE_SEPARATOR)
#                 if line_title != title:
#                     temp_file.write(line)
#                 else:
#                     value_from_disk = json.loads(data)
#                     self.entries_in_files -= 1

#         os.replace(temp_file_name, file_name)
#         return value_from_disk

#     def add(self, title: str, score: float):
#         # dbg_string = "Adding (%s) | Cache Avl.: %d" % (
#         #     # author[0:10] + "...",
#         #     title,
#         #     self.cache_vacants - self.n_elements_in_cache(),
#         # )
#         # logging.debug(dbg_string)
#         # Already cached, add the new reviews and return
#         if title in self.cache.keys():
#             current_score, current_count = self.cache[title]
#             self.cache[title] = [current_score + score, current_count + 1]
#             logging.debug(f"Updated title in cache: {title}")
#             return

#         # Could be in file
#         if self.entries_in_files > 0:
#             title_reviews = self._pop_from_disk(title)
#             title_reviews[0] += score
#             title_reviews[1] += 1
#         else:
#             title_reviews = [score, 1]

#         # If it is in the disk, add the new review to the existing ones, otherwise create a new list

#         # If the cache is full, write the oldest entry to disk
#         if self.n_elements_in_cache() >= self.cache_vacants:
#             if self.entries_in_files % self.DEBUG_FREQ == 0:
#                 logging.debug(
#                     f"Committing 1 entry to disk | CACHE_ENTRIES:{self.cached_entries} | ENTRIES_INF_FILE: {self.entries_in_files}"
#                 )
#             self._write_oldest_to_disk()

#         # Add the author to the cache, whether it was in the disk or a new one
#         self.cached_entries += 1
#         self.cache.update({title: title_reviews})

#     def get(self, title: str) -> Union[list[float], None]:
#         if title in self.cache.keys():
#             return self.cache[title]

#         elif self.entries_in_files > 0:
#             sum_and_count = self._pop_from_disk(title)
#             if sum_and_count is not None:
#                 self.cache.update({title: sum_and_count})
#             return sum_and_count
#         else:
#             return None


class ReviewCounter:
    THRESHOLD = 500

    INPUT_QUEUE = "review_counter_queue"
    FILTER_TYPE = "review_counter"
    CONTROL_GROUP = "CONTROL"

    OUTPUT_Q3 = "results_queue"
    OUTPUT_Q4 = "results_queue"

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
        self.messaging.add_queues(self.OUTPUT_Q3, self.OUTPUT_Q4)

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
        self.messaging.send_to_queue(self.OUTPUT_Q3, msg)
        self.messaging.send_to_queue(self.OUTPUT_Q4, msg)
        logging.debug(f"Sent EOF to: {self.OUTPUT_Q3} and {self.OUTPUT_Q4}")

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

        if msg.has_key("EOF"):
            if self.q3_output_batch_size > 0:
                self._send_q3_batch(check_full=False)

            self._send_q4_batch()

            logging.info(f"EN TOTAL FUERON {self.reviews.cached_entries}")
            logging.info(f"MSGS RECIBIDOS {self.msg_received}")
            self._reset_state()
            self._send_EOF()
            return

        msg_reviews = msg.get("data")
        for review in msg_reviews:
            title = review.get("title")
            score_to_sum = float(review.get("review/score"))
            self.reviews.add(title, [score_to_sum, 1.0])
            if title not in self.titles_in_last_msg:
                self.titles_in_last_msg.update({title: review.get("authors")})

        self._load_q3_batch()
        self._send_q3_batch(check_full=True)

    def _send_q4_batch(self):
        to_sort = list(
            map(
                lambda title: {title: self.reviews.get(title).value[0] / self.reviews.get(title).value[1]},  # type: ignore
                self.titles_over_thresh,
            )
        )
        to_sort.sort(key=lambda x: list(x.values())[0], reverse=True)
        data = list(map(lambda x: {"title": list(x.keys())[0]}, to_sort[:10]))

        msg = Message({"query": 4, "data": data})
        self.messaging.send_to_queue(self.OUTPUT_Q4, msg)

    def _send_q3_batch(self, check_full: bool):

        while (self.q3_output_batch_size >= self.items_per_batch) or not check_full:
            data = self.q3_output_batch[: self.items_per_batch]
            self.q3_output_batch = self.q3_output_batch[self.items_per_batch :]
            self.q3_output_batch_size -= self.items_per_batch

            msg = Message({"query": 3, "data": data})
            self.messaging.send_to_queue(self.OUTPUT_Q3, msg)
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
        sum_and_increment,
    )

    counter = ReviewCounter(
        items_per_batch=filter_config.get("ITEMS_PER_BATCH"), reviews=reviews
    )

    signal.signal(signal.SIGTERM, lambda sig, frame: sigterm_handler(counter))
    counter.listen()

    logging.info("Shutting Down.")


if __name__ == "__main__":
    main()
