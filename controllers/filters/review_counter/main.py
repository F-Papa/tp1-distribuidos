from typing import Any
from messaging.goutong import Goutong
from messaging.message import Message
import logging
import signal
import json
import os

from utils.config_loader import Configuration
from exceptions.shutting_down import ShuttingDown

from collections import defaultdict


shutting_down = False


class InvalidCacheState(Exception):
    def __init__(self):
        pass

class ReviewCache:
    Q3_4_FILE = "books_q3_4.json"
    KEY_VALUE_SEPARATOR = "%%%"

    def __init__(self, cache_vacants: int) -> None:
        self.cache: dict[tuple[str, int], Any] = {}
        self.cache_vacants = cache_vacants
        self.cached_entries = 0
        self.entries_in_files = 0
        # create files from scratch
        with open(self.Q3_4_FILE, "w") as f:
            pass

    def n_elements_in_cache(self) -> int:
        return self.cached_entries

    def _write_oldest_to_disk(self):
        if len(self.cache) == 0:
            raise ValueError("Cache is empty")

        first_title_and_query: tuple[str, int] = list(self.cache.keys())[0]
        first_data = self.cache.pop(first_title_and_query)
        first_title, query = first_title_and_query

        file = self.Q3_4_FILE
        entry = f"{first_title}{self.KEY_VALUE_SEPARATOR}{json.dumps(first_data)}\n"
        with open(file, "a") as f:
            f.write(entry)

        self.entries_in_files += 1 
        self.cached_entries -= 1

    def _pop_from_disk(self, query: int, title: str) -> tuple[str, Any]:
        temp_file_name = "temp.json"

        file_name = self.Q3_4_FILE
        value_from_disk = None
        with open(file_name, "r") as original_file, open(
            temp_file_name, "w"
        ) as temp_file:
            for line in original_file:
                title_in_file = line.split(self.KEY_VALUE_SEPARATOR)[0]
                if title_in_file != title:
                    temp_file.write(line)
                else:
                    value_from_disk = json.loads(line.split(self.KEY_VALUE_SEPARATOR)[1])

        os.replace(temp_file_name, file_name)
        self.entries_in_files -= 1

        return (title, value_from_disk)

    def append(self, query: int, review: dict):
        key = (review["title"], query)
        value = review["review/score"]
        dbg_string = (
            "Adding review for (%s) | REVIEW: | Cache Avl.: %d" % (
                review["title"][0:10] + f"...(Q:{query})",
                self.cache_vacants - self.n_elements_in_cache(),

            )
        )
        logging.debug(dbg_string)
        
        
        if self.n_elements_in_cache() >= self.cache_vacants:
            logging.debug(f"Committing 1 entry to disk")
            self._write_oldest_to_disk()
        
        self.cached_entries += 1
        old_value = self.cache.get(key)
        if old_value is not None:
            new_value = old_value.append(value)
        else:
            raise InvalidCacheState


    def get(self, query: int, title: str):
        key = (title, query)
        if key in self.cache.keys():
            return self.cache[key]
        elif self.entries_in_files > 0:
            title, data = self._pop_from_disk(query, title)
            if data is not None:
                self.cache.update({(title, query): data})
            return data
        else:
            return None


class ReviewCounter:
    THRESHOLD = 500

    INPUT_QUEUE = "review_counter_queue"
    FILTER_TYPE = "review_counter"
    CONTROL_GROUP = "CONTROL"

    OUTPUT_Q3 = "results_queue"
    OUTPUT_Q4 = "rating_average_queue"

    def __init__(self, items_per_batch: int, cache_vacants: int):
        self.reviews = ReviewCache(cache_vacants)
        self.shutting_down = False
        self.reviews_per_title = defaultdict(list)
        self.review_counts = defaultdict(int)
        self.titles_over_thresh = set()
        
        self.items_per_batch = items_per_batch
        self.titles_in_last_msg = set()

        self.q3_output_batch = []
        self.q3_output_batch_size = 0

        self.q4_output_batch = []
        self.q4_output_batch_size = 0

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

        self.messaging.set_callback(
            self.INPUT_QUEUE,
            self.callback_filter
        )
    
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
        self.reviews_per_title = defaultdict(list)
        self.review_counts = defaultdict(int)
        self.titles_over_thresh = set()
        self.titles_in_last_msg = set()
        self.q3_output_batch = []
        self.q3_output_batch_size = 0
        self.q4_output_batch = []
        self.q4_output_batch_size = 0

    def callback_filter(
        self,
        messaging: Goutong,
        msg: Message,
    ):

        if msg.has_key("EOF"):
            if self.q3_output_batch_size > 0:
                msg = Message({"query": 3, "data": self.q3_output_batch})
                self.messaging.send_to_queue(self.OUTPUT_Q3, msg)

            if self.q4_output_batch_size > 0:
                msg = Message({"query": 4, "data": self.q4_output_batch})
                self.messaging.send_to_queue(self.OUTPUT_Q4, msg)

            self._reset_state()
            self._send_EOF()
            return

        msg_reviews = msg.get("data")
        for review in msg_reviews:
            title = review.get("title")
            # Ya llego a 500 revs
            if title in self.titles_over_thresh:
                # Solo Query 4
                self.q4_output_batch.append({"title": title, "review/score": review["review/score"]})
                self.q4_output_batch_size += 1
            
            # No llego a 500 revs
            else:
                self.reviews.append(4, review)
                self.review_counts[title] += 1
                self.titles_in_last_msg.add(title)
        
        self._load_data_if_thresh_met()
        self._send_batches_if_full()

    def _send_batches_if_full(self):
        while self.q3_output_batch_size >= self.items_per_batch:
            data = self.q3_output_batch[:self.items_per_batch]
            self.q3_output_batch = self.q3_output_batch[self.items_per_batch:]
            self.q3_output_batch_size -= self.items_per_batch
            msg = Message({"query": 3, "data": data})
            self.messaging.send_to_queue(self.OUTPUT_Q3, msg)
            logging.debug(f"MANDE A Q3: {data}")

        while self.q4_output_batch_size >= self.items_per_batch:
            data = self.q4_output_batch[:self.items_per_batch]
            self.q4_output_batch = self.q4_output_batch[self.items_per_batch:]
            self.q4_output_batch_size -= self.items_per_batch
            msg = Message({"query": 4, "data": data})
            self.messaging.send_to_queue(self.OUTPUT_Q4, msg)
            logging.debug(f"MANDE A Q4: {data}")

    def _load_data_if_thresh_met(
            self
    ):  
        for title in self.titles_in_last_msg:
            reviews = self.reviews.get(4, title)

            if self.review_counts[title] >= self.THRESHOLD:
                #Query 3
                if title not in self.titles_over_thresh:
                    self.titles_over_thresh.add(title)
                    if reviews is None:
                        raise InvalidCacheState
                    else:
                        authors = reviews[0].get("authors")
                        self.q3_output_batch.append({"title": title, "authors": authors})
                        self.q3_output_batch_size += 1
            
                #Query 4
                if reviews is None:
                    raise InvalidCacheState
                else:
                    for r in reviews:
                        self.q4_output_batch.append({"title": title, "review/score": r["review/score"]})
                        self.q4_output_batch_size += 1          

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
        "LOGGING_LEVEL": str,
        "CACHE_VACANTS": int,
        "ITEMS_PER_BATCH": int
    }
    filter_config = Configuration.from_file(required, "config.ini")
    filter_config.update_from_env()
    filter_config.validate()

    config_logging(filter_config.get("LOGGING_LEVEL"))
    logging.info(filter_config)

    counter = ReviewCounter(items_per_batch=filter_config.get("ITEMS_PER_BATCH"), cache_vacants=filter_config.get("CACHE_VACANTS"))
    signal.signal(signal.SIGTERM, lambda sig, frame: sigterm_handler(counter))
    counter.listen()

    logging.info("Shutting Down.")




if __name__ == "__main__":
    main()
