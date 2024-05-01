from typing import Any, Union
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
    Q3_4_FILE = "q3_4_reviews.json"
    KEY_VALUE_SEPARATOR = "%%%"

    def __init__(self, cache_vacants: int) -> None:
        self.cache = defaultdict(list)
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

        first_author: str = list(self.cache.keys())[0]
        first_author_decades = list(self.cache.pop(first_author))

        file = self.Q3_4_FILE
        entry = f"{first_author}{self.KEY_VALUE_SEPARATOR}{json.dumps(first_author_decades)}\n"
        with open(file, "a") as f:
            f.write(entry)

        logging.info(f"SAVED ENTRU: {entry}")
        self.entries_in_files += 1
        self.cached_entries -= 1

    def _pop_from_disk(self, title: str) -> tuple[str, Union[list, None]]:
        temp_file_name = "temp.json"

        file_name = self.Q3_4_FILE
        value_from_disk: Union[list, None] = None
        with open(file_name, "r") as original_file, open(
            temp_file_name, "w"
        ) as temp_file:
            for line in original_file:
                title_in_file = line.split(self.KEY_VALUE_SEPARATOR)[0]
                if title_in_file != title:
                    temp_file.write(line)
                else:
                    aux = json.loads(
                        line.split(self.KEY_VALUE_SEPARATOR)[1]
                    )
                    value_from_disk = aux

        os.replace(temp_file_name, file_name)
        if value_from_disk is not None:
            self.entries_in_files -= 1
            logging.info("NO ES NONE")
        return (title, value_from_disk)

    def add(self, title: str, review: dict):
        # dbg_string = "Adding (%s) | Cache Avl.: %d" % (
        #     # author[0:10] + "...",
        #     title,
        #     self.cache_vacants - self.n_elements_in_cache(),
        # )
        # logging.debug(dbg_string)

        # Already cached, add the new reviews and return
        if title in self.cache.keys():
            self.cache[title].append(review)
            return

        review_in_disk = None
        # Could be in file
        if self.entries_in_files > 0:
            _, review_in_disk = self._pop_from_disk(title)

        # If it is in the disk, add the new review to the existing ones, otherwise create a new list
        title_reviews = review_in_disk if review_in_disk is not None else []
        title_reviews.append(review)

        # If the cache is full, write the oldest entry to disk
        if self.n_elements_in_cache() >= self.cache_vacants:
            logging.debug(f"Committing 1 entry to disk | CACHE_ENTRIES:{self.cached_entries} | ENTRIES_INF_FILE: {self.entries_in_files}")
            self._write_oldest_to_disk()

        # Add the author to the cache, whether it was in the disk or a new one
        self.cached_entries += 1
        self.cache[title].append(review)

    def get(self, title: str) -> Union[list, None]:
        if title in self.cache.keys():
            return self.cache[title]

        elif self.entries_in_files > 0:
            _, reviews = self._pop_from_disk(title)
            if reviews is not None:
                self.cache.update({title: reviews})
            return reviews
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
        # RESETEAR EL CACHE
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

            logging.info(f"EN TOTAL FUERON {self.reviews.cached_entries}")
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
                self.reviews.add(title, review)
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
            reviews = self.reviews.get(title)
            if reviews is None:
                raise InvalidCacheState
            else:
                pass

            if self.review_counts[title] >= self.THRESHOLD:
                #Query 3
                if title not in self.titles_over_thresh:
                    self.titles_over_thresh.add(title)
                    authors = reviews[0].get("authors")
                    self.q3_output_batch.append({"title": title, "authors": authors})
                    self.q3_output_batch_size += 1
            
                #Query 4
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
