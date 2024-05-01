from typing import Any
from messaging.goutong import Goutong
from messaging.message import Message
import logging
import signal

from utils.config_loader import Configuration
from exceptions.shutting_down import ShuttingDown

from collections import defaultdict


shutting_down = False

class ReviewCounter:
    THRESHOLD = 500

    INPUT_QUEUE = "review_counter_queue"
    FILTER_TYPE = "review_counter"
    CONTROL_GROUP = "CONTROL"

    OUTPUT_Q3 = "results_queue"
    OUTPUT_Q4 = "rating_average_queue"

    def __init__(self, items_per_batch: int):
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
                self.reviews_per_title[title].append(review) #Enorme
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
            reviews = self.reviews_per_title[title]

            if self.review_counts[title] >= self.THRESHOLD and title not in self.titles_over_thresh:
                # Query 3
                self.titles_over_thresh.add(title)
                authors = reviews[0].get("authors")
                self.q3_output_batch.append({"title": title, "authors": authors})
                self.q3_output_batch_size += 1
                # Query 4
                reviews = self.reviews_per_title[title]
                for r in reviews:
                    self.q4_output_batch.append({"title": title, "review/score": r["review/score"]})
                    self.q4_output_batch_size += 1            

        self.titles_in_last_msg.clear()


    # def _send_batch_q3(self, buffer: list):
    #     data = list(
    #         map(
    #             lambda item: {
    #                 "title": item["title"],
    #                 "authors": item["review"]["authors"],
    #             },
    #             buffer
    #         )
    #     )

    #     msg = Message({"query": 3, "data": data})
    #     self.messaging.send_to_queue(self.OUTPUT_Q3, msg)
    #     logging.debug(f"Sent Data to: {self.OUTPUT_Q3} data: {data} LUEGO: {self.titles_over_thresh} Y ADEMAS: {self.review_counts.get('The Last Juror')}")

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
        "ITEMS_PER_BATCH": int
    }
    filter_config = Configuration.from_file(required, "config.ini")
    filter_config.update_from_env()
    filter_config.validate()

    config_logging(filter_config.get("LOGGING_LEVEL"))
    logging.info(filter_config)

    counter = ReviewCounter(items_per_batch=filter_config.get("ITEMS_PER_BATCH"))
    signal.signal(signal.SIGTERM, lambda sig, frame: sigterm_handler(counter))
    counter.listen()

    logging.info("Shutting Down.")




if __name__ == "__main__":
    main()
