import json
import os
from typing import Any
from src.messaging.goutong import Goutong
from src.messaging.message import Message
import sys
import logging
import signal

from src.utils.config_loader import Configuration
from src.exceptions.shutting_down import ShuttingDown
from src.data_access.data_access import DataAccess


class SwitchingState(Exception):
    pass


class Joiner:
    RECEIVING_BOOKS = 1
    RECEIVING_REVIEWS = 2
    REVIEWS_INPUT_QUEUE = "joiner_reviews_queue"
    BOOKS_INPUT_QUEUE = "joiner_books_queue"
    CONTROL_QUEUE = "joiner_control"
    CONTROL_GROUP = "CONTROL"
    OUTPUT_Q3_4 = "review_counter_queue"
    OUTPUT_Q5 = "sentiment_analyzer_queue"

    def __init__(
        self, items_per_batch: int, books_q3_4: DataAccess, books_q5: DataAccess
    ):

        self.shutting_down = False
        self.eof_received = 0
        self.items_per_batch = items_per_batch
        self.state = self.RECEIVING_BOOKS
        self.books_q3_4 = books_q3_4
        self.books_q5 = books_q5

        self.batch_q3_4 = []
        self.batch_q5 = []
        self._init_messaging()
        self._set_receive_books()

    def listen(self):
        if self.state == self.RECEIVING_BOOKS:
            logging.info("Listening for Books")
        else:
            logging.info("Listening for Reviews")
        try:
            self.messaging.listen()
        except SwitchingState:
            logging.info("Switching State")
            if self.state == self.RECEIVING_BOOKS:
                self._set_receive_reviews()
            else:
                self._set_receive_books()
            self.listen()
        except ShuttingDown:
            logging.debug("Stopping Listening")

    def shutdown(self):
        logging.info("Initiating Graceful Shutdown")
        msg = Message({"ShutDown": True})
        self.messaging.broadcast_to_group(self.CONTROL_GROUP, msg)
        self.shutting_down = True
        self.messaging.close()
        raise ShuttingDown

    def _init_messaging(self):
        self.messaging = Goutong()
        self.messaging.add_queues(
            self.REVIEWS_INPUT_QUEUE,
            self.BOOKS_INPUT_QUEUE,
            self.CONTROL_QUEUE,
            self.OUTPUT_Q3_4,
            self.OUTPUT_Q5,
        )
        self.messaging.add_broadcast_group(self.CONTROL_GROUP, [self.CONTROL_QUEUE])
        self.messaging.set_callback(self.CONTROL_QUEUE, self.callback_control)

    def _set_receive_books(self):
        self.state = self.RECEIVING_BOOKS
        self.eof_received = 0
        self.books_q3_4.clear()
        self.books_q5.clear()

        self.messaging.set_callback(
            self.BOOKS_INPUT_QUEUE, self._handle_receiving_books
        )

    def _set_receive_reviews(self):
        self.state = self.RECEIVING_REVIEWS
        self.eof_received = 0
        self.messaging.set_callback(
            self.REVIEWS_INPUT_QUEUE, self._handle_receiving_reviews
        )

    def _send_EOF(self, connection_id: int):
        msg = Message({"conn_id": connection_id, "queries": [3, 4], "EOF": True})
        self.messaging.send_to_queue(self.OUTPUT_Q3_4, msg)
        logging.debug(f"Sent EOF to: {self.OUTPUT_Q3_4}")

        msg = Message({"conn_id": connection_id, "queries": 5, "EOF": True})
        self.messaging.send_to_queue(self.OUTPUT_Q5, msg)
        logging.debug(f"Sent EOF to: {self.OUTPUT_Q5}")

    def _handle_receiving_books(self, messaging: Goutong, msg: Message):
        queries = msg.get("queries")
        connection_id = msg.get("conn_id")

        if msg.has_key("EOF"):
            logging.info(f"Received EOF number {self.eof_received+1} from query {queries}")
            if self.state == self.RECEIVING_BOOKS:
                self.eof_received += 1
                if self.eof_received == 2:
                    raise SwitchingState
            return

        books = msg.get("data")

        if 3 in queries or 4 in queries:
            for book in books:
                self.books_q3_4.add(book["title"], book["authors"])
        elif 5 in queries:
            for book in books:
                self.books_q5.add(book["title"], True)

    def _handle_receiving_reviews(self, messaging: Goutong, msg: Message):
        connection_id = msg.get("conn_id")
        queries = msg.get("queries")
        if msg.has_key("EOF"):
            logging.info(f"Received EOF from query {queries}")
            if len(self.batch_q3_4) > 0:
                self._send_batch_q3_4(connection_id)
                self.batch_q3_4.clear()
            if len(self.batch_q5) > 0:
                self._send_batch_q5(connection_id)
                self.batch_q5.clear()

            self._send_EOF(connection_id)
            raise SwitchingState

        reviews = msg.get("data")

        for review in reviews:

            # Check if the review's title is in the books for query 3 and 4
            entry_q3_4 = self.books_q3_4.get(review["title"])
            if entry_q3_4 is not None:
                authors = entry_q3_4.value
                title = review["title"]
                review_score = review["review/score"]
                self.batch_q3_4.append((title, authors, review_score))
                if len(self.batch_q3_4) >= self.items_per_batch:
                    self._send_batch_q3_4(connection_id)
                    self.batch_q3_4.clear()

            # Check if the review's title is in the books for query 5
            entry_q5 = self.books_q5.get(review["title"])
            if entry_q5 is not None:
                title = review["title"]
                review_text = review["review/text"]
                self.batch_q5.append((title, review_text))

                if len(self.batch_q5) >= self.items_per_batch:
                    self._send_batch_q5(connection_id)
                    self.batch_q5.clear()

    def _send_batch_q3_4(self, connection_id: int):
        data = list(
            map(
                lambda item: {
                    "title": item[0],
                    "authors": item[1],
                    "review/score": item[2],
                },
                self.batch_q3_4,
            )
        )
        msg = Message({"conn_id": connection_id, "queries": [3, 4], "data": data})
        self.messaging.send_to_queue(self.OUTPUT_Q3_4, msg)

    def _send_batch_q5(self, connection_id: int):
        data = list(
            map(lambda item: {"title": item[0], "review/text": item[1]}, self.batch_q5)
        )
        msg = Message({"conn_id": connection_id, "queries": [5], "data": data})
        self.messaging.send_to_queue(self.OUTPUT_Q5, msg)

    def callback_control(self, messaging: Goutong, msg: Message):
        if msg.has_key("ShutDown"):
            self.shutting_down = True
            raise ShuttingDown


# Graceful Shutdown
def sigterm_handler(joiner: Joiner):
    logging.info("SIGTERM received.")
    joiner.shutdown()


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
        "ITEMS_PER_BATCH": int,
        "CACHE_VACANTS": int,
        "LOGGING_LEVEL": str,
        "N_PARTITIONS": int,
    }

    filter_config = Configuration.from_file(required, "config.ini")
    filter_config.update_from_env()
    filter_config.validate()

    config_logging(filter_config.get("LOGGING_LEVEL"))
    logging.info(filter_config)

    books_q3_4 = DataAccess(
        str,
        list,
        filter_config.get("CACHE_VACANTS") // 2,
        filter_config.get("N_PARTITIONS"),
        "q3_4_books",
    )

    books_q5 = DataAccess(
        str,
        bool,
        filter_config.get("CACHE_VACANTS") // 2,
        filter_config.get("N_PARTITIONS"),
        "q5_books",
    )

    joiner = Joiner(
        items_per_batch=filter_config.get("ITEMS_PER_BATCH"),
        books_q3_4=books_q3_4,
        books_q5=books_q5,
    )
    signal.signal(signal.SIGTERM, lambda sig, frame: sigterm_handler(joiner))
    joiner.listen()


if __name__ == "__main__":
    main()
