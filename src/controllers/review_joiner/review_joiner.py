import json
import logging
import os
import signal
import sys
import threading
import time

import pika
import pika.exceptions
from src.controller_state.controller_state import ControllerState
from src.controllers.common.healthcheck import healthcheck_handler
from src.exceptions.shutting_down import ShuttingDown
from src.messaging.goutong import Goutong
from src.messaging.message import Message
from src.utils.config_loader import Configuration
import random

OUTPUT_Q5 = "sentiment_analyzer_queue"
OUTPUT_Q3_4 = "review_counter_queue"
MAX_RETRIES = 3
total_books = 0


def crash_maybe():
    if random.random() < 0.00001:
        sys.exit(1)


class ReviewsJoiner:
    BOOKS_EOF_EXPECTED = 2
    RECEIVING_BOOKS = 0
    RECEIVING_REVIEWS = 1
    CONTROLLER_NAME = "review_joiner"

    BOOK_COLUMNS_BY_QUERY = {
        (3, 4): ["title", "authors"],
        (5,): ["title"],
    }
    REVIEW_COLUMNS_BY_QUERY = {
        (5,): ["review/text", "review/score"],
        (3, 4): ["review/score"],
    }

    def __init__(
        self, config, state: ControllerState, messaging: Goutong, output_queues: dict
    ):
        self._filter_number = config.get("FILTER_NUMBER")
        self._controller_id = f"{self.CONTROLLER_NAME}{self._filter_number}"
        self._shutting_down = False
        self._state = state
        self._messaging = messaging
        self._books_queue = f"{self.CONTROLLER_NAME}_{self._filter_number}_books"
        self._reviews_queue_prefix = (
            f"{self.CONTROLLER_NAME}_{self._filter_number}_reviews_conn_"
        )
        self.unacked_msg_limit = config.get("UNACKED_MSG_LIMIT")
        self.unacked_time_limit_in_seconds = config.get("UNACKED_TIME_LIMIT_IN_SECONDS")
        self.unacked_msgs = []
        self.unacked_msg_count = 0
        self.time_of_last_commit = time.time()
        self._proxy_queue = f"{self.CONTROLLER_NAME}_proxy"
        # self._output_queue_q5 = output_queue_q5
        # self._output_queue_q3_4 = output_queue_q3_4

        self._output_queues = output_queues
        self._extra_book_columns_to_save = {}
        for queries in self.BOOK_COLUMNS_BY_QUERY:
            self._extra_book_columns_to_save[queries] = self.BOOK_COLUMNS_BY_QUERY[
                queries
            ].copy()
            if "title" in self._extra_book_columns_to_save[queries]:
                self._extra_book_columns_to_save[queries].remove("title")
            if len(self._extra_book_columns_to_save[queries]) == 0:
                del self._extra_book_columns_to_save[queries]

    @classmethod
    def default_state(
        cls, controller_id: str, file_path: str, temp_file_path: str
    ) -> ControllerState:
        extra_fields = {
            "saved_books": {},
            "ongoing_connections": {},
        }

        return ControllerState(
            controller_id=controller_id,
            file_path=file_path,
            temp_file_path=temp_file_path,
            extra_fields=extra_fields,
        )

    def start(self):
        logging.info("Starting Review Joiner")
        threading.Thread(
            target=healthcheck_handler,
            args=(self,),
        ).start()
        try:
            if not self._shutting_down:
                self._messaging.set_callback(
                    self.books_queue(), self.callback_books, auto_ack=False
                )
                for conn_id in self._state.get("ongoing_connections"):
                    if (
                        self._state.get("ongoing_connections")[conn_id]["state"]
                        == self.RECEIVING_REVIEWS
                    ):
                        self._set_callback_reviews(conn_id)

                self._messaging.listen()

        except ShuttingDown:
            pass
        finally:
            logging.info("Shutting Down.")
            self._messaging.close()

    # region: Query methods
    def controller_id(self):
        return self._controller_id

    def is_shutting_down(self):
        return self._shutting_down

    def books_queue(self):
        return self._books_queue

    def reviews_queue(self, conn_id: int):
        return self._reviews_queue_prefix + str(conn_id)

    def output_queue_name(self, queries: tuple, conn_id: int):
        entry = self._output_queues.get(queries)
        if entry is None:
            raise ValueError(f"Output queue not found for queries {queries}")

        if entry["is_prefix"]:
            return entry["name"] + str(conn_id)
        return entry["name"]

    def _is_transaction_id_valid(self, msg: Message):
        transaction_id = msg.get("transaction_id")
        sender = f"{msg.get('sender')}@{msg.queue_name}"

        expected_transaction_id = self._state.next_inbound_transaction_id(sender)

        return transaction_id == expected_transaction_id

    # endregion

    # region: Callbacks
    def callback_books(self, _: Goutong, msg: Message):
        # Validate transaction_id
        if not self._is_transaction_id_valid(msg):
            self._handle_invalid_transaction_id(msg)
            return

        conn_id = msg.get("conn_id")
        conn_id_str = str(conn_id)
        queries = tuple(msg.get("queries"))
        sender = f"{msg.get('sender')}@{msg.queue_name}"
        queries_str = json.dumps(
            queries
        )  # Cannot save tuples as keys in json so we convert to string
        saved_books = self._state.get("saved_books")
        # Add connection to saved_books if it doesn't exist
        if conn_id_str not in saved_books:
            saved_books[conn_id_str] = {}
        # Add queries to saved_books to connection if it doesn't exist
        if queries_str not in saved_books[conn_id_str]:
            saved_books[conn_id_str][queries_str] = {}
        # Add connection to ongoing_connections if it doesn't exist
        ongoing_connections = self._state.get("ongoing_connections")
        if conn_id_str not in ongoing_connections:
            ongoing_connections[conn_id_str] = {
                "state": self.RECEIVING_BOOKS,
                "EOFs": 0,
            }

        # Increment EOFs if received
        if msg.get("EOF"):
            ongoing_connections[conn_id_str]["EOFs"] += 1
            if ongoing_connections[conn_id_str]["EOFs"] == self.BOOKS_EOF_EXPECTED:
                # All books received, start listening for reviews at the reviews queue for this conn_id
                ongoing_connections[conn_id_str] = {"state": self.RECEIVING_REVIEWS}
                for k in saved_books.keys():
                    logging.info("saved_books Key: " + k)
                    for k2 in saved_books[k].keys():
                        logging.info(
                            "saved_books Key2: "
                            + k2
                            + " "
                            + str(len(saved_books[k][k2]))
                        )

                self._set_callback_reviews(conn_id)
        self._state.set("ongoing_connections", ongoing_connections)
        # Add books to saved_books
        if books := msg.get("data"):
            for book in books:
                to_save = {}
                if extra_columns := self._extra_book_columns_to_save.get(queries):
                    for column in extra_columns:
                        to_save[column] = book[column]
                saved_books[conn_id_str][queries_str][book.get("title")] = to_save

        self._state.set("saved_books", saved_books)
        self._state.inbound_transaction_committed(sender)

        self.unacked_msg_count
        self.unacked_msgs.append(msg.delivery_id)

        now = time.time()
        time_since_last_commit = now - self.time_of_last_commit

        if self.unacked_msg_count == 0:
            return

        if (
            self.unacked_msg_count > self.unacked_msg_limit
            or time_since_last_commit > self.unacked_time_limit_in_seconds
        ):
            logging.info(
                f"Committing to disk | Unacked Msgs.: {self.unacked_msg_count} | Secs. since last commit: {time_since_last_commit}"
            )
            crash_maybe()
            self._state.save_to_disk()
            self.time_of_last_commit = now

            for delivery_id in self.unacked_msgs:
                crash_maybe()
                self._messaging.ack_delivery(delivery_id)

            self.unacked_msg_count = 0
            self.unacked_msgs.clear()

    def _callback_reviews_aux(self, msg: Message, queries_str: str):
        conn_id = msg.get("conn_id")
        conn_id_str = str(conn_id)
        queries_tuple = tuple(json.loads(queries_str))
        output_queue = self.output_queue_name(queries_tuple, conn_id)

        if reviews := msg.get("data"):
            books_this_queries_and_conn = self._state.get("saved_books")[conn_id_str][
                queries_str
            ]
            joined_data = self._books_and_reviews_left_join_by_title(
                books_this_queries_and_conn, reviews
            )
            trimmed_data = self._columns_for_queries(joined_data, queries_tuple)

            if not trimmed_data and not msg.get("EOF"):
                return

            if trimmed_data:
                msg_content = {
                    "transaction_id": self._state.next_outbound_transaction_id(
                        self._proxy_queue
                    ),
                    "conn_id": conn_id,
                    "queries": [3, 4],
                    "data": trimmed_data,
                    "forward_to": [output_queue],
                }
                crash_maybe()
                self._messaging.send_to_queue(self._proxy_queue, Message(msg_content))
                self._state.outbound_transaction_committed(self._proxy_queue)

        if msg.get("EOF"):
            msg_content = {
                "transaction_id": self._state.next_outbound_transaction_id(
                    self._proxy_queue
                ),
                "EOF": True,
                "conn_id": conn_id,
                "queries": [3, 4],
                "data": [],
                "forward_to": [output_queue],
            }
            crash_maybe()
            self._messaging.send_to_queue(self._proxy_queue, Message(msg_content))
            self._state.outbound_transaction_committed(self._proxy_queue)

    def callback_reviews(self, _: Goutong, msg: Message):
        # Validate transaction_id
        if not self._is_transaction_id_valid(msg):
            self._handle_invalid_transaction_id(msg)
            return

        sender = f"{msg.get('sender')}@{msg.queue_name}"
        conn_id = msg.get("conn_id")
        conn_id_str = str(conn_id)

        # Send data to output queue

        for queries_str in self._state.get("saved_books")[conn_id_str]:
            self._callback_reviews_aux(msg, queries_str)

        if msg.get("EOF"):
            logging.info(f"End of reviews received from {msg.get('conn_id')}")
            crash_maybe()
            self._messaging.stop_consuming(self.reviews_queue(conn_id))
            self._state.get("ongoing_connections").pop(conn_id_str)
            self._state.get("saved_books").pop(conn_id_str)

        self._state.inbound_transaction_committed(sender)

        self.unacked_msg_count
        self.unacked_msgs.append(msg.delivery_id)

        now = time.time()
        time_since_last_commit = now - self.time_of_last_commit

        if self.unacked_msg_count == 0:
            return

        if (
            self.unacked_msg_count > self.unacked_msg_limit
            or time_since_last_commit > self.unacked_time_limit_in_seconds
        ):
            logging.info(
                f"Committing to disk | Unacked Msgs.: {self.unacked_msg_count} | Secs. since last commit: {time_since_last_commit}"
            )
            crash_maybe()
            self._state.save_to_disk()
            self.time_of_last_commit = now

            for delivery_id in self.unacked_msgs:
                crash_maybe()
                self._messaging.ack_delivery(delivery_id)

            self.unacked_msg_count = 0
            self.unacked_msgs.clear()

    # endregion

    # region: Command methods
    def shutdown(self):
        logging.info("SIGTERM received. Initiating Graceful Shutdown.")
        self._shutting_down = True
        raise ShuttingDown

    def _handle_invalid_transaction_id(self, msg: Message):
        transaction_id = msg.get("transaction_id")
        sender = f"{msg.get('sender')}@{msg.queue_name}"
        expected_transaction_id = self._state.next_inbound_transaction_id(sender)

        if transaction_id < expected_transaction_id:
            logging.info(
                f"Received Duplicate Transaction {transaction_id} from {sender}: "
                + msg.marshal()[:100]
            )
            self._messaging.ack_delivery(msg.delivery_id)

        elif transaction_id > expected_transaction_id:
            self._messaging.requeue(msg)
            logging.info(
                f"Requeueing out of order {transaction_id}, expected {str(expected_transaction_id)}"
            )

    def _set_callback_reviews(self, conn_id: int):
        self._messaging.set_callback(
            self.reviews_queue(conn_id), self.callback_reviews, auto_ack=False
        )

    def _books_and_reviews_left_join_by_title(self, books: dict, reviews: list) -> list:
        joined_data = []
        for review in reviews:
            if review.get("title") in books:
                book = books[review.get("title")].copy()
                book["title"] = review.get("title")
                to_append = {"book": book, "review": review}
                joined_data.append(to_append)
        return joined_data

    def _columns_for_queries(self, data: list[dict], queries: tuple) -> list:
        book_columns = self.BOOK_COLUMNS_BY_QUERY[queries]
        review_columns = self.REVIEW_COLUMNS_BY_QUERY[queries]

        filtered_data = []
        for row in data:
            new_item = {}
            for column in book_columns:
                new_item[column] = row["book"][column]
            for column in review_columns:
                new_item[column] = row["review"][column]
            filtered_data.append(new_item)
        return filtered_data

    # endregion


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
        "MESSAGING_HOST": str,
        "MESSAGING_PORT": int,
        "FILTER_NUMBER": int,
        "UNACKED_MSG_LIMIT": int,
        "UNACKED_TIME_LIMIT_IN_SECONDS": int,
    }

    config = Configuration.from_file(required, "config.ini")
    config.update_from_env()
    config.validate()

    config_logging(config.get("LOGGING_LEVEL"))
    logging.info(config)

    controller_id = f"{ReviewsJoiner.CONTROLLER_NAME}_{config.get('FILTER_NUMBER')}"
    state_file_path = f"state/{controller_id}.json"
    temp_file_path = f"state/{controller_id}.tmp"

    state = ReviewsJoiner.default_state(controller_id, state_file_path, temp_file_path)

    if os.path.exists(state_file_path):
        logging.info("State file found. Loading state.")
        state.update_from_file()
        to_show = ""
        for conn in state.get("saved_books").keys():
            for q in state.get("saved_books")[conn].keys():
                to_show += f"{len(state.get('saved_books')[conn][q])} books from conn {conn} with queries {q}\n"

        # logging.info(to_show)

    output_queues = {
        (3, 4): {"name": OUTPUT_Q3_4, "is_prefix": False},
        (5,): {"name": OUTPUT_Q5, "is_prefix": False},
    }

    messaging = Goutong(sender_id=controller_id)
    joiner = ReviewsJoiner(config, state, messaging, output_queues)
    signal.signal(signal.SIGTERM, lambda sig, frame: joiner.shutdown())
    joiner.start()


if __name__ == "__main__":
    main()
