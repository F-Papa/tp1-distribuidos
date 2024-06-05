import json
import logging
import os
from typing import Optional
from src.data_store.data_store import DataStore
from src.exceptions.shutting_down import ShuttingDown
from src.messaging.goutong import Goutong
from src.messaging.message import Message
from src.utils.config_loader import Configuration


counter = 0


class JoinerState:
    IDLE = 0
    RECEIVING_BOOKS = 1
    RECEIVING_REVIEWS = 2

    FILE_NAME = "joiner_state.txt"

    def __init__(
        self, state: int, current_connection: Optional[int], eof_received: int
    ):
        if state not in [
            JoinerState.IDLE,
            JoinerState.RECEIVING_BOOKS,
            JoinerState.RECEIVING_REVIEWS,
        ]:
            raise ValueError(f"Invalid state: {state}")
        self._phase = state
        self._eof_received = eof_received
        self._current_connection = current_connection

    @classmethod
    def restore_or_init(cls) -> "JoinerState":
        # Create file with default values if it doesn't exist
        if not os.path.exists(JoinerState.FILE_NAME):
            with open(JoinerState.FILE_NAME, "w") as f:
                f.write(
                    json.dumps(
                        {
                            "state": cls.IDLE,
                            "current_connection": None,
                            "eof_received": 0,
                        }
                    )
                )

        # Load values from file
        with open(JoinerState.FILE_NAME, "r") as f:
            saved_state = json.loads(f.readline())

        return JoinerState(
            saved_state["state"],
            saved_state["current_connection"],
            saved_state["eof_received"],
        )

    # Query Methods
    def receiving_books(self) -> bool:
        return self._phase == JoinerState.RECEIVING_BOOKS

    def receiving_reviews(self) -> bool:
        return self._phase == JoinerState.RECEIVING_REVIEWS

    def idle(self) -> bool:
        return self._phase == JoinerState.IDLE

    # Command Methods
    def mark_first_eof_received(self):
        self._eof_received += 1
        self.save()

    def mark_idle(self):
        self._phase = JoinerState.IDLE
        self._current_connection = None
        self.eof_received = 0
        self.save()

    def mark_receiving_books(self, connection_id: int):
        self._phase = JoinerState.RECEIVING_BOOKS
        self._current_connection = connection_id
        self._eof_received = 0
        self.save()

    def mark_receiving_reviews(self):
        self._phase = JoinerState.RECEIVING_REVIEWS
        self._eof_received = 0
        self.save()

    def save(self):
        tmp_file = "temp_" + JoinerState.FILE_NAME

        with open(tmp_file, "w") as f:
            f.write(
                json.dumps(
                    {
                        "state": self._phase,
                        "current_connection": self._current_connection,
                        "eof_received": self._eof_received,
                    }
                )
                + "\n"
            )

        os.replace(tmp_file, JoinerState.FILE_NAME)

    def __str__(self) -> str:
        return f"JoinerState(phase={self._phase}, current_connection={self._current_connection})"


class Joiner:
    DATA_FILE_NAME = "joiner_data.txt"
    PENDING_CONN_QUEUE = "joiner_pending"
    BOOKS_QUEUE_PREFIX = "books_queue_"
    REVIEWS_QUEUE_PREFIX = "reviews_queue_"
    Q5_OUTPUT_QUEUE = "sentiment_analyzer_queue"
    Q3_4_OUTPUT_QUEUE = "review_counter_queue"

    def __init__(
        self,
        config: Configuration,
        data: DataStore,
        restored_state: JoinerState,
        messaging_module: type,
    ):
        self._messaging_module = messaging_module
        self._batch_limit = config.get("BATCH_LIMIT")
        self._unacked_msg_limit = config.get("UNACKED_MSG_LIMIT")
        self._data = data
        self._state = restored_state
        self._unacked_delivery_ids = []

        self._shutting_down = False
        self._messaging_host = config.get("MESSAGING_HOST")
        self._messaging_port = config.get("MESSAGING_PORT")

    def start(self):
        self._messaging = self._messaging_module(
            self._messaging_host, self._messaging_port
        )
        try:
            self._start_aux()
        except ShuttingDown:
            logging.info("Shutting down")

    # Control Flow

    def _start_aux(self):
        while not self._shutting_down:
            if self._state.idle():
                self._accept_next_connection()
            elif self._state.receiving_books():
                self._receive_books()
            elif self._state.receiving_reviews():
                self.receive_reviews()

    def _accept_next_connection(self):
        logging.info("Accepting Next Connection")
        self._state.mark_idle()
        self._data.clear()

        self._messaging: Goutong = self._messaging_module(
            self._messaging_host, self._messaging_port
        )

        self._messaging.set_callback(
            Joiner.PENDING_CONN_QUEUE,
            self._accept_next_connection_callback,
            auto_ack=False,
        )
        self._messaging.listen()

    def _accept_next_connection_callback(self, messaging: Goutong, msg: Message):
        self._state.mark_receiving_books(msg.get("conn_id"))
        # messaging.ack_n_messages(1)
        messaging.ack_delivery(msg.delivery_id)
        messaging.stop_consuming(msg.queue_name)

    # Books methods

    def _receive_books(self):
        logging.info(
            "Receiving Books on queue books_" + str(self._state._current_connection)
        )
        # self._messaging = self._messaging_module(
        #     self._messaging_host, self._messaging_port
        # )
        if self._state._current_connection is None:
            raise ValueError("No connection to receive books from")

        books_queue = Joiner.BOOKS_QUEUE_PREFIX + str(self._state._current_connection)

        self._messaging.set_callback(
            books_queue, self._receive_books_callback, auto_ack=False
        )
        self._messaging.listen()

    def _receive_books_callback(self, messaging: Goutong, msg: Message):
        queries = msg.get("queries")
        books = msg.get("data")

        if books:
            for b in books:
                value = b["authors"] if 3 in queries else True
                self._data.add([queries, b["title"]], value)

        is_EOF = msg.get("EOF")

        self._unacked_delivery_ids.append(msg.delivery_id)

        if is_EOF:
            if self._state._eof_received == 0:
                self._state.mark_first_eof_received()
            else:
                self._state.mark_receiving_reviews()

        if len(self._unacked_delivery_ids) >= self._unacked_msg_limit or is_EOF:
            self._data.commit_to_disk()
            for delivery_id in self._unacked_delivery_ids:
                messaging.ack_delivery(delivery_id)
            self._unacked_delivery_ids.clear()

        if self._state.receiving_reviews():
            messaging.stop_consuming(msg.queue_name)

    # Reviews methods
    def receive_reviews(self):
        logging.info("Receiving Reviews")
        # self._messaging = self._messaging_module(
        #     self._messaging_host, self._messaging_port
        # )

        reviews_queue = Joiner.REVIEWS_QUEUE_PREFIX + str(
            self._state._current_connection
        )

        self._messaging.set_callback(
            reviews_queue, self._receive_reviews_callback, auto_ack=False
        )
        self._messaging.listen()

    def _receive_reviews_callback(self, messaging: Goutong, msg: Message):
        reviews = msg.get("data")
        global counter

        if reviews:
            self._receive_reviews_q5(messaging, reviews)
            self._receive_reviews_q3_4(messaging, reviews)

        # Forward EOF
        if msg.get("EOF"):
            body = {
                "transaction_id": counter,
                "conn_id": self._state._current_connection,
                "queries": [5],
                "data": [],
                "EOF": True,
            }
            messaging.send_to_queue(Joiner.Q5_OUTPUT_QUEUE, Message(body))

            body = {
                "transaction_id": counter,
                "conn_id": self._state._current_connection,
                "queries": [3, 4],
                "data": [],
                "EOF": True,
            }
            messaging.send_to_queue(Joiner.Q3_4_OUTPUT_QUEUE, Message(body))

            counter += 1
        # Acknowledge message
        # messaging.ack_n_messages(1)
        # Stop listening

        if msg.get("EOF"):
            self._state.mark_idle()

        messaging.ack_delivery(msg.delivery_id)

        if msg.get("EOF"):
            messaging.stop_consuming(msg.queue_name)

    def _receive_reviews_q5(self, messaging: Goutong, reviews: list):
        global counter
        batch = []
        output_queue = Joiner.Q5_OUTPUT_QUEUE

        # Find reviews that have a title in the data
        for r in reviews:
            key = [[5], r["title"]]
            value = self._data.get(key)
            if value:
                batch.append({"title": r["title"], "review/text": r["review/text"]})

        # Send the reviews
        while batch:
            to_send = pop_n(batch, self._batch_limit)
            body = {
                "transaction_id": counter,
                "conn_id": self._state._current_connection,
                "queries": [5],
                "data": to_send,
            }
            msg = Message(body)
            messaging.send_to_queue(output_queue, msg)
            counter += 1

    def _receive_reviews_q3_4(self, messaging: Goutong, reviews: list):
        batch = []
        global counter
        output_queue = Joiner.Q3_4_OUTPUT_QUEUE

        # Find reviews that have a title in the data
        for r in reviews:
            key = [[3, 4], r["title"]]
            value = self._data.get(key)
            if value:
                batch.append(
                    {
                        "title": r["title"],
                        "authors": value,
                        "review/score": r["review/score"],
                    }
                )

        # Send the reviews
        while batch:
            to_send = pop_n(batch, self._batch_limit)
            body = {
                "transaction_id": counter,
                "conn_id": self._state._current_connection,
                "queries": [3, 4],
                "data": to_send,
            }
            msg = Message(body)
            messaging.send_to_queue(output_queue, msg)
            counter += 1


def pop_n(a_list: list, n: int) -> list:
    to_return = a_list[:n]
    del a_list[:n]
    return to_return


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


if __name__ == "__main__":

    required = {
        "LOGGING_LEVEL": str,
        "BATCH_LIMIT": int,
        "MAX_ITEMS_IN_MEMORY": int,
        "UNACKED_MSG_LIMIT": int,
        "MESSAGING_HOST": str,
        "MESSAGING_PORT": int,
    }

    joiner_config = Configuration.from_file(required, "config.ini")
    joiner_config.update_from_env()
    joiner_config.validate()

    config_logging(joiner_config.get("LOGGING_LEVEL"))

    recovered_state = JoinerState.restore_or_init()
    logging.info(recovered_state)

    recovered_data = DataStore.restore_or_init(Joiner.DATA_FILE_NAME)
    logging.info(f"Recovered data: {recovered_data.num_items()} items")

    joiner = Joiner(joiner_config, recovered_data, recovered_state, Goutong)
    joiner.start()
