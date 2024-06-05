from collections import defaultdict
from src.messaging.goutong import Goutong
from src.messaging.message import Message
import logging
import signal
import os

from src.controller_state.controller_state import ControllerState

from src.utils.config_loader import Configuration
from src.exceptions.shutting_down import ShuttingDown

FILTER_TYPE = "date_filter"
EOF_QUEUE = "date_filter_eof"
CONTROL_GROUP = "CONTROL"


UPPER_Q1 = 2023
LOWER_Q1 = 2000

UPPER_Q3_4 = 1999
LOWER_Q3_4 = 1990

OUTPUT_Q1 = "title_filter_queue"
OUTPUT_Q3_4_PREFIX = "books_queue_"


class DateFilter:
    FILTER_TYPE = "date_filter"

    def __init__(
        self,
        filter_config: Configuration,
        state: ControllerState,
        messaging: Goutong,
        input_queue: str,
        upper_q3_4: int,
        lower_q3_4: int,
        upper_q1: int,
        lower_q1: int,
        output_q1: str,
        output_q3_4_prefix: str,
        eof_queue: str,
    ):
        self._shutting_down = False
        self.filter_config = filter_config
        self._state = state
        if os.path.exists(state.file_path):
            state.update_from_file()
        self._messaging = messaging
        self.input_queue = input_queue
        self.upper_q3_4 = upper_q3_4
        self.lower_q3_4 = lower_q3_4
        self.upper_q1 = upper_q1
        self.lower_q1 = lower_q1
        self.output_q1 = output_q1
        self.output_q3_4_prefix = output_q3_4_prefix
        self.eof_queue = eof_queue


    def start(self):

        # Main Flow
        try:
            while not self._shutting_down:
                if not self._state.committed:
                    self.handle_uncommited_transactions()
                self.get_next_message()
        except ShuttingDown:
            pass

        finally:
            logging.info("Shutting Down.")
            self._messaging.close()
            self._state.save_to_disk()

    # Graceful Shutdown
    def sigterm_handler(self):
        logging.info("SIGTERM received. Initiating Graceful Shutdown.")
        self._shutting_down = True
        raise ShuttingDown

    def get_next_message(self):
        self._messaging.set_callback(
            self.input_queue, self.callback_filter, auto_ack=False
        )
        self._messaging.listen()

    def handle_uncommited_transactions(self):
        to_send_q1 = self._state.get("filtered_books_q1")
        to_send_q3_4 = self._state.get("filtered_books_q3_4")

        if to_send_q1 or to_send_q3_4:
            self._send_batches(
                batch_q1=to_send_q1,
                batch_q3_4=to_send_q3_4,
                connection_id=self._state.get("conn_id"),
                queries=self._state.get("queries"),
                transaction_id=self._state.id_for_next_transaction(),
            )

        if self._state.get("EOF"):
            self._send_EOF(
                connection_id=self._state.get("conn_id"),
                transaction_id=self._state.id_for_next_transaction() + "_EOF",
            )
        self._state.mark_transaction_committed()

    def _columns_for_query1(self, book: dict) -> dict:
        return {
            "title": book["title"],
            "categories": book["categories"],
            "publisher": book["publisher"],
        }

    def _columns_for_query3_4(self, book: dict) -> dict:
        return {
            "title": book["title"],
            "authors": book["authors"],
        }

    def _send_batches(
        self,
        batch_q1: list,
        batch_q3_4: list,
        connection_id: int,
        queries: list,
        transaction_id: str,
    ):
        if batch_q1:
            self._send_batch_q1(batch_q1, connection_id, transaction_id)
        if batch_q3_4:
            self._send_batch_q3_4(batch_q3_4, connection_id, transaction_id)

    def _send_batch_q1(self, batch: list, connection_id: int, transaction_id: str):
        data = []
        for b in batch:
            columns = self._columns_for_query1(b)
            data.append(columns)

        msg = Message(
            {
                "transaction_id": transaction_id,
                "conn_id": connection_id,
                "queries": [1],
                "data": data,
            }
        )
        self._messaging.send_to_queue(self.output_q1, msg)
        # logging.debug(f"Sent Data to: {OUTPUT_Q1}")

    def _send_batch_q3_4(self, batch: list, connection_id: int, transaction_id: str):
        data = []
        for b in batch:
            columns = self._columns_for_query3_4(b)
            data.append(columns)
        msg = Message(
            {
                "transaction_id": transaction_id,
                "conn_id": connection_id,
                "queries": [3, 4],
                "data": data,
            }
        )
        output_queue = self.output_q3_4_prefix + str(connection_id)
        self._messaging.send_to_queue(output_queue, msg)
        # logging.debug(f"Sent Data to: {OUTPUT_Q3_4}")

    def _send_EOF(self, connection_id: int, transaction_id: str):
        output_q3_4 = self.output_q3_4_prefix + str(connection_id)
        msg = Message(
            {
                "transaction_id": transaction_id,
                "conn_id": connection_id,
                "queries": [1, 3, 4],
                "EOF": True,
                "forward_to": [self.output_q1, output_q3_4],
            }
        )
        self._messaging.send_to_queue(self.eof_queue, msg)

    def callback_filter(self, messaging: Goutong, msg: Message):
        # logging.debug(f"Received: {msg.marshal()}")
        transaction_id = msg.get("transaction_id")

        # Ignore duplicate transactions
        if transaction_id in self._state.transactions_received:
            self._messaging.ack_delivery(msg.delivery_id)
            logging.info(
                f"Received Duplicate Transaction {msg.get('transaction_id')}: "
                + msg.marshal()[:100]
            )
            return

        # Add new data to self._state
        eof = msg.has_key("EOF")
        books_received = msg.get("data") if msg.has_key("data") else []
        filtered_books_q1, filtered_books_q3_4 = self.filter_data(books_received)

        conn_id = msg.get("conn_id")
        transaction_id = msg.get("transaction_id")
        queries = msg.get("queries")

        self._state.set("filtered_books_q1", filtered_books_q1)
        self._state.set("filtered_books_q3_4", filtered_books_q3_4)
        self._state.set("conn_id", conn_id)
        self._state.set("queries", queries)
        self._state.set("EOF", eof)
        self._state.set("committed", False)
        self._state.mark_transaction_received(transaction_id)
        self._state.save_to_disk()

        # Acknowledge message now that it's saved
        self._messaging.ack_delivery(msg.delivery_id)
        self._messaging.stop_consuming(msg.queue_name)
        logging.debug(f"no escucho mas queue {msg.queue_name}")

    def filter_data(self, data: list):
        filtered_data_q1 = []
        filtered_data_q3_4 = []

        for book in data:
            year = book.get("year")
            if self.lower_q1 <= year <= self.upper_q1:
                filtered_data_q1.append(book)
            if self.lower_q3_4 <= year <= self.upper_q3_4:
                filtered_data_q3_4.append(book)

        return (filtered_data_q1, filtered_data_q3_4)


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
        "FILTER_NUMBER": int,
        "LOGGING_LEVEL": str,
        "ITEMS_PER_BATCH": int,
    }
    filter_config = Configuration.from_file(required, "config.ini")
    filter_config.update_from_env()
    filter_config.validate()

    config_logging(filter_config.get("LOGGING_LEVEL"))
    logging.info(filter_config)

    # Load State
    controller_id = f"{DateFilter.FILTER_TYPE}_{filter_config.get('FILTER_NUMBER')}"

    extra_fields = {
        "filtered_books_q1": [],
        "filtered_books_q3_4": [],
        "conn_id": 0,
        "queries": [],
        "EOF": False,
    }

    state = ControllerState(
        controller_id=controller_id,
        file_path=f"state/{controller_id}.json",
        temp_file_path=f"state/{controller_id}.tmp",
        extra_fields=extra_fields,
    )

    if os.path.exists(state.file_path):
        state.update_from_file()

    messaging = Goutong()

    # Set up the queues
    control_queue_name = (
        DateFilter.FILTER_TYPE + str(filter_config.get("FILTER_NUMBER")) + "_control"
    )
    input_queue_name = DateFilter.FILTER_TYPE + str(filter_config.get("FILTER_NUMBER"))
    filter = DateFilter(
        filter_config=filter_config,
        state=state,
        messaging=messaging,
        input_queue=input_queue_name,
        upper_q3_4=UPPER_Q3_4,
        lower_q3_4=LOWER_Q3_4,
        upper_q1=UPPER_Q1,
        lower_q1=LOWER_Q1,
        output_q1=OUTPUT_Q1,
        output_q3_4_prefix=OUTPUT_Q3_4_PREFIX,
        eof_queue=EOF_QUEUE,
    )

    signal.signal(signal.SIGTERM, lambda sig, frame: filter.sigterm_handler())
    filter.start()


if __name__ == "__main__":
    main()
