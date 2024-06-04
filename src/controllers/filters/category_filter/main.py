from collections import defaultdict
from src.messaging.goutong import Goutong
from src.utils.config_loader import Configuration
import logging
import signal
import os

from src.controller_state.controller_state import ControllerState


from src.messaging.message import Message
from src.exceptions.shutting_down import ShuttingDown

EOF_QUEUE = "category_filter_eof"

CATEGORY_Q1 = "Computers"
CATEGORY_Q5 = "Fiction"

OUTPUT_Q1_PREFIX = "results_"
OUTPUT_Q5_PREFIX = "books_queue_"


class CategoryFilter:
    FILTER_TYPE = "category_filter"

    def __init__(
        self,
        filter_config: Configuration,
        state: ControllerState,
        messaging: Goutong,
        input_queue: str,
        category_q1: str,
        output_queue_q1_prefix: str,
        category_q5: str,
        output_queue_q5_prefix,
        eof_queue: str,
    ):
        self._shutting_down = False
        self._state = state
        self._shutting_down = False
        if os.path.exists(state.file_path):
            state.update_from_file()

        self._config = filter_config
        self.input_queue = input_queue
        self.eof_queue = eof_queue
        self.category_q1 = category_q1
        self.output_queue_q1_prefix = output_queue_q1_prefix
        self.category_q5 = category_q5
        self.output_queue_q5_prefix = output_queue_q5_prefix
        self._messaging = messaging

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

    def filter_data(self, data: list, category: str):
        filtered_data = []
        for book in data:
            categories = book.get("categories")

            if category in categories:
                filtered_data.append(book)

        return filtered_data

    # Graceful Shutdown
    def sigterm_handler(self):
        logging.info("SIGTERM received. Initiating Graceful Shutdown.")
        self._shutting_down = True
        raise ShuttingDown

    def get_next_message(self):
        self._messaging.set_callback(
            self.input_queue, self.callback_filter, auto_ack=False)
        self._messaging.listen()

    def handle_uncommited_transactions(self):
        queries = self._state.get("queries")
        filtered_books = self._state.get("filtered_books")
        if filtered_books:
            self._send_batch(
                messaging=self._messaging,
                batch=filtered_books,
                conn_id=self._state.get("conn_id"),
                queries=queries,
                transaction_id=self._state.id_for_next_transaction(),
            )

        if self._state.get("EOF"):
            self._send_EOF_by_queryID(
                messaging=self._messaging,
                connection_id=self._state.get("conn_id"),
                queries=queries,
                transaction_id=self._state.id_for_next_transaction() + "_EOF",
            )

        self._state.mark_transaction_committed()

    def callback_filter(self, messaging: Goutong, msg: Message):
        transaction_id = msg.get("transaction_id")
        queries = msg.get("queries")

        # Ignore duplicate transactions
        if transaction_id in self._state.transactions_received:
            messaging.ack_delivery(msg.delivery_id)
            logging.info(
                f"Received Duplicate Transaction {msg.get('transaction_id')}: "
                + msg.marshal()[:100]
            )
            return

        # Add new data to self._state
        eof = msg.has_key("EOF")
        books_received = msg.get("data") if msg.has_key("data") else []

        if 1 in msg.get("queries"):
            queries = [1]
            category = self.category_q1
        elif 5 in msg.get("queries"):
            queries = [5]
            category = self.category_q5
        else:
            to_show = {
                "transaction_id": msg.get("transaction_id"),
                "conn_id": msg.get("conn_id"),
                "queries": msg.get("queries"),
                "data": msg.get("data"),
                "EOF": msg.get("EOF"),
            }
            raise ValueError(f"Invalid queries: {to_show}")
        filtered_books = self.filter_data(books_received, category)

        conn_id = msg.get("conn_id")
        transaction_id = msg.get("transaction_id")

        self._state.set("filtered_books", filtered_books)
        self._state.set("conn_id", conn_id)
        self._state.set("queries", queries)
        self._state.set("EOF", eof)
        self._state.set("committed", False)
        self._state.mark_transaction_received(transaction_id)
        self._state.save_to_disk()

        # Acknowledge message now that it's saved
        messaging.ack_delivery(msg.delivery_id)
        messaging.stop_consuming(msg.queue_name)
        logging.debug(f"no escucho mas queue {msg.queue_name}")

    def _send_EOF_by_queryID(
        self, messaging: Goutong, connection_id: int, queries: list, transaction_id: str
    ):
        if 1 in queries:
            output_queue = self.output_queue_q1_prefix + str(connection_id)
            self._send_EOF(
                messaging,
                forward_to=output_queue,
                connection_id=connection_id,
                queries=[1],
                transaction_id=transaction_id,
            )
        if 5 in queries:
            output_queue = self.output_queue_q5_prefix + str(connection_id)
            self._send_EOF(
                messaging,
                forward_to=output_queue,
                connection_id=connection_id,
                queries=[5],
                transaction_id=transaction_id,
            )

    def _columns_for_query1(self, book: dict) -> dict:
        return {
            "title": book.get("title"),
            "publisher": book["publisher"],
        }

    def _columns_for_query5(self, book: dict) -> dict:
        return {
            "title": book.get("title"),
        }

    def _send_batch(
        self,
        messaging: Goutong,
        batch: list,
        conn_id: int,
        queries: list,
        transaction_id: str,
    ):
        if 1 in queries and batch:
            self._send_batch_q1(messaging, batch, conn_id, transaction_id)
        if 5 in queries and batch:
            self._send_batch_q5(messaging, batch, conn_id, transaction_id)

    def _send_batch_q1(
        self, messaging: Goutong, batch: list, connection_id: int, transaction_id: str
    ):
        data = list(map(self._columns_for_query1, batch))
        msg = Message(
            {
                "transaction_id": transaction_id,
                "conn_id": connection_id,
                "queries": [1],
                "data": data,
            }
        )
        output_queue = self.output_queue_q1_prefix + str(connection_id)
        messaging.send_to_queue(output_queue, msg)
        logging.debug(f"Sent Data to: {output_queue}")

    def _send_batch_q5(
        self, messaging: Goutong, batch: list, connection_id: int, transaction_id: str
    ):
        data = list(map(self._columns_for_query5, batch))
        msg = Message(
            {
                "transaction_id": transaction_id,
                "conn_id": connection_id,
                "queries": [5],
                "data": data,
            }
        )

        output_queue = self.output_queue_q5_prefix + str(connection_id)
        messaging.send_to_queue(output_queue, msg)
        logging.debug(f"Sent Data to: {output_queue}")

    def _send_EOF(
        self,
        messaging: Goutong,
        forward_to: str,
        connection_id: int,
        queries: list,
        transaction_id: str,
    ):

        msg = Message(
            {
                "transaction_id": transaction_id,
                "conn_id": connection_id,
                "EOF": True,
                "forward_to": [forward_to],
                "queries": queries,
            }
        )
        messaging.send_to_queue(self.eof_queue, msg)


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
        "ITEMS_PER_BATCH": int,
        "FILTER_NUMBER": int,
    }

    filter_config = Configuration.from_file(required, "config.ini")
    filter_config.update_from_env()
    filter_config.validate()

    config_logging(filter_config.get("LOGGING_LEVEL"))
    logging.info(filter_config)

    # Load State
    controller_id = f"{CategoryFilter.FILTER_TYPE}_{filter_config.get('FILTER_NUMBER')}"

    extra_fields = {
        "filtered_books": [],
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

    category_filter = CategoryFilter(
        filter_config=filter_config,
        state=state,
        messaging=messaging,
        category_q1=CATEGORY_Q1,
        output_queue_q1_prefix=OUTPUT_Q1_PREFIX,
        category_q5=CATEGORY_Q5,
        output_queue_q5_prefix=OUTPUT_Q5_PREFIX,
        eof_queue=EOF_QUEUE,
        input_queue=CategoryFilter.FILTER_TYPE
        + str(filter_config.get("FILTER_NUMBER")),
    )

    signal.signal(signal.SIGTERM, lambda sig, frame: category_filter.sigterm_handler())

    category_filter.start()


if __name__ == "__main__":
    main()
