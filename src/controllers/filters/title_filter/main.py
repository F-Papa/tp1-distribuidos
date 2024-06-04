import json
import os
from src.controller_state.controller_state import ControllerState
from src.messaging.goutong import Goutong
from src.exceptions.shutting_down import ShuttingDown
import logging
import signal

from src.messaging.message import Message
from src.utils.config_loader import Configuration

EOF_QUEUE = "title_filter_eof"
KEYWORD_Q1 = "distributed"
OUTPUT_Q1 = "category_filter_queue"

shutting_down = False

class TitleFilter:
    FILTER_TYPE = "title_filter"
    
    def __init__(self, filter_config: Configuration, state: ControllerState, messaging: Goutong, title_keyword: str, output_queue: str, eof_queue: str):
        self._shutting_down = False
        self._state = state

        if os.path.exists(state.file_path):
            state.update_from_file()
            
        self._config = filter_config
        control_queue_name = (
            self.FILTER_TYPE + str(filter_config.get("FILTER_NUMBER")) + "_control"
        )
        self.input_queue_name = self.FILTER_TYPE + str(filter_config.get("FILTER_NUMBER"))
        self.title_keyword = title_keyword
        self.output_queue = output_queue
        self.eof_queue = eof_queue
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

    def filter_data(self, data: list):
        filtered_data = []

        for book in data:
            title = book.get("title")
            if self.title_keyword.lower() in title.lower():
                filtered_data.append(book)

        return filtered_data

    # Graceful Shutdown
    def sigterm_handler(self):
        logging.info("SIGTERM received. Initiating Graceful Shutdown.")
        self._shutting_down = True
        raise ShuttingDown


    def get_next_message(self):
        self._messaging.set_callback(
            self.input_queue_name, self.callback_title_filter, auto_ack=False, args=(self._state,)
        )
        self._messaging.listen()


    def handle_uncommited_transactions(self):
        if self._state.get("filtered_books"):

            # list(map(lambda b: logging.info(f"Sending {b['title']}"), self._state.get("filtered_books")))

            self._send_batch(
                messaging=self._messaging,
                batch=self._state.get("filtered_books"),
                conn_id=self._state.get("conn_id"),
                queries=self._state.get("queries"),
                transaction_id=self._state.id_for_next_transaction(),
            )
        if self._state.get("EOF"):
            self._send_EOF(messaging=self._messaging, conn_id=self._state.get("conn_id"), transaction_id=self._state.id_for_next_transaction() + "_EOF")

        self._state.mark_transaction_committed()


    def callback_title_filter(self, messaging: Goutong, msg: Message, state: ControllerState):
        transaction_id = msg.get("transaction_id")

        # Ignore duplicate transactions
        if transaction_id in state.transactions_received:
            messaging.ack_delivery(msg.delivery_id)
            logging.info(f"Received Duplicate Transaction {msg.get('transaction_id')}")
            return

        # Add new data to state
        books_received = msg.get("data") if msg.has_key("data") else []
        filtered_books = self.filter_data(books_received)
        eof = msg.has_key("EOF")
        conn_id = msg.get("conn_id")
        transaction_id = msg.get("transaction_id")

        state.set("filtered_books", filtered_books)
        state.set("conn_id", conn_id)
        state.set("queries", msg.get("queries"))
        state.set("EOF", eof)
        state.set("committed", False)
        state.mark_transaction_received(transaction_id)
        state.save_to_disk()

        # Acknowledge message now that it's saved
        messaging.ack_delivery(msg.delivery_id)
        messaging.stop_consuming(msg.queue_name)
        logging.debug(f"no escucho mas queue {msg.queue_name}")


    def _columns_for_query1(self, book: dict) -> dict:
        return {
            "title": book["title"],
            "publisher": book["publisher"],
            "categories": book["categories"],
        }

    def _send_batch(
        self,
        messaging: Goutong,
        batch: list,
        conn_id: int,
        queries: list[int],
        transaction_id: str,
    ):
        msg_content = {
            "transaction_id": transaction_id,
            "conn_id": conn_id,
            "queries": queries,
        }
        if batch:
            data = list(map(self._columns_for_query1, batch))
            msg_content["data"] = data

        msg = Message(msg_content)
        messaging.send_to_queue(self.output_queue, msg)


    def _send_EOF(self, messaging: Goutong, conn_id: int, transaction_id: str):
        msg = Message(
            {"transaction_id": transaction_id, "conn_id": conn_id, "EOF": True, "forward_to": [self.output_queue], "queries": [1]}
        )
        messaging.send_to_queue(self.eof_queue, msg)
        logging.debug(f"Sent EOF to: {self.eof_queue}")



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
    controller_id = f"{TitleFilter.FILTER_TYPE}_{filter_config.get('FILTER_NUMBER')}"

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

    title_filter = TitleFilter(filter_config, state, messaging, KEYWORD_Q1, OUTPUT_Q1, EOF_QUEUE)
    signal.signal(signal.SIGTERM, lambda sig, frame: title_filter.sigterm_handler())
    title_filter.start()


if __name__ == "__main__":
    main()
