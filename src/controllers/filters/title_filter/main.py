from enum import Enum
import os
import socket
from src.controller_state.controller_state import ControllerState
from src.messaging.goutong import Goutong
from src.exceptions.shutting_down import ShuttingDown
from src.controllers.common.healthcheck import healthcheck_handler
import logging
import signal
import threading

from src.messaging.message import Message
from src.utils.config_loader import Configuration

KEYWORD_Q1 = "distributed"
OUTPUT_Q1 = "category_filter_queue"


def crash_maybe():
    import random

    if random.random() < 0.0001:
        logging.error("Crashing...")
        os._exit(1)


class TitleFilter:
    FILTER_TYPE = "title_filter"
    CONTROL_PORT = 12347
    MSG_REDUNDANCY = 3

    def __init__(
        self,
        filter_config: Configuration,
        state: ControllerState,
        messaging: Goutong,
        title_keyword: str,
        output_queue: str,
    ):
        self._shutting_down = False
        self._state = state
        self._config = filter_config

        self._proxy_queue = f"{self.FILTER_TYPE}_proxy"
        self.title_keyword = title_keyword
        self._output_queue = output_queue
        self._messaging = messaging
        self._controller_id = self.FILTER_TYPE + str(filter_config.get("FILTER_NUMBER"))
        self.input_queue_name = self._controller_id

    def start(self):
        threading.Thread(
            target=healthcheck_handler,
            args=(self,),
        ).start()

        # Main Flow
        try:
            if not self._shutting_down:
                self._messaging.set_callback(
                    self.input_queue_name,
                    self._callback_title_filter,
                    auto_ack=False,
                )
                self._messaging.listen()
        except ShuttingDown:
            pass

        finally:
            logging.info("Shutting Down.")
            self._messaging.close()
            self._state.save_to_disk()

    # MAIN FUNCTIONALITY
    @classmethod
    def default_state(
        cls, controller_id: str, file_path: str, temp_file_path: str
    ) -> ControllerState:
        return ControllerState(
            controller_id=controller_id,
            file_path=file_path,
            temp_file_path=temp_file_path,
            extra_fields={},
        )

    def _filter_data(self, data: list):
        filtered_data = []
        if not data:
            return filtered_data
        for book in data:
            title = book.get("title")
            if self.title_keyword.lower() in title.lower():
                filtered_data.append(book)

        return filtered_data

    def shutdown(self):
        logging.info("SIGTERM received. Initiating Graceful Shutdown.")
        self._shutting_down = True
        raise ShuttingDown

    def input_queue(self):
        return self.input_queue_name

    def output_queue(self):
        return self._output_queue

    def _handle_invalid_transaction_id(self, msg: Message):
        transaction_id = msg.get("transaction_id")
        sender = msg.get("sender")
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

    def controller_id(self):
        return self._controller_id

    def is_shutting_down(self):
        return self._shutting_down

    def _is_transaction_id_valid(self, msg: Message):
        transaction_id = msg.get("transaction_id")
        sender = msg.get("sender")
        expected_transaction_id = self._state.next_inbound_transaction_id(sender)

        return transaction_id == expected_transaction_id

    def _callback_title_filter(self, _: Goutong, msg: Message):
        sender = msg.get("sender")
        conn_id = msg.get("conn_id")
        queries = msg.get("queries")

        if not self._is_transaction_id_valid(msg):
            self._handle_invalid_transaction_id(msg)
            return

        # Send filtered data
        if filtered_books := self._filter_data(msg.get("data")):
            filtered_books = [self._columns_for_query1(b) for b in filtered_books]
            transaction_id = self._state.next_outbound_transaction_id(self._proxy_queue)

            msg_content = {
                "transaction_id": transaction_id,
                "conn_id": conn_id,
                "queries": queries,
                "data": filtered_books,
                "forward_to": [self.output_queue()],
            }
            crash_maybe()
            self._messaging.send_to_queue(self._proxy_queue, Message(msg_content))
            self._state.outbound_transaction_committed(self._proxy_queue)

        # Send End of File
        if msg.get("EOF"):
            transaction_id = self._state.next_outbound_transaction_id(self._proxy_queue)

            msg_content = {
                "transaction_id": transaction_id,
                "conn_id": conn_id,
                "EOF": True,
                "forward_to": [self.output_queue()],
                "queries": [1],
            }
            crash_maybe()
            self._messaging.send_to_queue(self._proxy_queue, Message(msg_content))
            self._state.outbound_transaction_committed(self._proxy_queue)

        self._state.inbound_transaction_committed(sender)
        crash_maybe()
        self._state.save_to_disk()
        crash_maybe()
        self._messaging.ack_delivery(msg.delivery_id)

    def _columns_for_query1(self, book: dict) -> dict:
        return {
            "title": book["title"],
            "publisher": book["publisher"],
            "categories": book["categories"],
        }


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

    state = TitleFilter.default_state(
        controller_id=controller_id,
        file_path=f"state/{controller_id}.json",
        temp_file_path=f"state/{controller_id}.tmp",
    )

    if os.path.exists(state.file_path):
        logging.info("Loading state from file...")
        state.update_from_file()

    messaging = Goutong(sender_id=controller_id)

    title_filter = TitleFilter(
        filter_config=filter_config,
        state=state,
        messaging=messaging,
        title_keyword=KEYWORD_Q1,
        output_queue=OUTPUT_Q1,
    )

    signal.signal(signal.SIGTERM, lambda sig, frame: title_filter.shutdown())
    title_filter.start()


if __name__ == "__main__":
    main()
