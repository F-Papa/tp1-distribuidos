from collections import defaultdict
from enum import Enum
import random
import socket
import sys
import threading
from src.controllers.common.healthcheck_handler import HealthcheckHandler
from src.messaging.goutong import Goutong
from src.utils.config_loader import Configuration
import logging
import signal
import os

from src.controller_state.controller_state import ControllerState


from src.messaging.message import Message


EOF_QUEUE = "category_filter_eof"

CATEGORY_Q1 = "Computers"
CATEGORY_Q5 = "Fiction"

OUTPUT_Q1_PREFIX = "results_"
OUTPUT_Q5_PREFIX = "review_joiner_books"

def crash_maybe():
    if random.random() < 0.0005:
        logging.error("CRASHING..")
        sys.exit(1)


class ControlMessage(Enum):
    HEALTHCHECK = 6
    IM_ALIVE = 7

class CategoryFilter:
    FILTER_TYPE = "category_filter"
    CONTROL_PORT = 12347
    MSG_REDUNDANCY = 3

    def __init__(
        self,
        filter_config: Configuration,
        state: ControllerState,
        category_q1: str,
        output_queue_q1_prefix: str,
        category_q5: str,
        output_queue_q5_prefix,
    ):
        self._shutting_down = False
        self._state = state

        self._config = filter_config
        self._input_queue = self.FILTER_TYPE + str(filter_config.get("FILTER_NUMBER"))
        self._category_q1 = category_q1
        self._proxy_queue = f"{self.FILTER_TYPE}_proxy"
        self._output_queue_q1_prefix = output_queue_q1_prefix
        self._category_q5 = category_q5
        self._output_queue_q5_prefix = output_queue_q5_prefix
        self.controller_name = self.FILTER_TYPE + str(
            filter_config.get("FILTER_NUMBER")
        )
        self._messaging = Goutong(sender_id=self.controller_name)

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

    def controller_id(self):
        return self.controller_name

    # HEALTHCHECK HANDLING
    def send_healthcheck_response(self, address, seq_num):
        message = (
            f"{seq_num},{self.controller_name},{ControlMessage.IM_ALIVE.value}$"
        )
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        logging.info(f"Sending IM ALIVE to {address}")
        logging.debug(f"IM ALIVE message: {message}")

        for _ in range(self.MSG_REDUNDANCY):
            sock.sendto(message.encode(), (address, self.CONTROL_PORT))

    def start(self):
        # Main Flow
        try:
            self._messaging.set_callback(
                self._input_queue, self.callback_filter, auto_ack=False
            )
            self._messaging.listen()
        except:
            if self._shutting_down:
                pass
        logging.info("Shutting Down.")
        self._state.save_to_disk()

    def input_queue(self):
        return self._input_queue

    def output_queue_q1(self, conn_id: int):
        return self._output_queue_q1_prefix + str(conn_id)

    def output_queue_q5(self, conn_id: int):
        return self._output_queue_q5_prefix
        return self._output_queue_q5_prefix + str(conn_id)

    def filter_data(self, data: list, category: str):
        filtered_data = []
        for book in data:
            categories = book.get("categories")

            if category in categories:
                filtered_data.append(book)

        return filtered_data

    # Graceful Shutdown
    def shutdown(self):
        self._shutting_down = True
        self._messaging.close()


    def _handle_invalid_transaction_id(self, msg: Message):
        transaction_id = msg.get("transaction_id")
        sender = msg.get("sender")
        expected_transaction_id = self._state.next_inbound_transaction_id(sender)

        if transaction_id < expected_transaction_id:
            logging.info(
                f"Received Duplicate Transaction {transaction_id} from {sender}: "
                + msg.marshal()[:100]
            )
            # crash_maybe()
            self._messaging.ack_delivery(msg.delivery_id)

        elif transaction_id > expected_transaction_id:
            self._messaging.requeue(msg)
            logging.info(
                f"Requeueing out of order {transaction_id}, expected {str(expected_transaction_id)}"
            )


    def _is_transaction_id_valid(self, msg: Message):
        transaction_id = msg.get("transaction_id")
        sender = msg.get("sender")
        expected_transaction_id = self._state.next_inbound_transaction_id(sender)

        return transaction_id == expected_transaction_id



    def callback_filter(self, _: Goutong, msg: Message):
        if not self._is_transaction_id_valid(msg):
            self._handle_invalid_transaction_id(msg)
            return

        queries = msg.get("queries")

        # Send filtered data to Query 1
        if 1 in queries:
            self.callback_aux_q1(msg)

        elif 5 in queries:
            self.callback_aux_q5(msg)

    def callback_aux_q5(self, msg: Message):
        conn_id = msg.get("conn_id")
        output_queue_q5 = self.output_queue_q5(conn_id)
        filtered_books = self._filter_by_category(msg.get("data"), self._category_q5)
        filtered_books = [self._columns_for_query5(b) for b in filtered_books]
        
        transaction_id = self._state.next_outbound_transaction_id(self._proxy_queue)
        msg_content = {
            "transaction_id": transaction_id,
            "conn_id": conn_id,
            "queries": [5],
            "forward_to": [output_queue_q5]
        }

        if filtered_books:
            msg_content["data"] = filtered_books

        if msg.get("EOF"):
            msg_content["EOF"] = True

        crash_maybe()
        self._messaging.send_to_queue(self._proxy_queue, Message(msg_content))
        self._state.outbound_transaction_committed(self._proxy_queue)
        self._state.inbound_transaction_committed(msg.get("sender"))
        crash_maybe()
        self._state.save_to_disk()
        crash_maybe()
        self._messaging.ack_delivery(msg.delivery_id)      


    def callback_aux_q1(self, msg: Message):
        conn_id = msg.get("conn_id")
        output_queue_q1 = self.output_queue_q1(conn_id)


        filtered_books_q1 = self._filter_by_category(msg.get("data"), self._category_q1)
        filtered_books_q1 = [self._columns_for_query1(b) for b in filtered_books_q1]
        
        transaction_id = self._state.next_outbound_transaction_id(self._proxy_queue)
        msg_content = {
            "transaction_id": transaction_id,
            "conn_id": conn_id,
            "queries": [1],
            "forward_to": [output_queue_q1]
        }
        if filtered_books_q1:
            msg_content["data"] = filtered_books_q1

        if msg.get("EOF"):
            msg_content["EOF"] = True

        crash_maybe()
        self._messaging.send_to_queue(self._proxy_queue, Message(msg_content))
        self._state.outbound_transaction_committed(self._proxy_queue)
        self._state.inbound_transaction_committed(msg.get("sender"))
        crash_maybe()
        self._messaging.ack_delivery(msg.delivery_id)

    def _filter_by_category(self, data: list, category: str) -> list:
        filtered_data = []
        if not data:
            return filtered_data

        for book in data:
            categories = book.get("categories")

            if category in categories:
                filtered_data.append(book)

        return filtered_data

    def _columns_for_query1(self, book: dict) -> dict:
        return {
            "title": book.get("title"),
            "publisher": book["publisher"],
        }

    def _columns_for_query5(self, book: dict) -> dict:
        return {
            "title": book.get("title"),
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
    pika_logger.setLevel(logging.CRITICAL)


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

    state = CategoryFilter.default_state(
        controller_id=controller_id,
        file_path=f"state/{controller_id}.json",
        temp_file_path=f"state/{controller_id}.tmp",
    )

    if os.path.exists(state.file_path):
        #logging.info("Loading state from file...")
        state.update_from_file()


    category_filter = CategoryFilter(
        filter_config=filter_config,
        state=state,
        category_q1=CATEGORY_Q1,
        output_queue_q1_prefix=OUTPUT_Q1_PREFIX,
        category_q5=CATEGORY_Q5,
        output_queue_q5_prefix=OUTPUT_Q5_PREFIX,
    )


    controller_thread = threading.Thread(target=category_filter.start)
    healthcheck_handler = HealthcheckHandler(category_filter)
    signal.signal(signal.SIGTERM, lambda sig, frame: healthcheck_handler.shutdown())

    controller_thread.start()
    healthcheck_handler.start()



if __name__ == "__main__":
    main()
