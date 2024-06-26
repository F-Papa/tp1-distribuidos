from collections import defaultdict
from enum import Enum
import random
import socket
import sys
import threading
from src.controllers.common.healthcheck_handler import HealthcheckHandler
from src.messaging.goutong import Goutong
from src.messaging.message import Message
import logging
import signal
import os

from src.controller_state.controller_state import ControllerState

from src.utils.config_loader import Configuration
from src.exceptions.shutting_down import ShuttingDown

UPPER_Q1 = 2023
LOWER_Q1 = 2000

UPPER_Q3_4 = 1999
LOWER_Q3_4 = 1990

OUTPUT_Q1 = "title_filter_queue"
OUTPUT_Q3_4_PREFIX = "review_joiner_books"

def crash_maybe():
    if random.random() < 0.001:
        logging.error("CRASHING..")
        sys.exit(1)

class ControlMessage(Enum):
    HEALTHCHECK = 6
    IM_ALIVE = 7

class DateFilter:
    FILTER_TYPE = "date_filter"
    CONTROL_PORT = 12347
    MSG_REDUNDANCY = 3

    def __init__(
        self,
        filter_config: Configuration,
        state: ControllerState,
        upper_q3_4: int,
        lower_q3_4: int,
        upper_q1: int,
        lower_q1: int,
        output_q1: str,
        output_q3_4_prefix: str,
    ):
        self._shutting_down = False
        self.filter_config = filter_config
        self._state = state
        self._input_queue = self.FILTER_TYPE + str(
            filter_config.get("FILTER_NUMBER")
        )
        self._proxy_queue = f"{self.FILTER_TYPE}_proxy"
        self.upper_q3_4 = upper_q3_4
        self.lower_q3_4 = lower_q3_4
        self.upper_q1 = upper_q1
        self.lower_q1 = lower_q1
        self._output_q1 = output_q1
        self._output_q3_4_prefix = output_q3_4_prefix
        self.controller_name = self.FILTER_TYPE + str(
            filter_config.get("FILTER_NUMBER")
        )
        self._messaging =  Goutong(sender_id=self.controller_name)

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
            if not self._shutting_down:
                self._messaging.set_callback(
                    self._input_queue, self.callback_filter, auto_ack=False
                )
                self._messaging.listen()

        except ShuttingDown:
            pass

        finally:
            logging.info("Shutting Down.")
            self._messaging.close()
            self._state.save_to_disk()

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

    def shutdown(self):
        logging.info("SIGTERM received. Initiating Graceful Shutdown.")
        self._shutting_down = True
        raise ShuttingDown

    def input_queue(self):
        return self._input_queue

    def output_queue_q1(self):
        return self._output_q1

    def output_queue_q3_4(self, conn_id: int):
        return self._output_q3_4_prefix
        return self._output_q3_4_prefix + str(conn_id)

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

    def _filter_data_q1(self, data: list) -> list:
        filtered_data = []
        if not data:
            return filtered_data

        for book in data:
            year = book.get("year")
            if self.lower_q1 <= year <= self.upper_q1:
                filtered_data.append(book)
        return filtered_data

    def _filter_data_q3_4(self, data: list) -> list:
        filtered_data = []
        if not data:
            return filtered_data
        for book in data:
            year = book.get("year")
            if self.lower_q3_4 <= year <= self.upper_q3_4:
                filtered_data.append(book)
        return filtered_data

    def _handle_invalid_transaction_id(self, msg: Message):
        transaction_id = msg.get("transaction_id")
        sender = msg.get("sender")
        expected_transaction_id = self._state.next_inbound_transaction_id(sender)

        if transaction_id < expected_transaction_id:
            logging.info(
                f"Received Duplicate Transaction {transaction_id} from {sender}: "
                + msg.marshal()[:100]
            )
            crash_maybe()
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
        sender = msg.get("sender")
        conn_id = msg.get("conn_id")
        output_q3_4 = self.output_queue_q3_4(conn_id)
        
        # Duplicate transaction
        if not self._is_transaction_id_valid(msg):
            self._handle_invalid_transaction_id(msg)
            return

        # Send filtered data to Query 1
        if filtered_books_q1 := self._filter_data_q1(msg.get("data")):
            filtered_books = [self._columns_for_query1(b) for b in filtered_books_q1]
            transaction_id = self._state.next_outbound_transaction_id(self._proxy_queue)

            msg_content = {
                "transaction_id": transaction_id,
                "conn_id": conn_id,
                "queries": [1],
                "data": filtered_books,
                "forward_to": [self.output_queue_q1()],
            }

            if msg.get("EOF"):
                msg_content["EOF"] = True

            crash_maybe()
            self._messaging.send_to_queue(self._proxy_queue, Message(msg_content))
            self._state.outbound_transaction_committed(self._proxy_queue)

        elif msg.get("EOF"):
            transaction_id = self._state.next_outbound_transaction_id(self._proxy_queue)
            msg_content = {
                "transaction_id": transaction_id,
                "conn_id": conn_id,
                "queries": [1],
                "EOF": True,
                "forward_to": [self.output_queue_q1()],
            }
            crash_maybe()
            self._messaging.send_to_queue(self._proxy_queue, Message(msg_content))
            self._state.outbound_transaction_committed(self._proxy_queue)
    
        # Send filtered data to Query 3,4
        if filtered_books_q3_4 := self._filter_data_q3_4(msg.get("data")):
            filtered_books = [
                self._columns_for_query3_4(b) for b in filtered_books_q3_4
            ]
            transaction_id = self._state.next_outbound_transaction_id(self._proxy_queue)
            msg_content = {
                "transaction_id": transaction_id,
                "conn_id": conn_id,
                "queries": [3, 4],
                "data": filtered_books,
                "forward_to": [output_q3_4],
            }

            if msg.get("EOF"):
                msg_content["EOF"] = True
            
            crash_maybe()
            self._messaging.send_to_queue(self._proxy_queue, Message(msg_content))
            self._state.outbound_transaction_committed(self._proxy_queue)

        elif msg.get("EOF"):
            transaction_id = self._state.next_outbound_transaction_id(self._proxy_queue)
            msg_content = {
                "transaction_id": transaction_id,
                "conn_id": conn_id,
                "queries": [3, 4],
                "EOF": True,
                "forward_to": [output_q3_4],
            }
            crash_maybe()
            self._messaging.send_to_queue(self._proxy_queue, Message(msg_content))
            self._state.outbound_transaction_committed(self._proxy_queue)
  
        self._state.inbound_transaction_committed(sender)
        crash_maybe()
        self._state.save_to_disk()
        crash_maybe()
        self._messaging.ack_delivery(msg.delivery_id)

    def controller_id(self):
        return self.controller_name

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

    state = DateFilter.default_state(
        controller_id=controller_id,
        file_path=f"state/{controller_id}.json",
        temp_file_path=f"state/temp_{controller_id}.json",
    )

    if os.path.exists(state.file_path):
        state.update_from_file()

    messaging = Goutong(sender_id=controller_id)

    filter = DateFilter(
        filter_config=filter_config,
        state=state,
        upper_q3_4=UPPER_Q3_4,
        lower_q3_4=LOWER_Q3_4,
        upper_q1=UPPER_Q1,
        lower_q1=LOWER_Q1,
        output_q1=OUTPUT_Q1,
        output_q3_4_prefix=OUTPUT_Q3_4_PREFIX,
    )

    signal.signal(signal.SIGTERM, lambda sig, frame: filter.shutdown())
    
    controller_thread = threading.Thread(target=filter.start)
    controller_thread.start()

    # HEALTCHECK HANDLING
    healthcheck_handler = HealthcheckHandler(filter)
    healthcheck_handler.start()



if __name__ == "__main__":
    main()
