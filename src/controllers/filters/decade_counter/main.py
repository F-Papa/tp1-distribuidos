from collections import defaultdict
import json
import os
from typing import Union
from src.messaging.goutong import Goutong
from src.messaging.message import Message
from src.controller_state.controller_state import ControllerState

import logging
import signal

from src.utils.config_loader import Configuration
from src.exceptions.shutting_down import ShuttingDown

INPUT_QUEUE = "decade_counter_queue"
FILTER_TYPE = "decade_counter"

OUTPUT_QUEUE_PREFIX = "results_"

class DecadeCounter:
    CONTROLLER_NAME = "decade_counter"

    def __init__(self, config, state: ControllerState, messaging: Goutong, output_queues: dict
    ):
        self._filter_number = config.get("FILTER_NUMBER")
        self._shutting_down = False
        self._state = state
        self._messaging = messaging
        self._input_queue = f"{INPUT_QUEUE}"
        self._output_queues = output_queues

        # por cada conexion se guarda un dict: cuya clave es autor y valor un set de decadas

    @classmethod
    def default_state(
        cls, controller_id: str, file_path: str, temp_file_path: str
    ) -> ControllerState:
        extra_fields = {
            "saved_counts": defaultdict(lambda: defaultdict(lambda: set())),
            "ongoing_connections": set(),
        }

        return ControllerState(
            controller_id=controller_id,
            file_path=file_path,
            temp_file_path=temp_file_path,
            extra_fields=extra_fields,
        )
    
    # region: Command methods
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

    def _is_transaction_id_valid(self, msg: Message):
        transaction_id = msg.get("transaction_id")
        sender = msg.get("sender")
        expected_transaction_id = self._state.next_inbound_transaction_id(sender)

        return transaction_id == expected_transaction_id

    def _get_10_decade_authors(self, conn_id: str):
        saved_counts = self._state.get("saved_counts")
        decades_by_author = saved_counts[conn_id]
        ten_decade_authors = [author for author in decades_by_author\
                               if len(decades_by_author[author]) >= 10]
        return ten_decade_authors

    def _send_results(self, conn_id: str):
        ten_decade_authors = self._get_10_decade_authors(conn_id)
        msg = Message({"conn_id": conn_id, "queries": [2], "data": ten_decade_authors})
        
        output_queue = OUTPUT_QUEUE_PREFIX + conn_id
        self._messaging.send_to_queue(output_queue, msg)
        self._state.outbound_transaction_committed(output_queue)
        logging.debug(f"Sent Data to: {output_queue}")

    def _send_eof(self, conn_id: str):
        msg = Message({"conn_id": conn_id, "EOF": True, "queries": [2]})
        output_queue = OUTPUT_QUEUE_PREFIX + conn_id
        self._messaging.send_to_queue(output_queue, msg)
        logging.debug(f"Sent EOF to: {output_queue}")

    def _callback_filter(self, _: Goutong, msg: Message):
        logging.info(f"1")
        # Validate transaction_id
        if not self._is_transaction_id_valid(msg):
            logging.info(f"2")
            self._handle_invalid_transaction_id(msg)
            return
        
        conn_id = msg.get("conn_id")
        conn_id_str = str(conn_id)
        sender = msg.get("sender")

        ongoing_connections = self._state.get("ongoing_connections")
        saved_counts = self._state.get("saved_counts")
        ongoing_connections.add(conn_id_str) #es un Set(), si ya exitse no la agrega

        logging.info(f"4")
        if msg.get("EOF"):  #calculate results, send results, send EOF
            logging.info(f"5")
            logging.info(f"EOF received from {conn_id_str}")
            self._send_results(conn_id_str)
            self._send_eof(conn_id_str)
            self._state.get("ongoing_connections").remove(conn_id_str)
            self._state.get("saved_counts").pop(conn_id_str)
        else:   #add decades and authors to state
            books = msg.get("data")
            for book in books:
                decade = book.get("decade")
                for author in book.get("authors"):
                    saved_counts[conn_id_str][author].add(decade)
            logging.info(f"6")
            logging.debug(f"Authors and Decs: {saved_counts[conn_id_str]}")

        self._state.inbound_transaction_committed(sender)
        #self._state.save_to_disk()
        self._messaging.ack_delivery(msg.delivery_id)

    def start(self):
        logging.info("Starting Decade Counter")
        try:
            if not self._shutting_down:
                self._messaging.set_callback(
                    self._input_queue, self._callback_filter, auto_ack=False
                )
                self._messaging.listen()
        except ShuttingDown:
            pass
        finally:
            logging.info("Shutting Down.")
            self._messaging.close()

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
        "FILTER_NUMBER": int,
    }

    filter_config = Configuration.from_file(required, "config.ini")
    filter_config.update_from_env()
    filter_config.validate()

    config_logging(filter_config.get("LOGGING_LEVEL"))
    logging.info(filter_config)

    controller_id = f"{DecadeCounter.CONTROLLER_NAME}_{filter_config.get('FILTER_NUMBER')}"
    state_file_path = f"state/{controller_id}.json"
    temp_file_path = f"state/{controller_id}.tmp"

    state = DecadeCounter.default_state(controller_id, state_file_path, temp_file_path)

    if os.path.exists(state_file_path):
        logging.info("State file found. Loading state.")
        state.update_from_file()
        to_show = ""
        for conn in state.get("saved_counts").keys():
            to_show += f"{len(state.get('saved_counts')[conn])} books from conn {conn}\n"
        logging.info(to_show)

    output_queues = {(2,): {"name": OUTPUT_QUEUE_PREFIX, "is_prefix": True},}

    messaging = Goutong(sender_id=FILTER_TYPE)
    decade_counter = DecadeCounter(filter_config, state, messaging, output_queues)
    decade_counter.start()

if __name__ == "__main__":
    main()