from collections import defaultdict
import json
import os
import threading
import time
from typing import Union
from src.controllers.common.healthcheck_handler import HealthcheckHandler
from src.messaging.goutong import Goutong
from src.messaging.message import Message
from src.controller_state.controller_state import ControllerState

import logging
import signal
import sys
import random

from src.utils.config_loader import Configuration

INPUT_QUEUE_PREFIX = "decade_counter"
FILTER_TYPE = "decade_counter"

OUTPUT_QUEUE_PREFIX = "results_"


def crash_maybe():
    if random.random() < 0.00015:
        logging.error("CRASHING..")
        sys.exit(1)

class DecadeCounter:
    CONTROLLER_NAME = "decade_counter"

    def __init__(self, config, state: ControllerState, output_queues: dict):
        self._filter_number = config.get("FILTER_NUMBER")
        self._shutting_down = False
        self._input_queue = f"{INPUT_QUEUE_PREFIX}{self._filter_number}"
        self._proxy_queue = f"{INPUT_QUEUE_PREFIX}_proxy"
        self._output_queues = output_queues
        self.controller_name = f"{DecadeCounter.CONTROLLER_NAME}{self._filter_number}"
        self.unacked_msg_limit = config.get("UNACKED_MSG_LIMIT")
        self.unacked_time_limit_in_seconds = config.get("UNACKED_TIME_LIMIT_IN_SECONDS")
        self.unacked_msgs = []
        self.unacked_msg_count = 0
        self.time_of_last_commit = time.time()

        self._state = state
        self._denormalize_state()
        self._messaging =  Goutong(sender_id=self.controller_name)

    @classmethod
    def default_state(
        cls, controller_id: str, file_path: str, temp_file_path: str
    ) -> ControllerState:
        extra_fields = {
            # por cada conexion se guarda un dict: cuya clave es autor y valor un set de decadas
            "saved_counts": defaultdict(lambda: defaultdict(set)),
            "ongoing_connections": set(),
        }

        return ControllerState(
            controller_id=controller_id,
            file_path=file_path,
            temp_file_path=temp_file_path,
            extra_fields=extra_fields,
        )
    
    def shutdown(self):
        self._shutting_down = True
        self._messaging.close()

    # Devuelve las saved_counts pero con los Set() de decadas casteados a lista
    def _normalize_counts(self, saved_counts):
        normalized_counts = {
            conn_id: {
                author: list(decades)
                for author, decades in authors_and_decades.items()
            }
            for conn_id, authors_and_decades in saved_counts.items()
        }
        return normalized_counts

    # Devuelve las saved_counts pero con las list() de decadas casteadas a Set
    def _denormalize_counts(self, saved_counts):
        denormalized_counts = defaultdict(lambda: defaultdict(set))
        for conn_id, authors_and_decades in saved_counts.items():
            for author, decades in authors_and_decades.items():
                denormalized_counts[conn_id][author] = set(decades)
        return denormalized_counts

    def _normalize_state(self):
        """Normalize state to JSON friendly format used for persistance"""
        saved_counts = self._state.get("saved_counts")
        saved_counts = self._normalize_counts(saved_counts)
        self._state.set("saved_counts", saved_counts)
        #logging.debug(f"{saved_counts}")

        ongoing_connections = self._state.get("ongoing_connections")
        ongoing_connections = list(ongoing_connections)
        self._state.set("ongoing_connections", ongoing_connections)

    def _denormalize_state(self):
        """Denormalize state to Sets() and defaultdict format used for program flow"""
        saved_counts = self._state.get("saved_counts")
        saved_counts = self._denormalize_counts(saved_counts)
        self._state.set("saved_counts", saved_counts)

        ongoing_connections = self._state.get("ongoing_connections")
        ongoing_connections = set(ongoing_connections)
        self._state.set("ongoing_connections", ongoing_connections)

    def _save_state(self):
        self._normalize_state()
        self._state.save_to_disk()
        self._denormalize_state()

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

    def _get_10_decade_authors(self, conn_id: str):
        saved_counts = self._state.get("saved_counts")
        decades_by_author = saved_counts[conn_id]
        ten_decade_authors = [author for author in decades_by_author\
                               if len(decades_by_author[author]) >= 10]
        return ten_decade_authors

    def _send_results(self, conn_id: str):
        ten_decade_authors = self._get_10_decade_authors(conn_id)
        output_queue = OUTPUT_QUEUE_PREFIX + conn_id
        transaction_id = self._state.next_outbound_transaction_id(self._proxy_queue)
        msg = Message({"conn_id": conn_id, "queries": [2], "data": ten_decade_authors, "forward_to": [output_queue], "transaction_id": transaction_id})
        crash_maybe()
        self._messaging.send_to_queue(self._proxy_queue, msg)
        self._state.outbound_transaction_committed(self._proxy_queue)
        logging.debug(f"Sent Data to: {self._proxy_queue}")

    def _send_eof(self, conn_id: str):
        output_queue = OUTPUT_QUEUE_PREFIX + conn_id
        transaction_id = self._state.next_outbound_transaction_id(self._proxy_queue)
        msg = Message({"conn_id": conn_id, "EOF": True, "queries": [2], "forward_to": [output_queue], "transaction_id": transaction_id})
        crash_maybe()
        self._messaging.send_to_queue(self._proxy_queue, msg)
        self._state.outbound_transaction_committed(self._proxy_queue)
        logging.debug(f"Sent EOF to: {self._proxy_queue}")

    def _callback_filter(self, _: Goutong, msg: Message):
        # Validate transaction_id
        if not self._is_transaction_id_valid(msg):
            self._handle_invalid_transaction_id(msg)
            return
        
        
        conn_id = msg.get("conn_id")
        conn_id_str = str(conn_id)
        sender = msg.get("sender")
        saved_counts = self._state.get("saved_counts")
        

        ongoing_connections = self._state.get("ongoing_connections")
        ongoing_connections.add(conn_id_str) #es un Set(), si ya exitse no la agrega

        if msg.get("EOF"):  #calculate results, send results, send EOF
            # logging.info(f"EOF received from {conn_id_str}")
            self._send_results(conn_id_str)
            self._send_eof(conn_id_str)

            #update state
            ongoing_connections.remove(conn_id_str)
            self._state.set("ongoing_connections", ongoing_connections)
            saved_counts.pop(conn_id_str)
            self._state.set("saved_counts", saved_counts)
        else:   #add decades and authors to state
            books = msg.get("data")
            for book in books:
                decade = book.get("decade")
                for author in book.get("authors"):
                    saved_counts[conn_id_str][author].add(decade)
            self._state.set("saved_counts", saved_counts)
            logging.debug(f"Authors and Decs: {saved_counts[conn_id_str]}")

        
        self._state.inbound_transaction_committed(sender)
        self.unacked_msg_count += 1
        self.unacked_msgs.append(msg.delivery_id)

        if (self.unacked_msg_count > self.unacked_msg_limit or msg.get("EOF")):
            
            # logging.info(f"Committing to disk | Unacked Msgs.: {self.unacked_msg_count}")

            crash_maybe()
            self._save_state()
            
            self.time_of_last_commit = time.time()
            
            for delivery_id in self.unacked_msgs:
                crash_maybe()
                self._messaging.ack_delivery(delivery_id)

            self.unacked_msg_count = 0
            self.unacked_msgs.clear()

    def controller_id(self):
        return self.controller_name

    def start(self):
        logging.info("Starting Decade Counter")
        try:
            self._messaging.set_callback(
                self._input_queue, self._callback_filter, auto_ack=False
            )
            self._messaging.listen()
        except:
            if self._shutting_down:
                pass
        logging.info("Shutting Down.")
        self._save_state()

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
        "FILTER_NUMBER": int,
        "UNACKED_MSG_LIMIT": int,
        "UNACKED_TIME_LIMIT_IN_SECONDS": int,
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
            to_show += f"{len(state.get('saved_counts')[conn])} authors with decades from conn {conn}\n"
        logging.info(to_show)

    output_queues = {(2,): {"name": OUTPUT_QUEUE_PREFIX, "is_prefix": True},}

    decade_counter = DecadeCounter(filter_config, state, output_queues)
    healthcheck_handler = HealthcheckHandler(decade_counter)
    signal.signal(signal.SIGTERM, lambda sig, frame: healthcheck_handler.shutdown())

    controller_thread = threading.Thread(target=decade_counter.start)
    controller_thread.start()
    healthcheck_handler.start()

if __name__ == "__main__":
    main()