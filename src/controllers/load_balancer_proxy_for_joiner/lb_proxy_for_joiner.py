"""
A Barrier controller that distributes data to multiple filters in a round-robin fashion. 
It also works as a threading barrier, forwarding EOF messages to the next filter in the chain once all filters have processed the data.
"""

from collections import defaultdict
import os
import random
import sys
import threading
import time
from src.controllers.common.healthcheck_handler import HealthcheckHandler
from src.utils.config_loader import Configuration
import logging
import signal

from src.messaging.message import Message
from src.messaging.goutong import Goutong
from src.exceptions.shutting_down import ShuttingDown
from src.controller_state.controller_state import ControllerState

INPUT_QUEUE_BOOKS = "review_joiner_books"
INPUT_QUEUE_REVIEWS = "review_joiner_reviews"
FILTER_TYPE = "review_joiner"
CONTROLLER_ID = "review_joiner_proxy"


def crash_maybe(): #402254
    if random.random() < 0.00001:
        logging.error("CRASHING..")
        exit(1)


class LoadBalancerProxyForJoiner:

    def __init__(
        self, config: Configuration, state: ControllerState
    ):
        self.controller_name = CONTROLLER_ID
        self.barrier_config = config
        self._messaging = Goutong(sender_id=self.controller_name)
        self._state = state

        # Graceful Shutdown Handling
        self.shutting_down = False
        self._input_queue_books = INPUT_QUEUE_BOOKS
        self._input_queue_reviews = INPUT_QUEUE_REVIEWS
        self._input_queue_proxy = FILTER_TYPE + "_proxy"
        self._filter_count = config.get("FILTER_COUNT")
        self._filter_queues = []
        for i in range(1, self._filter_count + 1):
            books_queue = f"{FILTER_TYPE}_{i}_books"
            reviews_queue_prefix = f"{FILTER_TYPE}_{i}_reviews_conn_"
            self._filter_queues.append(
                {"books": books_queue, "reviews": reviews_queue_prefix}
            )

        logging.info("Filter Queues: " + str(self._filter_queues))

    def controller_id(self):
        return self.controller_name

    def start(self):
        try:
            if not self.shutting_down:
                # Set callbacks
                self._messaging.set_callback(
                    self._input_queue_books,
                    self._msg_from_other_cluster,
                    auto_ack=False,
                )

                self._messaging.set_callback(
                    self._input_queue_reviews,
                    self._msg_from_other_cluster,
                    auto_ack=False,
                )

                self._messaging.set_callback(
                    self._input_queue_proxy, self._msg_from_controllers, auto_ack=False
                )

                self._messaging.listen()

        except ShuttingDown:
            logging.debug("Shutting Down Message Received Via Broadcast")

        self._messaging.close()
        logging.info("Shutting Down.")

    def filter_queues(self) -> list[str]:
        return self._filter_queues

    @classmethod
    def default_state(
        cls, controller_id: str, file_path: str, temp_file_path: str
    ) -> ControllerState:
        extra_fields = {
            "eof_received": {},
        }

        return ControllerState(
            controller_id=controller_id,
            file_path=file_path,
            temp_file_path=temp_file_path,
            extra_fields=extra_fields,
        )

    def _separate_data_by_queue(self, source_queue: str, conn_id: int, data: list):
        separated_data = defaultdict(list)

        if source_queue == self._input_queue_books:
            for element in data:
                hash_idx = hash(element["title"]) % len(self._filter_queues)
                output_queue = self._filter_queues[hash_idx]["books"]
                separated_data[output_queue].append(element)

        elif source_queue == self._input_queue_reviews:
            for element in data:
                hash_idx = hash(element["title"]) % len(self._filter_queues)
                output_queue = self._filter_queues[hash_idx]["reviews"] + str(conn_id)
                separated_data[output_queue].append(element)

        return separated_data

    def _msg_from_controllers(self, _: Goutong, msg: Message):
        # Duplicate transaction
        if not self._is_transaction_id_valid(msg):
            self._handle_invalid_transaction_id(msg)
            return

        conn_id = msg.get("conn_id")
        sender = msg.get("sender")
        queries = msg.get("queries")

        conn_id_str = str(conn_id)
        queries_str = str(queries)
        eof_received = self._state.get("eof_received")

        if conn_id_str not in self._state.get("eof_received"):
            eof_received[conn_id_str] = {}

        if queries_str not in eof_received[conn_id_str]:
            eof_received[conn_id_str][queries_str] = {}

        if data := msg.get("data"):
            for queue in msg.get("forward_to"):
                transaction_id = self._state.next_outbound_transaction_id(queue)
                msg_body = {
                    "transaction_id": transaction_id,
                    "conn_id": conn_id,
                    "queries": queries,
                    "data": data,
                }
                crash_maybe()
                self._messaging.send_to_queue(queue, Message(msg_body))
                self._state.outbound_transaction_committed(queue)

        if msg.get("EOF"):
            eof_received[conn_id_str][queries_str][sender] = True
            
            for queue_pair in self._filter_queues:
                prefix = queue_pair["reviews"]
                queue_to_remove = prefix + str(conn_id)
                
                try:
                    self._messaging.delete_queue(queue_to_remove)
                    logging.error(f"Queue {queue_to_remove} deleted.")
                except:
                    logging.info(f"Queue: {queue_to_remove} already deleted.")
                

            if len(eof_received[conn_id_str][queries_str]) == len(self._filter_queues):
                for queue in msg.get("forward_to"):
                    transaction_id = self._state.next_outbound_transaction_id(queue)
                    msg_body = {
                        "transaction_id": transaction_id,
                        "conn_id": conn_id,
                        "queries": queries,
                        "EOF": True,
                    }
                    crash_maybe()
                    self._messaging.send_to_queue(queue, Message(msg_body))
                    self._state.outbound_transaction_committed(queue)
            self._state.set("eof_received", eof_received)

        self._state.inbound_transaction_committed(sender)
        crash_maybe()
        self._state.save_to_disk()
        crash_maybe()
        self._messaging.ack_delivery(msg.delivery_id)

    def _is_transaction_id_valid(self, msg: Message):
        transaction_id = msg.get("transaction_id")
        sender = msg.get("sender")
        expected_transaction_id = self._state.next_inbound_transaction_id(sender)

        return transaction_id == expected_transaction_id

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

    def _msg_from_other_cluster(self, _: Goutong, msg: Message):
        queries = msg.get("queries")
        conn_id = msg.get("conn_id")
        sender = msg.get("sender")

        # Duplicate transaction
        if not self._is_transaction_id_valid(msg):
            self._handle_invalid_transaction_id(msg)
            return

        # Forward data to one of the filters
        if data := msg.get("data"):
            self._dispatch_data(
                data=data, conn_id=conn_id, queries=queries, source=msg.queue_name
            )
        if msg.get("EOF"):
            self._forward_end_of_file(
                conn_id=conn_id, queries=queries, source=msg.queue_name
            )

        self._state.inbound_transaction_committed(sender)
        crash_maybe()
        self._state.save_to_disk()
        crash_maybe()
        self._messaging.ack_delivery(msg.delivery_id)

    def _forward_end_of_file(self, conn_id: int, queries: list[int], source: str):
        output_queues = []

        if source == self._input_queue_books:
            for queue_pair in self._filter_queues:
                output_queues.append(queue_pair["books"])
        else:
            for queue_pair in self._filter_queues:
                prefix = queue_pair["reviews"]
                output_queues.append(prefix + str(conn_id))

        for queue in output_queues:
            transaction_id = self._state.next_outbound_transaction_id(queue)
            msg_body = {
                "transaction_id": transaction_id,
                "conn_id": conn_id,
                "queries": queries,
                "EOF": True,
            }
            crash_maybe()
            self._messaging.send_to_queue(queue, Message(msg_body))
            self._state.outbound_transaction_committed(queue)

    def _dispatch_data(self, data: list, conn_id: int, queries: list[int], source: str):

        data_by_queue = self._separate_data_by_queue(source, conn_id, data)

        for queue, data_for_queue in data_by_queue.items():
            transaction_id = self._state.next_outbound_transaction_id(queue)

            msg_body = {
                "transaction_id": transaction_id,
                "conn_id": conn_id,
                "queries": queries,
                "data": data_for_queue,
            }
            crash_maybe()
            self._messaging.send_to_queue(queue, Message(msg_body))
            self._state.outbound_transaction_committed(queue)

    def shutdown(self, messaging: Goutong):
        logging.info("SIGTERM received. Initiating Graceful Shutdown.")
        self.shutting_down = True


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
        "FILTER_COUNT": int,
        "LOGGING_LEVEL": str,
    }
    barrier_config = Configuration.from_env(required, "config.ini")
    barrier_config.validate()

    config_logging(barrier_config.get("LOGGING_LEVEL"))
    controller_id = f"{FILTER_TYPE}_lb_proxy"

    state = LoadBalancerProxyForJoiner.default_state(
        controller_id=controller_id,
        file_path="state.json",
        temp_file_path="state_temp.json",
    )

    if os.path.exists(state.file_path):
        #logging.info("Loading state from file...")
        state.update_from_file()
    else:
        logging.info("no state file")

    logging.info(barrier_config)
    load_balancer = LoadBalancerProxyForJoiner(barrier_config, state)
    controller_thread = threading.Thread(target=load_balancer.start)
    controller_thread.start()

    # HEALTCHECK HANDLING
    healthcheck_handler = HealthcheckHandler(load_balancer)
    healthcheck_handler.start()


if __name__ == "__main__":
    main()
