"""
A Barrier controller that distributes data to multiple filters in a round-robin fashion. 
It also works as a threading barrier, forwarding EOF messages to the next filter in the chain once all filters have processed the data.
"""

from collections import defaultdict
from enum import Enum
import os
import socket
import threading
from src.controllers.common.healthcheck_handler import HealthcheckHandler
from src.utils.config_loader import Configuration
import logging
import signal

from src.messaging.message import Message
from src.messaging.goutong import Goutong
from src.exceptions.shutting_down import ShuttingDown
from src.controller_state.controller_state import ControllerState

class ControlMessage(Enum):
    HEALTHCHECK = 6
    IM_ALIVE = 7

class LoadBalancerProxy:
    CONTROL_PORT = 12347
    MSG_REDUNDANCY = 3

    def __init__(
        self, config: Configuration, messaging: Goutong, state: ControllerState
    ):
        self.barrier_config = config
        self._messaging = messaging
        self._state = state

        self.controller_name = config.get("FILTER_TYPE") + "_proxy"

        # Graceful Shutdown Handling
        self.shutting_down = False

        self._is_proxy = config.get("IS_PROXY")

        self._input_queues_lb = config.get("INPUT_QUEUES").split()
        self._input_queue_proxy = config.get("FILTER_TYPE") + "_proxy"

        self._key_to_hash = config.get("KEY_TO_HASH")

        self._filter_queues = []
        for i in range(1, config.get("FILTER_COUNT") + 1):
            queue_name = config.get("FILTER_TYPE") + str(i)
            self._filter_queues.append(queue_name)

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
        try:
            if not self.shutting_down:
                # Set callbacks
                for queue in self._input_queues_lb:
                    self._messaging.set_callback(
                        queue, self._msg_from_other_cluster, auto_ack=False
                    )
                
                if self._is_proxy:
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
                self._messaging.send_to_queue(queue, Message(msg_body))
                self._state.outbound_transaction_committed(queue)
        
        if msg.get("EOF"):
            eof_received[conn_id_str][queries_str][sender] = True

            if len(eof_received[conn_id_str][queries_str]) == len(self._filter_queues):
                for queue in msg.get("forward_to"):
                    transaction_id = self._state.next_outbound_transaction_id(queue)
                    msg_body = {
                        "transaction_id": transaction_id,
                        "conn_id": conn_id,
                        "queries": queries,
                        "EOF": True,
                    }
                    self._messaging.send_to_queue(queue, Message(msg_body))
                    self._state.outbound_transaction_committed(queue)
            self._state.set("eof_received", eof_received)
        
        self._state.inbound_transaction_committed(sender)
        self._state.save_to_disk()
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
                data=data, conn_id=conn_id, queries=queries
            )
        if msg.get("EOF"):
            self._forward_end_of_file(conn_id=conn_id, queries=queries)

        self._state.inbound_transaction_committed(sender)
        self._state.save_to_disk()
        self._messaging.ack_delivery(msg.delivery_id)

    def _forward_end_of_file(self, conn_id: int, queries: list[int]):
        if self._key_to_hash != "conn_id":
            for queue in self._filter_queues:
                transaction_id = self._state.next_outbound_transaction_id(queue)
                msg_body = {
                    "transaction_id": transaction_id,
                    "conn_id": conn_id,
                    "queries": queries,
                    "EOF": True,
                }
                self._messaging.send_to_queue(queue, Message(msg_body))
                self._state.outbound_transaction_committed(queue)
        else:
            hashed_idx = hash(conn_id) % len(self._filter_queues)
            queue = self._filter_queues[hashed_idx]
            transaction_id = self._state.next_outbound_transaction_id(queue)
            msg_body = {
                    "transaction_id": transaction_id,
                    "conn_id": conn_id,
                    "queries": queries,
                    "EOF": True,
                }
            self._messaging.send_to_queue(queue, Message(msg_body))
            self._state.outbound_transaction_committed(queue)

    def _dispatch_data(
        self, data: list, conn_id: int, queries: list[int]
    ):
        
        batches = defaultdict(list)

        if self._key_to_hash != "conn_id":
            for element in data:
                value_to_hash = element[self._key_to_hash]
                if isinstance(value_to_hash, list):
                    for value in value_to_hash:
                        if not value:
                            continue
                        element_copy = element.copy()
                        element_copy[self._key_to_hash] = [value]
                        hashed = hash(value) % len(self._filter_queues)
                        queue = self._filter_queues[hashed]
                        batches[queue].append(element_copy)
                else:
                    hashed_idx = hash(element[self._key_to_hash]) % len(self._filter_queues)
                    queue = self._filter_queues[hashed_idx]
                    batches[queue].append(element)
        else:
            hashed_idx = hash(conn_id) % len(self._filter_queues)
            queue = self._filter_queues[hashed_idx]
            batches[queue] = data

        for queue, batch in batches.items():
            transaction_id = self._state.next_outbound_transaction_id(queue)
            msg_body = {
                "transaction_id": transaction_id,
                "conn_id": conn_id,
                "queries": queries,
                "data": batch,
            }

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
        "FILTER_TYPE": str,
        "INPUT_QUEUES": str,
        "KEY_TO_HASH": str,
        "IS_PROXY": bool,
    }
    config = Configuration.from_env(required, "config.ini")
    config.validate()

    controller_id = f"{config.get('FILTER_TYPE')}_lb_proxy"

    state = LoadBalancerProxy.default_state(
        controller_id=controller_id,
        file_path="state.json",
        temp_file_path="state_temp.json",
    )

    if os.path.exists(state.file_path):
        logging.info("Loading state from file...")
        state.update_from_file()

    config_logging(config.get("LOGGING_LEVEL"))
    logging.info(config)

    messaging = Goutong(sender_id=controller_id)
    load_balancer = LoadBalancerProxy(config, messaging, state)


    controller_thread = threading.Thread(target=load_balancer.start)
    controller_thread.start()

    # HEALTCHECK HANDLING
    healthcheck_handler = HealthcheckHandler(load_balancer)
    healthcheck_handler.start()


if __name__ == "__main__":
    main()
