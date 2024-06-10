"""
A Barrier controller that distributes data to multiple filters in a round-robin fashion. 
It also works as a threading barrier, forwarding EOF messages to the next filter in the chain once all filters have processed the data.
"""

from collections import defaultdict
import os
from src.utils.config_loader import Configuration
import logging
import signal

from src.messaging.message import Message
from src.messaging.goutong import Goutong
from src.exceptions.shutting_down import ShuttingDown
from src.controller_state.controller_state import ControllerState


class LoadBalancerProxy:

    def __init__(
        self, config: Configuration, messaging: Goutong, state: ControllerState
    ):
        self.barrier_config = config
        self._messaging = messaging
        self._state = state

        # Graceful Shutdown Handling
        self.shutting_down = False

        self._input_queues_lb = config.get("INPUT_QUEUES").split()
        self._input_queue_proxy = config.get("FILTER_TYPE") + "_proxy"
        self.broadcast_group_name = config.get("FILTER_TYPE") + "_broadcast"

        self._filter_queues = []
        for i in range(1, config.get("FILTER_COUNT") + 1):
            queue_name = config.get("FILTER_TYPE") + str(i)
            self._filter_queues.append(queue_name)

        # Add queues and broadcast group
        if not self.shutting_down:
            self._messaging.add_broadcast_group(
                self.broadcast_group_name, self._filter_queues
            )

    def start(self):
        try:
            if not self.shutting_down:
                # Set callbacks
                for queue in self._input_queues_lb:
                    self._messaging.set_callback(
                        queue, self._msg_from_other_cluster, auto_ack=False
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
        for queue in self._filter_queues:
            transaction_id = self._state.next_outbound_transaction_id(queue)
            msg_body = {
                "transaction_id": transaction_id,
                "conn_id": conn_id,
                "queries": queries,
                "EOF": True,
            }
            self._messaging.send_to_queue(queue, Message(msg_body))

        for queue in self._filter_queues:
            self._state.outbound_transaction_committed(queue)

    def _dispatch_data(
        self, data: list, conn_id: int, queries: list[int]
    ):
        
        batches = defaultdict(list)

        for element in data:
            key_to_hash = "title" if "title" in element else element.keys()[0]
            hashed_idx = hash(element[key_to_hash]) % len(self._filter_queues)
            queue = self._filter_queues[hashed_idx]
            batches[queue].append(element)

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
    }
    barrier_config = Configuration.from_env(required, "config.ini")
    barrier_config.validate()

    controller_id = f"{barrier_config.get('FILTER_TYPE')}_lb_proxy"

    state = LoadBalancerProxy.default_state(
        controller_id=controller_id,
        file_path="state.json",
        temp_file_path="state_temp.json",
    )

    if os.path.exists(state.file_path):
        logging.info("Loading state from file...")
        state.update_from_file()

    config_logging(barrier_config.get("LOGGING_LEVEL"))
    logging.info(barrier_config)

    messaging = Goutong(sender_id=controller_id)
    load_balancer = LoadBalancerProxy(barrier_config, messaging, state)
    load_balancer.start()


if __name__ == "__main__":
    main()
