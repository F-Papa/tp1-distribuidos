"""
A Barrier controller that distributes data to multiple filters in a round-robin fashion. 
It also works as a threading barrier, forwarding EOF messages to the next filter in the chain once all filters have processed the data.
"""

import os
from src.utils.config_loader import Configuration
import logging
import signal

from src.messaging.message import Message
from src.messaging.goutong import Goutong
from src.exceptions.shutting_down import ShuttingDown
from src.controller_state.controller_state import ControllerState

CONTROL_GROUP = "CONTROL"


class ProxyBarrier:
    DATA = 1
    EOF = 2

    def __init__(
        self, barrier_config: Configuration, messaging: Goutong, state: ControllerState
    ):
        self.barrier_config = barrier_config
        self.messaging = messaging
        self.state = state

        # Graceful Shutdown Handling
        self.shutting_down = False
        control_queue_name = barrier_config.get("FILTER_TYPE") + "_barrier_control"
        messaging.add_queues(control_queue_name)
        messaging.add_broadcast_group(CONTROL_GROUP, [control_queue_name])

        self.eof_queue_name = barrier_config.get("FILTER_TYPE") + "_eof"
        self.input_queue_name = barrier_config.get("FILTER_TYPE") + "_queue"
        self.broadcast_group_name = barrier_config.get("FILTER_TYPE") + "_broadcast"

        self.filter_queues = []
        for i in range(1, barrier_config.get("FILTER_COUNT") + 1):
            queue_name = barrier_config.get("FILTER_TYPE") + str(i)
            self.filter_queues.append(queue_name)

        # Add queues and broadcast group
        if not self.shutting_down:
            self.messaging.add_queues(self.eof_queue_name)
            self.messaging.add_queues(self.input_queue_name)
            self.messaging.add_queues(*self.filter_queues)

            self.messaging.add_broadcast_group(
                self.broadcast_group_name, self.filter_queues
            )

    def start(self):
        try:
            while not self.shutting_down:
                if not self.state.committed:
                    if self.state.get("type_last_message") == self.EOF:
                        self.handle_uncommitted_eof(self.messaging, self.state)
                    else:
                        self.distribute_data(self.messaging, self.state)

                # Set callbacks
                self.messaging.set_callback(
                    self.input_queue_name,
                    self.data_callback,
                    auto_ack=False,
                    args=(self.state,),
                )
                self.messaging.set_callback(
                    self.eof_queue_name,
                    self.eof_callback,
                    auto_ack=False,
                    args=(self.state,),
                )
                self.messaging.listen()

        except ShuttingDown:
            logging.debug("Shutting Down Message Received Via Broadcast")

        self.messaging.close()
        logging.info("Shutting Down.")

    def eof_callback(self, messaging: Goutong, msg: Message, state: ControllerState):
        print("YELLOW")
        eof_count = state.get("barrier.eof_count")
        queries = msg.get("queries")
        conn_id = msg.get("conn_id")
        transaction_id = msg.get("transaction_id")

        if transaction_id in state.transactions_received:
            messaging.ack_delivery(msg.delivery_id)
            logging.info(
                f"Received Duplicate Transaction {msg.get('transaction_id')}: "
                + msg.marshal()[:100]
            )
            return

        if eof_count.get(f"{conn_id}_{queries}"):
            eof_count[f"{conn_id}_{queries}"] += 1
        else:
            eof_count[f"{conn_id}_{queries}"] = 1

        state.set("barrier.eof_count", eof_count)
        state.set("barrier.forward_to", msg.get("forward_to"))
        state.set("committed", False)
        state.set("type_last_message", self.EOF)
        state.set("conn_id", conn_id)
        state.set("queries", queries)
        state.mark_transaction_received(transaction_id)
        state.save_to_disk()

        # Acknowledge message
        messaging.ack_delivery(msg.delivery_id)
        messaging.stop_consuming(self.input_queue_name)
        messaging.stop_consuming(self.eof_queue_name)

    def handle_uncommitted_eof(self, messaging: Goutong, state: ControllerState):
        received = state.get("barrier.eof_count")
        expected = self.barrier_config.get("FILTER_COUNT")

        logging.info(f"Received: {received}, Expected: {expected}")

        conn_id = state.get("conn_id")
        queries = state.get("queries")
        eof_count = state.get("barrier.eof_count")
        eof_count_this_flow = eof_count[f"{conn_id}_{queries}"]

        if eof_count_this_flow == self.barrier_config.get("FILTER_COUNT"):
            # Forward EOF
            for queue in state.get("barrier.forward_to"):
                to_send = {
                    "transaction_id": state.id_for_next_transaction(),
                    "conn_id": conn_id,
                    "queries": queries,
                    "EOF": True,
                }
                msg = Message(to_send)
                logging.info(f"Forwarding EOF | Queue: {queue}, msg: {msg.marshal()}")
                self.messaging.send_to_queue(queue, msg)

            del eof_count[f"{conn_id}_{queries}"]
            state.set("barrier.eof_count", eof_count)
            state.mark_transaction_committed()
        state.save_to_disk()

    def increase_current_queue_index(self):
        old = self.state.get("proxy.current_queue")
        plus_one = old + 1
        overflow = plus_one % self.barrier_config.get("FILTER_COUNT")

        # logging.info(f"Old: {old}, Plus One: {plus_one}, Overflow: {overflow}")
        self.state.set("proxy.current_queue", overflow)

    def data_callback(self, messaging: Goutong, msg: Message, state: ControllerState):
        # logging.info("Received Data")

        transaction_id = msg.get("transaction_id")

        if transaction_id in state.transactions_received:
            messaging.ack_delivery(msg.delivery_id)
            logging.info(
                f"Received Duplicate Transaction {msg.get('transaction_id')}: "
                + msg.marshal()[:100]
            )
            return

        # Retrieve data and queries
        data = msg.get("data") if msg.has_key("data") else []
        queries = msg.get("queries")
        conn_id = msg.get("conn_id")
        contains_eof = msg.get("EOF")

        # Add data and queries to state
        state.set("conn_id", conn_id)
        state.set("queries", queries)
        state.set("proxy.data", data)
        state.set("proxy.EOF", contains_eof)
        state.set("type_last_message", self.DATA)
        state.set("committed", False)

        # Mark transaction as received
        state.mark_transaction_received(transaction_id)

        # Save state to disk
        state.save_to_disk()

        # Acknowledge message
        messaging.ack_delivery(msg.delivery_id)
        messaging.stop_consuming(self.input_queue_name)
        messaging.stop_consuming(self.eof_queue_name)

    def distribute_data(self, messaging: Goutong, state: ControllerState):

        # Add transaction id
        data = state.get("proxy.data")
        queries = state.get("queries")
        conn_id = state.get("conn_id")

        msg_body = {
            "transaction_id": state.id_for_next_transaction(),
            "conn_id": conn_id,
            "data": data,
            "queries": queries,
        }

        msg = Message(msg_body)

        # round-robin data
        queue_idx = state.get("proxy.current_queue")

        if data:
            self.messaging.send_to_queue(self.filter_queues[queue_idx], msg)
            self.increase_current_queue_index()

        if state.get("proxy.EOF"):
            # Forward EOF
            for queue in self.filter_queues:
                msg_body = {
                    "transaction_id": state.id_for_next_transaction() + "_EOF",
                    "conn_id": conn_id,
                    "queries": queries,
                    "EOF": True,
                }
                msg = Message(msg_body)
                self.messaging.send_to_queue(queue, msg)

            state.set("proxy.EOF", False)

        # Mark transaction as committed
        state.mark_transaction_committed()
        state.save_to_disk()

    def sigterm_handler(self, messaging: Goutong):
        logging.info("SIGTERM received. Initiating Graceful Shutdown.")
        self.shutting_down = True
        # msg = Message({"ShutDown": True})
        # messaging.broadcast_to_group(CONTROL_GROUP, msg)

    def callback_control(self, messaging: Goutong, msg: Message):
        if msg.has_key("ShutDown"):
            self.shutting_down = True
            raise ShuttingDown


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
    required = {"FILTER_COUNT": int, "LOGGING_LEVEL": str, "FILTER_TYPE": str}
    barrier_config = Configuration.from_env(required, "config.ini")
    barrier_config.validate()

    controller_id = f"{barrier_config.get('FILTER_TYPE')}_proxy_barrier"

    extra_fields = {
        "type_last_message": ProxyBarrier.DATA,
        "proxy.EOF": False,
        "queries": [],
        "conn_id": 0,
        "proxy.current_queue": 0,
        "proxy.data": [],
        "barrier.eof_count": {},
        "barrier.forward_to": "",
    }

    state = ControllerState(
        controller_id=controller_id,
        file_path=f"state/{controller_id}.json",
        temp_file_path=f"state/{controller_id}.tmp",
        extra_fields=extra_fields,
    )

    if os.path.exists(state.file_path):
        state.update_from_file()

    config_logging(barrier_config.get("LOGGING_LEVEL"))
    logging.info(barrier_config)

    messaging = Goutong()
    proxy_barrier = ProxyBarrier(barrier_config, messaging, state)
    proxy_barrier.start()


if __name__ == "__main__":
    main()
