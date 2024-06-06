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


class Proxy:

    def __init__(
        self, barrier_config: Configuration, messaging: Goutong, state: ControllerState
    ):
        self.barrier_config = barrier_config
        self.messaging = messaging
        self.state = state

        # Graceful Shutdown Handling
        self.shutting_down = False

        self._input_queue = barrier_config.get("FILTER_TYPE") + "_queue"
        self.broadcast_group_name = barrier_config.get("FILTER_TYPE") + "_broadcast"

        self._filter_queues = []
        for i in range(1, barrier_config.get("FILTER_COUNT") + 1):
            queue_name = barrier_config.get("FILTER_TYPE") + str(i)
            self._filter_queues.append(queue_name)

        # Add queues and broadcast group
        if not self.shutting_down:
            self.messaging.add_broadcast_group(
                self.broadcast_group_name, self._filter_queues
            )

    def start(self):
        try:
            if not self.shutting_down:

                # Set callbacks
                self.messaging.set_callback(
                    self._input_queue,
                    self._msg_from_other_cluster,
                    auto_ack=False,
                )

                self.messaging.listen()

        except ShuttingDown:
            logging.debug("Shutting Down Message Received Via Broadcast")

        self.messaging.close()
        logging.info("Shutting Down.")

    def input_queue(self) -> str:
        return self._input_queue

    def filter_queues(self) -> list[str]:
        return self._filter_queues

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


    def _msg_from_other_cluster(self, _: Goutong, msg: Message):
        transaction_id = msg.get("transaction_id")
        queries = msg.get("queries")
        conn_id = msg.get("conn_id")
        sender = msg.get("sender")

        expected_transaction_id = self.state.next_inbound_transaction_id(sender)

        # Duplicate transaction
        if transaction_id < expected_transaction_id:
            self.messaging.ack_delivery(msg.delivery_id)
            logging.info(
                f"Received Duplicate Transaction {transaction_id} from {sender}: "
                + msg.marshal()[:100]
            )
            return

        # Some transactions were lost
        if transaction_id > expected_transaction_id:
            # Todo!
            logging.info(
                f"Received Out of Order Transaction {transaction_id} from {sender}. Expected: {expected_transaction_id}"
            )
            return

        # Forward data to one of the filters
        data = msg.get("data")
        has_eof = msg.get("EOF")
        if data or has_eof:
            destination_queue = self._calculate_destination_queue(conn_id=conn_id)
            self._distribute_data(
                data=data, conn_id=conn_id, queries=queries, queue=destination_queue, eof=has_eof
            )

        self.state.inbound_transaction_committed(sender)
        self.state.save_to_disk()
        self.messaging.ack_delivery(msg.delivery_id)

    def _calculate_destination_queue(self, conn_id: int) -> str:
        return self._filter_queues[(conn_id - 1) % len(self._filter_queues)]

    def _forward_end_of_file(self, conn_id: int, queries: list[int]):
        for queue in self._filter_queues:
            transaction_id = self.state.next_outbound_transaction_id(queue)
            msg_body = {
                "transaction_id": transaction_id,
                "conn_id": conn_id,
                "queries": queries,
                "EOF": True,
            }
            msg = Message(msg_body)
            self.messaging.send_to_queue(queue, msg)

        for queue in self._filter_queues:
            self.state.outbound_transaction_committed(queue)

    def _distribute_data(
        self, data: list, conn_id: int, queries: list[int], queue: str, eof: bool
    ):
        transaction_id = self.state.next_outbound_transaction_id(queue)
        msg_body = {
            "transaction_id": transaction_id,
            "conn_id": conn_id,
            "queries": queries,
        }
        if data:
            msg_body["data"] = data

        if eof:
            msg_body["EOF"] = True

        msg = Message(msg_body)
        self.messaging.send_to_queue(queue, msg)
        self.state.outbound_transaction_committed(queue)

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
    required = {"FILTER_COUNT": int, "LOGGING_LEVEL": str, "FILTER_TYPE": str}
    barrier_config = Configuration.from_env(required, "config.ini")
    barrier_config.validate()

    controller_id = f"{barrier_config.get('FILTER_TYPE')}_proxy"

    state = Proxy.default_state(
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
    proxy = Proxy(barrier_config, messaging, state)
    proxy.start()


if __name__ == "__main__":
    main()
