"""
A Barrier controller that distributes data to multiple filters in a round-robin fashion. 
It also works as a threading barrier, forwarding EOF messages to the next filter in the chain once all filters have processed the data.
"""

from src.utils.config_loader import Configuration
import logging
import signal

from src.messaging.message import Message
from src.messaging.goutong import Goutong
from src.exceptions.shutting_down import ShuttingDown

CONTROL_GROUP = "CONTROL"


class ProxyBarrier:
    def __init__(self, barrier_config: Configuration, messaging: Goutong):
        self.barrier_config = barrier_config
        self.current_queue = 0
        self.eof_count = 0
        self.messaging = messaging

        # Graceful Shutdown Handling
        self.shutting_down = False
        control_queue_name = barrier_config.get("FILTER_TYPE") + "_barrier_control"
        self.transaction_prefix = f"{barrier_config.get('FILTER_TYPE')}_proxy_barrier"
        self.next_transaction = 1
        messaging.add_queues(control_queue_name)
        messaging.add_broadcast_group(CONTROL_GROUP, [control_queue_name])
        messaging.set_callback(control_queue_name, self.callback_control, auto_ack=True)
        signal.signal(
            signal.SIGTERM, lambda sig, frame: self.sigterm_handler(messaging)
        )

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

            # Set callbacks
            self.messaging.set_callback(self.input_queue_name, self.distribute_data, auto_ack=True)
            self.messaging.set_callback(self.eof_queue_name, self.eof_received, auto_ack=True)

    def start(self):
        if not self.shutting_down:
            try:
                self.messaging.listen()
            except ShuttingDown:
                logging.debug("Shutting Down Message Received Via Broadcast")
        self.messaging.close()
        logging.info("Shutting Down.")

    def eof_received(self, _messaging: Goutong, msg: Message):
        self.eof_count += 1
        logging.debug(f"Received EOF number {self.eof_count}: {msg.marshal()}")
        if self.eof_count == self.barrier_config.get("FILTER_COUNT"):
            # Forward EOF
            logging.debug("Forwarding EOF")
            forward_to = msg.get("forward_to")
            for queue in forward_to:
                transaction_id = f"{self.transaction_prefix}_{self.next_transaction}"
                self.next_transaction += 1
                to_send = {"conn_id": msg.get("conn_id"), "queries": msg.get("queries"), "EOF": True, "transaction_id": transaction_id}
                msg = Message(to_send)
                self.messaging.send_to_queue(queue, msg)
            self.eof_count = 0

    def increase_current_queue_index(self):
        self.current_queue = (self.current_queue + 1) % self.barrier_config.get(
            "FILTER_COUNT"
        )

    def distribute_data(self, _messaging: Goutong, msg: Message):
        # logging.debug(f"Received: {msg.marshal()}")

        if msg.has_key("EOF"):
            self.messaging.broadcast_to_group(self.broadcast_group_name, msg)
            return
        
        # Add transaction id
        data = msg.get("data")
        queries = msg.get("queries")
        transaction_id = f"{self.transaction_prefix}_{self.next_transaction}"
        conn_id = msg.get("conn_id")
        
        self.next_transaction += 1
        msg = Message({"conn_id": conn_id, "data": data, "queries": queries, "transaction_id": transaction_id})
        
        # round-robin data
        self.messaging.send_to_queue(self.filter_queues[self.current_queue], msg)
        # logging.debug(f"Passed to: {self.filter_queues[self.current_queue]}")
        self.increase_current_queue_index()

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

    config_logging(barrier_config.get("LOGGING_LEVEL"))
    logging.info(barrier_config)

    messaging = Goutong()
    proxy_barrier = ProxyBarrier(barrier_config, messaging)
    proxy_barrier.start()


if __name__ == "__main__":
    main()