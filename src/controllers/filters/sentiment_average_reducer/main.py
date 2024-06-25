import random
import sys
import threading
import time
from typing import Any, Union
from src.controller_state.controller_state import ControllerState
from src.controllers.common.healthcheck_handler import HealthcheckHandler
from src.messaging.goutong import Goutong
from src.messaging.message import Message
import logging
import signal
import json
import os

from src.utils.config_loader import Configuration
from src.exceptions.shutting_down import ShuttingDown

def crash_maybe():
    pass
    # if random.random() < 0.001:
    #     logging.error("CRASHING..")
    #     sys.exit(1)

class SentimentAverager:

    CONTROLLER_NAME = "sentiment_averager"

    def __init__(
        self,
        config: Configuration,
        state: ControllerState,
        output_queues: dict,
    ):
        self._filter_number = config.get("FILTER_NUMBER")
        self.quantile = config.get("QUANTILE")
        self._state = state
        self._output_queues = output_queues
        self._shutting_down = False
        self.controller_name = f"{self.CONTROLLER_NAME[:-1]}_reducer{self._filter_number}"
        self._messaging = Goutong(sender_id=f"{SentimentAverager.CONTROLLER_NAME}_{config.get('FILTER_NUMBER')}")
        self._input_queue = f"{self.CONTROLLER_NAME}{self._filter_number}"

        self.unacked_msg_limit = config.get("UNACKED_MSG_LIMIT")
        self.unacked_time_limit_in_seconds = config.get("UNACKED_TIME_LIMIT_IN_SECONDS")
        self.unacked_msgs = []
        self.unacked_msg_count = 0
        self.time_of_last_commit = time.time()

    @classmethod
    def default_state(
        cls, controller_id: str, file_path: str, temp_file_path: str
    ) -> ControllerState:
        extra_fields = {
            "saved_reviews": {},
        }

        return ControllerState(
            controller_id=controller_id,
            file_path=file_path,
            temp_file_path=temp_file_path,
            extra_fields=extra_fields,
        )

    def controller_id(self):
        return self.controller_name    

    # region: callback methods
    def reviews_callback(self, _: Goutong, msg: Message):
        if not self._is_transaction_id_valid(msg):
            self._handle_invalid_transaction_id(msg)
            return

        conn_id = msg.get("conn_id")
        conn_id_str = str(conn_id)

        saved_reviews = self._state.get("saved_reviews")
        msg_reviews = msg.get("data")

        # Initialize the review counts and saved reviews for this connection
        if conn_id_str not in saved_reviews:
            saved_reviews[conn_id_str] = {}

        # Initialize the review counts for each title
        if msg_reviews:
            for review in msg_reviews:
                if review["title"] not in saved_reviews[conn_id_str]:
                    saved_reviews[conn_id_str][review["title"]] = {
                        "sum": 0,
                        "count": 0,
                    }

                saved_reviews[conn_id_str][review["title"]]["sum"] += review["sentiment"]
                saved_reviews[conn_id_str][review["title"]]["count"] += 1
        self._state.set("saved_reviews", saved_reviews)

        if msg.get("EOF"):
            self._send_results(conn_id)
            del saved_reviews[conn_id_str]

        self._state.set("saved_reviews", saved_reviews)
        self._state.inbound_transaction_committed(msg.get("sender"))
        
        self.unacked_msg_count += 1
        self.unacked_msgs.append(msg.delivery_id)

        if (
            self.unacked_msg_count > self.unacked_msg_limit
            or msg.get("EOF")
        ):
            logging.info(f"Committing to disk | Unacked Msgs.: {self.unacked_msg_count}")
            
            crash_maybe()
            self._state.save_to_disk()
            self.time_of_last_commit = time.time()
            for delivery_id in self.unacked_msgs:
                crash_maybe()
                self._messaging.ack_delivery(delivery_id)

            self.unacked_msg_count = 0
            self.unacked_msgs.clear()
        
    def time_window_passed(self):
        now = time.time()
        time_since_last_commit = now - self.time_of_last_commit
        
        #logging.info(f"TIME SINCe {time_since_last_commit} | LIMIT {self.unacked_time_limit_in_seconds} | UNACKED COUNT: {self.unacked_msg_count}")
        if (time_since_last_commit > self.unacked_time_limit_in_seconds) and self.unacked_msg_count:
            logging.info(f"Committing to disk | Unacked Msgs.: {self.unacked_msg_count} | Secs. since last commit: {time_since_last_commit}")
            
            crash_maybe()
            self._state.save_to_disk()
            self.time_of_last_commit = now
            for delivery_id in self.unacked_msgs:
                crash_maybe()
                self._messaging.ack_delivery(delivery_id)

            self.unacked_msg_count = 0
            self.unacked_msgs.clear()
    # endregion

    # region: Query methods
    def input_queue(self) -> str:
        return self._input_queue

    def output_queue_name(self, queries: tuple, conn_id: int) -> str:
        entry = self._output_queues.get(queries)
        if entry is None:
            raise ValueError(f"Output queue not found for queries {queries}")

        if entry["is_prefix"]:
            return entry["name"] + str(conn_id)
        return entry["name"]

    def _is_transaction_id_valid(self, msg: Message):
        transaction_id = msg.get("transaction_id")
        sender = msg.get("sender")
        expected_transaction_id = self._state.next_inbound_transaction_id(sender)

        return transaction_id == expected_transaction_id

    # endregion

    # region: Command methods
    def start(self):
        logging.info("Starting Review Counter")
        try:
            if not self._shutting_down:
                self._messaging.set_callback(
                    self.input_queue(), self.reviews_callback, auto_ack=False
                )

                self._messaging.listen()

        except ShuttingDown:
            pass
        finally:
            logging.info("Shutting Down.")
            self._messaging.close()


    def _send_results(self, conn_id: int):
        queries = (5,)
        saved_reviews = self._state.get("saved_reviews")
        conn_id_str = str(conn_id)

        q5_candidates = []

        for title, review in saved_reviews[conn_id_str].items():
            sum = review["sum"]
            count = review["count"]
            q5_candidates.append({"title": title, "average": sum / count})

        q5_candidates.sort(key=lambda x: x["average"], reverse=True)
        top_quantile_count = int(len(q5_candidates) / (100 - self.quantile))
        q5_results = list(map(lambda x: {x["title"]: x["average"]}, q5_candidates[:top_quantile_count]))

        output_queue = self.output_queue_name(queries, conn_id)

        q5_transaction_id = self._state.next_outbound_transaction_id(output_queue)
        msg_q5_body = {
            "queries": [5],
            "conn_id": conn_id,
            "transaction_id": q5_transaction_id,
            "data": q5_results,
            "EOF": True,
        }
        crash_maybe()
        self._messaging.send_to_queue(output_queue, Message(msg_q5_body))
        self._state.outbound_transaction_committed(output_queue)

    def shutdown(self):
        logging.info("SIGTERM received. Initiating Graceful Shutdown.")
        self._shutting_down = True
        raise ShuttingDown

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

    # endregion


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
        "MESSAGING_HOST": str,
        "MESSAGING_PORT": int,
        "QUANTILE": float,
        "FILTER_NUMBER": int,
        "UNACKED_MSG_LIMIT": int,
        "UNACKED_TIME_LIMIT_IN_SECONDS": int,
    }


    config = Configuration.from_file(required, "config.ini")
    config.update_from_env()
    config.validate()

    config_logging(config.get("LOGGING_LEVEL"))
    logging.info(config)

    controller_id = f"{SentimentAverager.CONTROLLER_NAME}_{config.get('FILTER_NUMBER')}"
    state_file_path = f"state/{controller_id}.json"
    temp_file_path = f"state/{controller_id}.tmp"

    state = SentimentAverager.default_state(
        controller_id=SentimentAverager.CONTROLLER_NAME,
        file_path=state_file_path,
        temp_file_path=temp_file_path,
    )

    output_queues = {
        (5,): {"name": "results_", "is_prefix": True},
    }

    if os.path.exists(state.file_path):
        logging.info("Loading state from file...")
        state.update_from_file()
    
    counter = SentimentAverager(config, state, output_queues)

    signal.signal(signal.SIGTERM, lambda sig, frame: counter.shutdown())
    
    controller_thread = threading.Thread(target=counter.start)
    controller_thread.start()

    # HEALTCHECK HANDLING
    healthcheck_handler = HealthcheckHandler(counter)
    healthcheck_handler.start()

if __name__ == "__main__":
    main()
