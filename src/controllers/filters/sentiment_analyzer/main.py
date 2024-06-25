import random
import sys
import threading
from src.controllers.common.healthcheck_handler import HealthcheckHandler
from src.messaging.goutong import Goutong
from src.utils.config_loader import Configuration
import logging
import signal
import os
from textblob import TextBlob  # type: ignore

from src.messaging.message import Message
from src.exceptions.shutting_down import ShuttingDown
from src.controller_state.controller_state import ControllerState

OUTPUT_QUEUE = "sentiment_averager_queue"

def crash_maybe():
    if random.random() < 0.00005:
        logging.error("CRASHING..")
        sys.exit(1)

class SentimentAnalyzer: 
    FILTER_TYPE = "sentiment_analyzer"

    def __init__(
        self,
        filter_config: Configuration,
        state: ControllerState,
        output_queue: str,
    ):
        self._shutting_down = False
        self._state = state
        self._config = filter_config
        self.controller_name = self.FILTER_TYPE + str(
            filter_config.get("FILTER_NUMBER")
        )
        self.input_queue_name = self.controller_name
        self._proxy_queue = f"{self.FILTER_TYPE}_proxy"
        self._output_queue = output_queue
        self._messaging = Goutong(sender_id=self.controller_name)

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
    
    def start(self):
        # Main Flow
        try:
            if not self._shutting_down:
                self._messaging.set_callback(
                    self.input_queue_name,
                    self._callback_sentiment_analyzer,
                    auto_ack=False,
                )
                self._messaging.listen()
        except ShuttingDown:
            pass

        finally:
            logging.info("Shutting Down.")
            self._messaging.close()
            self._state.save_to_disk()
    
    def shutdown(self):
        logging.info("SIGTERM received. Initiating Graceful Shutdown.")
        self._shutting_down = True
        raise ShuttingDown
    
    def input_queue(self):
        return self.input_queue_name

    def output_queue(self):
        return self._output_queue

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


    def _callback_sentiment_analyzer(self, _: Goutong, msg: Message):
        sender = msg.get("sender")
        expected_transaction_id = self._state.next_inbound_transaction_id(sender)
        transaction_id = msg.get("transaction_id")
        conn_id = msg.get("conn_id")
        queries = msg.get("queries")

        # Duplicate transaction
        if not self._is_transaction_id_valid(msg):
            self._handle_invalid_transaction_id(msg)
            return

        # Send Analyzed Reviews
        if filtered_books := self._analyze_reviews(msg.get("data")):
            transaction_id = self._state.next_outbound_transaction_id(
                self._proxy_queue
            )

            msg_content = {
                "transaction_id": transaction_id,
                "conn_id": conn_id,
                "queries": queries,
                "data": filtered_books,
                "forward_to": [self.output_queue()],
            }
            crash_maybe()
            self._messaging.send_to_queue(self._proxy_queue, Message(msg_content))
            self._state.outbound_transaction_committed(self._proxy_queue)

        # Send End of File
        if msg.get("EOF"):
            transaction_id = self._state.next_outbound_transaction_id(
                self._proxy_queue
            )

            msg_content = {
                "transaction_id": transaction_id,
                "conn_id": conn_id,
                "EOF": True,
                "forward_to": [self.output_queue()],
                "queries": [5],
            }

            crash_maybe()
            self._messaging.send_to_queue(self._proxy_queue, Message(msg_content))
            self._state.outbound_transaction_committed(self._proxy_queue)

        self._state.inbound_transaction_committed(sender)
        crash_maybe()
        self._messaging.ack_delivery(msg.delivery_id)
        crash_maybe()
        self._state.save_to_disk()

    def _analyze_reviews(self, reviews: list):
        analyzed_reviews = []
        if not reviews:
            return analyzed_reviews

        for review in reviews:
            review_text = review["review/text"]
            sentiment = self._analyze_sentiment(review_text)
            analyzed_reviews.append({"title": review["title"], "sentiment": sentiment})

        logging.debug(f"Analyzed {len(analyzed_reviews)} reviews")
        return analyzed_reviews

    def _analyze_sentiment(self, text: str):
        blob = TextBlob(text)
        sentiment = blob.sentiment.polarity  # type: ignore
        return sentiment
    
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
    controller_id = f"{SentimentAnalyzer.FILTER_TYPE}_{filter_config.get('FILTER_NUMBER')}"

    state = SentimentAnalyzer.default_state(
        controller_id=controller_id,
        file_path=f"state/{controller_id}.json",
        temp_file_path=f"state/{controller_id}.tmp",
    )

    if os.path.exists(state.file_path):
        #logging.info("Loading state from file...")
        state.update_from_file()


    sentiment_analyzer = SentimentAnalyzer(
        filter_config=filter_config,
        state=state,
        output_queue=OUTPUT_QUEUE,
    )

    signal.signal(signal.SIGTERM, lambda sig, frame: sentiment_analyzer.shutdown())
    controller_thread = threading.Thread(target=sentiment_analyzer.start)
    controller_thread.start()

    # HEALTCHECK HANDLING
    healthcheck_handler = HealthcheckHandler(sentiment_analyzer)
    healthcheck_handler.start()

if __name__ == "__main__":
    main()
