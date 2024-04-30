from messaging.goutong import Goutong
from messaging.message import Message
import logging
import signal

from utils.config_loader import Configuration
from exceptions.shutting_down import ShuttingDown


class SwitchingState(Exception):
    pass


class Joiner:
    RECEIVING_BOOKS = 1
    RECEIVING_REVIEWS = 2
    REVIEWS_INPUT_QUEUE = "joiner_reviews_queue"
    BOOKS_INPUT_QUEUE = "joiner_books_queue"
    CONTROL_QUEUE = "joiner_control"
    CONTROL_GROUP = "CONTROL"
    OUTPUT_Q3_4 = "review_counter_queue"
    OUTPUT_Q5 = "sentiment_analyzer_queue"

    def __init__(self, items_per_batch: int):
        self.shutting_down = False
        self.eof_received = 0
        self.items_per_batch = items_per_batch
        self.state = self.RECEIVING_BOOKS
        self.book_authors_q3_4 = {}
        self.books_q5 = {}
        self.batch_q3_4 = []
        self.batch_q5 = []
        self._init_messaging()
        self._set_receive_books()

    def listen(self):
        if self.state == self.RECEIVING_BOOKS:
            logging.debug("Listening for Books")
        else:
            logging.debug("Listening for Reviews")
        try:
            self.messaging.listen()
        except SwitchingState:
            logging.debug("Switching State")
            if self.state == self.RECEIVING_BOOKS:
                self._set_receive_reviews()
            else:
                self._set_receive_books()
            self.listen()
        except ShuttingDown:
            logging.debug("Stopping Listening")

    def shutdown(self):
        logging.info("Initiating Graceful Shutdown")
        self.shutting_down = True
        self.messaging.close()
        raise ShuttingDown

    def _init_messaging(self):
        self.messaging = Goutong()
        self.messaging.add_queues(
            self.REVIEWS_INPUT_QUEUE,
            self.BOOKS_INPUT_QUEUE,
            self.CONTROL_QUEUE,
            self.OUTPUT_Q3_4,
            self.OUTPUT_Q5,
        )
        self.messaging.add_broadcast_group(self.CONTROL_GROUP, [self.CONTROL_QUEUE])
        self.messaging.set_callback(self.CONTROL_QUEUE, self.callback_control)

    def _set_receive_books(self):
        self.state = self.RECEIVING_BOOKS
        self.eof_received = 0

        self.messaging.set_callback(
            self.BOOKS_INPUT_QUEUE, self._handle_receiving_books
        )

    def _set_receive_reviews(self):
        self.state = self.RECEIVING_REVIEWS
        self.eof_received = 0

        self.messaging.set_callback(
            self.REVIEWS_INPUT_QUEUE, self._handle_receiving_reviews
        )

    def _send_EOF(self):
        msg = Message({"query": [3, 4], "EOF": True})
        self.messaging.send_to_queue(self.OUTPUT_Q3_4, msg)
        logging.debug(f"Sent EOF to: {self.OUTPUT_Q3_4}")

        msg = Message({"query": 5, "EOF": True})
        self.messaging.send_to_queue(self.OUTPUT_Q5, msg)
        logging.debug(f"Sent EOF to: {self.OUTPUT_Q5}")

    def _handle_receiving_books(self, messaging: Goutong, msg: Message):
        # logging.debug(f"Received: {msg.marshal()}")
        if msg.has_key("EOF"):
            if self.state == self.RECEIVING_BOOKS:
                self.eof_received += 1
                if self.eof_received == 2:
                    raise SwitchingState
            return

        query = msg.get("query")
        books = msg.get("data")

        if 3 in query or 4 in query:
            for book in books:
                self.book_authors_q3_4[book["title"]] = book["authors"]
        elif query == 5:
            for book in books:
                self.books_q5[book["title"]] = True

    def _handle_receiving_reviews(self, messaging: Goutong, msg: Message):
        if msg.has_key("EOF"):
            self._send_EOF()
            raise SwitchingState

        query = msg.get("query")
        reviews = msg.get("data")

        # Queries 3,4 Flow
        if 3 in query or 4 in query:
            for review in reviews:
                title = review["title"]
                review = review["review/score"]
                authors = self.book_authors_q3_4.get(title)
                self.batch_q3_4.append((title, authors, review))
                if len(self.batch_q3_4) >= self.items_per_batch:
                    self._send_batch_q3_4()
                    self.batch_q3_4.clear()

            if len(self.batch_q3_4) > 0:
                self._send_batch_q3_4()
                self.batch_q3_4.clear()

        # Query 5 Flow
        elif query == 5:
            for review in reviews:
                if not self.books_q5.get(review["title"]):
                    continue
                title = review["title"]
                review_text = review["review/text"]
                self.batch_q5.append((title, review_text))
                if len(self.batch_q5) >= self.items_per_batch:
                    self._send_batch_q5()
                    self.batch_q5.clear()

    def _send_batch_q3_4(self):
        data = list(
            map(
                lambda item: {
                    "title": item[0],
                    "authors": item[1],
                    "review/score": item[2],
                },
                self.batch_q3_4,
            )
        )
        msg = Message({"query": [3, 4], "data": data})
        self.messaging.send_to_queue(self.OUTPUT_Q3_4, msg)
        logging.debug(
            f"Sent message with {len(self.batch_q3_4)} items to {self.OUTPUT_Q3_4}"
        )

    def _send_batch_q5(self):
        data = list(
            map(lambda item: {"title": item[0], "review/text": item[1]}, self.batch_q5)
        )
        msg = Message({"query": 5, "data": data})
        self.messaging.send_to_queue(self.OUTPUT_Q5, msg)

    def callback_control(self, messaging: Goutong, msg: Message):
        global shutting_down
        if msg.has_key("ShutDown"):
            shutting_down = True
            raise ShuttingDown


# Graceful Shutdown
def sigterm_handler(joiner: Joiner):
    logging.info("SIGTERM received.")
    joiner.shutdown()


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
        "ITEMS_PER_BATCH": int,
        "LOGGING_LEVEL": str,
    }

    filter_config = Configuration.from_file(required, "config.ini")
    filter_config.update_from_env()
    filter_config.validate()

    config_logging(filter_config.get("LOGGING_LEVEL"))
    logging.info(filter_config)

    joiner = Joiner(items_per_batch=filter_config.get("ITEMS_PER_BATCH"))
    signal.signal(signal.SIGTERM, lambda sig, frame: sigterm_handler(joiner))
    joiner.listen()


def callback_filter(
    messaging: Goutong, msg: Message, config: Configuration, decades_per_author: dict
):
    # logging.debug(f"Received: {msg.marshal()}")
    pass


# Query 3,4
# 1. Escuchar todos los books y almacenarlos en books_q3_4.
# 2. Recibir batches de reviews
#   - Concatenar el autor buscando en books_q3_4
# 3. Enviar a output_q3_4

# Query 5
# 1.    Escuchar todos los books y almacenarlos en books_q5.
# 2. Recibir batches de reviews
#   - Descartar los reviews que no tengan su titulo en books_q5.
# 3. Enviar a output_q5


if __name__ == "__main__":
    main()
