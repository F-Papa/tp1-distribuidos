import signal
from src.exceptions.shutting_down import ShuttingDown
from src.messaging.goutong import Goutong
from src.messaging.message import Message
from src.utils.config_loader import Configuration
import logging


BOOKS_QUEUE = "books"
REVIEWS_QUEUE = "reviews"

CONTROL_GROUP = "CONTROL"


DATE_FILTER_QUEUE = "date_filter_queue"
DECADES_COUNTER_QUEUE = "decade_counter_queue"
CATEGORY_FILTER_QUEUE = "category_filter_queue"
CONTROL_QUEUE_PREFIX = "input_control_"


class InputController:

    def __init__(self, config: Configuration, messaging: Goutong):
        self._items_per_batch = config.get("ITEMS_PER_BATCH")
        self._messaging = messaging
        self._shutting_down = False
        self._filter_number = config.get("FILTER_NUMBER")
        self._messaging.add_queues(BOOKS_QUEUE, REVIEWS_QUEUE)
        self._messaging.add_queues(
            DATE_FILTER_QUEUE, DECADES_COUNTER_QUEUE, CATEGORY_FILTER_QUEUE
        )
        self._next_batch = dict()  # {conn_id: [books]}
        control_queue_name = CONTROL_QUEUE_PREFIX + str(self._filter_number)
        self._messaging.add_queues(control_queue_name)

        self._messaging.set_callback(BOOKS_QUEUE, self._dispatch_books, args=())
        messaging.add_broadcast_group(CONTROL_GROUP, [control_queue_name])

    def start(self):
        try:
            self._messaging.listen()
        except ShuttingDown:
            self._messaging.close()
            logging.info("Shutdown successfully")

    def receiving_from_connections(self) -> set:
        return set(self._next_batch.keys())

    def _dispatch_books(self, _: Goutong, message: Message):
        conn_id = message.get("conn_id")

        end_of_books = message.get("EOF")

        if conn_id not in self._next_batch:
            self._next_batch.update({conn_id: []})

        self._next_batch[conn_id].extend(message.get("data"))

        while (
            len(self._next_batch[conn_id]) >= self._items_per_batch
            and not self._shutting_down
        ):
            to_send = self._next_batch[conn_id][: self._items_per_batch]
            self._next_batch[conn_id] = self._next_batch[conn_id][
                self._items_per_batch :
            ]
            self.__dispatch_books_aux(conn_id, to_send, False)

        if end_of_books:
            self.__dispatch_books_aux(conn_id, self._next_batch[conn_id], True)
            del self._next_batch[conn_id]
            logging.info(f"[Conn {conn_id}] All books dispatched")

    def __separate_columns_by_query(self, books: list):
        # Query 1, 3, 4
        data_q1_3_4 = []
        data_q2 = []
        data_q5 = []
        for book in books:
            try:
                data_q1_3_4.append(
                    {
                        "title": book["title"],
                        "year": book["year"],
                        "authors": book["authors"],
                        "publisher": book["publisher"],
                        "categories": book["categories"],
                    }
                )
            except Exception as e:
                logging.error(f"Error processing book {book}: {e}")
                raise e
            data_q2.append(
                {
                    "decade": self.__parse_decade(book["year"]),
                    "authors": book["authors"],
                }
            )
            data_q5.append({"title": book["title"], "categories": book["categories"]})

        return data_q1_3_4, data_q2, data_q5

    def __dispatch_books_aux(self, conn_id: int, books: list, end_of_books: bool):
        data_q1_3_4, data_q2, data_q5 = self.__separate_columns_by_query(books)

        # Queries 1,3,4
        msg_body = {"conn_id": conn_id, "data": data_q1_3_4, "queries": [1, 3, 4]}
        if end_of_books:
            msg_body["EOF"] = True
        self._messaging.send_to_queue(DATE_FILTER_QUEUE, Message(msg_body))

        # Query 2
        msg_body = {"conn_id": conn_id, "data": data_q2, "queries": [2]}
        if end_of_books:
            msg_body["EOF"] = True
        self._messaging.send_to_queue(DECADES_COUNTER_QUEUE, Message(msg_body))

        # Query 5
        msg_body = {"conn_id": conn_id, "data": data_q5, "queries": [5]}
        if end_of_books:
            msg_body["EOF"] = True
        self._messaging.send_to_queue(CATEGORY_FILTER_QUEUE, Message(msg_body))

    def __parse_decade(self, year: int) -> int:
        return year - (year % 10)

    # Graceful Shutdown
    def handle_sigterm(self):
        logging.info("SIGTERM received. Initiating Graceful Shutdown.")
        self._shutting_down = True
        raise ShuttingDown
        #msg = Message({"ShutDown": True})
        # self._messaging.broadcast_to_group(CONTROL_GROUP, msg)


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


if __name__ == "__main__":
    required = {
        "FILTER_NUMBER": int,
        "LOGGING_LEVEL": str,
        "ITEMS_PER_BATCH": int,
        "MESSAGING_HOST": str,
        "MESSAGING_PORT": int,
    }

    config = Configuration.from_file(required, "config.ini")
    config.update_from_env()
    config.validate()

    config_logging(config.get("LOGGING_LEVEL"))

    logging.info(config)
    msg_host = config.get("MESSAGING_HOST")
    msg_port = config.get("MESSAGING_PORT")
    messaging = Goutong(msg_host, msg_port)
    in_controller = InputController(config, messaging)
    signal.signal(signal.SIGTERM, lambda sig, frame: in_controller.handle_sigterm())
    in_controller.start()
