import json
import logging
import multiprocessing
import socket
import time

from src.messaging.goutong import Goutong
from src.messaging.goutong import Message
from src.utils.config_loader import Configuration

BOOKS_QUEUE = "books"
REVIEWS_QUEUE = "joiner_reviews_queue"
RESULTS_QUEUE_PREFIX = "results_"

BOOK_SIZE_LENGTH = 4

BATCH_SIZE_LEN = 4

COLUMN_SEPARATOR = "#@3*"  # Arbitrary separator
BOOKS_AND_REVIEWS_SEPARATOR = "M!j@"  # Arbitrary separator
EOF_SEPARATOR = "$!#a"  # Arbitrary code for the end of the file
NUMBER_OF_QUERIES = 5

# Indexes for the columns in the csv book file
BOOK_TITLE = 0
BOOK_AUTHORS = 2
BOOK_PUBLISHER = 5
BOOK_PUBLISHED_DATE = 6
BOOK_CATEGORIES = 8

# Indexed for the columns in the csv reviews file
REVIEW_BOOK_TITLE = 1
REVIEW_SCORE = 6
REVIEW_TEXT = 9


class ClientConnection:
    def __init__(
        self,
        conn: socket.socket,
        conn_id: int,
        items_per_batch: int,
        messaging_module: type,
        messaging_host: str,
        messaging_port: int,
    ):
        self.conn = conn
        self.conn_id = conn_id
        self.__shutting_down = False
        self.items_per_batch = items_per_batch
        self.messaging: Goutong = messaging_module(
            host=messaging_host, port=messaging_port
        )
        self.messaging.add_queues(BOOKS_QUEUE, REVIEWS_QUEUE)
        self.EOFs_received = 0
        self.reviews = b""
        self.books = b""

    def handle_connection(self):
        results_queue = RESULTS_QUEUE_PREFIX + str(self.conn_id)
        self.messaging.add_queues(results_queue)
        self.messaging.set_callback(
            results_queue, self.forward_results, args=()
        )

        self.__dispatch_data(BOOKS_QUEUE)
        self.__dispatch_data(REVIEWS_QUEUE)
        self.messaging.listen()
        self.conn.close()

    def forward_results(self, messaging: Goutong, msg: Message):
        encoded_msg = msg.marshal().encode("utf-8")
        length = len(encoded_msg).to_bytes(BATCH_SIZE_LEN, byteorder="big")

        bytes_sent = 0
        to_send = length + encoded_msg
        while bytes_sent < len(to_send):
            bytes_sent += self.conn.send(to_send[bytes_sent:])
        
    def __dispatch_data(self, output_queue_name: str):
        eof_reached = False
        items_received = []
        while not eof_reached:
            received = b""

            while len(received) < BATCH_SIZE_LEN:
                received += self.conn.recv(BATCH_SIZE_LEN - len(received))

            expected_length = int.from_bytes(received, byteorder="big")

            received = b""
            # Read next batch
            while len(received) < expected_length:
                to_read = expected_length - len(received)
                received += self.conn.recv(to_read)
            
            decoded = received.decode()
            received_data: dict = json.loads(decoded)

            items_received.extend(received_data["data"])

            eof_reached = received_data.get("EOF")

            while len(items_received) >= self.items_per_batch:
                to_send = items_received[: self.items_per_batch]
                items_received = items_received[self.items_per_batch :]
                self.__send_batch(self.conn_id, to_send, output_queue_name, False)
                logging.debug(f"Succesfully sent {self.items_per_batch} items to queue {output_queue_name}")
        
        logging.debug("EOF reached")
        # Send the remaining books and an EOF message
        logging.debug(f"Sending remaining {len(items_received)} items to queue {output_queue_name}")
        self.__send_batch(self.conn_id, items_received, output_queue_name, True)

    # Encodes a batch of books to the messaging server encoded in the correct format
    def __send_batch(
        self, conn_id: int, batch: list, queue_name: str, eof_reached: bool
    ):
        body = {
            "conn_id": conn_id,
            "data": batch,
        }

        if eof_reached:
            body["EOF"] = True

        message = Message(body)
        self.messaging.send_to_queue(queue_name, message)

class Boundary:
    def __init__(self, config: Configuration, messaging_module: type):
        self.__shutting_down = False
        self.__conn_id = 1
        self.backlog = config.get("BACKLOG")
        self.server_port = config.get("SERVER_PORT")
        self.__messaging_port = config.get("MESSAGING_PORT")
        self.__messaging_host = config.get("MESSAGING_HOST")
        self.items_per_batch = config.get("ITEMS_PER_BATCH")
        self.__messaging_module = messaging_module
        self.client_connections = {}

    # Listen for incoming connections and spawn a new process to handle each connection
    def listen_for_connections(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("", self.server_port))
        sock.listen(self.backlog)
        while not self.__shutting_down:
            new_sock, _ = sock.accept()
            logging.info(f"[Conn {self.__conn_id}] Connection accepted")
            new_connection = ClientConnection(
                new_sock,
                self.__conn_id,
                self.items_per_batch,
                self.__messaging_module,
                self.__messaging_host,
                self.__messaging_port,
            )
            self.client_connections[self.__conn_id] = new_connection
            p = multiprocessing.Process(
                target=new_connection.handle_connection, args=()
            )
            p.start()
            self.__conn_id += 1

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
    # Load configuration
    required = {
        "LOGGING_LEVEL": str,
        "ITEMS_PER_BATCH": int,
        "BACKLOG": int,
        "SERVER_PORT": int,
        "MESSAGING_HOST": str,
        "MESSAGING_PORT": int,
    }

    config = Configuration.from_file(required, "config.ini")
    config.update_from_env()
    config.validate()

    config_logging(config.get("LOGGING_LEVEL"))
    logging.info(config)

    # Initialize Boundary
    boundary = Boundary(config, Goutong)
    boundary.listen_for_connections()


if __name__ == "__main__":
    main()
