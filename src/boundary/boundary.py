from collections import defaultdict
import json
import logging
import multiprocessing
import socket
import time

from src.messaging.goutong import Goutong
from src.messaging.goutong import Message
from src.utils.config_loader import Configuration

Q1_3_4_QUEUE = "date_filter_queue"
Q2_QUEUE = "decade_counter_queue"
Q5_QUEUE = "category_filter_queue"

REVIEWS_QUEUE_PREFIX = "reviews_joiner_1_reviews_"
RESULTS_QUEUE_PREFIX = "results_"

BATCH_SIZE_LEN = 8

counter = 0

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
        controller_id = f"{Boundary.CONTROLLER_TYPE}_{conn_id}"
        self.messaging: Goutong = messaging_module(
            sender_id=Boundary.CONTROLLER_TYPE + str(conn_id),
            host=messaging_host,
            port=messaging_port,
        )
        self.next_transaction_ids = defaultdict(lambda: 1)

        self.EOFs_received = 0
        self.reviews = b""
        self.books = b""

    def handle_connection(self):
        # Send books to the messaging server
        self.__dispatch_books()

        # Send reviews to the messaging server
        self.__dispatch_reviews()

        logging.info(f"[Conn: {self.conn_id}] Finished sending data")
        # Listen for results
        results_queue = RESULTS_QUEUE_PREFIX + str(self.conn_id)
        self.messaging.set_callback(
            results_queue, self.forward_results, args=(), auto_ack=True
        )
        self.messaging.listen()
        self.conn.close()

    def forward_results(self, messaging: Goutong, msg: Message):
        if msg.has_key("EOF"):
            to_show = {
                "transaction_id": msg.get("transaction_id"),
                "conn_id": self.conn_id,
                "EOF": msg.get("EOF"),
                "data": str(msg.get("data"))[:50],
                "queries": msg.get("queries"),
                "sender": msg.get("sender"),
            }
            logging.info(f"EOF Received: {to_show}")
            # queries = msg.get("queries")
            # for q in queries:
            #     logging.info(f"[Conn: {self.conn_id}] Received EOF for query {q}")

        encoded_msg = msg.marshal().encode("utf-8")
        length = len(encoded_msg).to_bytes(BATCH_SIZE_LEN, byteorder="big")

        bytes_sent = 0
        to_send = length + encoded_msg
        while bytes_sent < len(to_send):
            bytes_sent += self.conn.send(to_send[bytes_sent:])

    def __dispatch_books(self):
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

            eof_reached = received_data.get("EOF")
            items_received.extend(received_data["data"])

            while len(items_received) >= self.items_per_batch:
                to_send = items_received[: self.items_per_batch]
                items_received = items_received[self.items_per_batch :]
                self.__send_batch_books(to_send, False)

        # Send the remaining books and an EOF message
        self.__send_batch_books(items_received, True)

    # Encodes a batch of books to the messaging server encoded in the correct format
    def __send_batch_books(self, batch: list, eof_reached: bool):

        data_q1_3_4, data_q2, data_q5 = self.__separate_columns_by_query(batch)
        # Queries 1,3,4
        msg_body = {
            "transaction_id": self.next_transaction_ids[Q1_3_4_QUEUE],
            "conn_id": self.conn_id,
            "data": data_q1_3_4,
            "queries": [1, 3, 4],
        }
        if eof_reached:
            msg_body["EOF"] = True
        self.messaging.send_to_queue(Q1_3_4_QUEUE, Message(msg_body))
        self.next_transaction_ids[Q1_3_4_QUEUE] += 1

        # Query 2
        msg_body = {
            "transaction_id": self.next_transaction_ids[Q2_QUEUE],
            "conn_id": self.conn_id,
            "data": data_q2,
            "queries": [2],
        }
        if eof_reached:
            msg_body["EOF"] = True
        self.messaging.send_to_queue(Q2_QUEUE, Message(msg_body))
        self.next_transaction_ids[Q2_QUEUE] += 1

        # Query 5
        msg_body = {
            "transaction_id": self.next_transaction_ids[Q5_QUEUE],
            "conn_id": self.conn_id,
            "data": data_q5,
            "queries": [5],
        }
        if eof_reached:
            msg_body["EOF"] = True
        self.messaging.send_to_queue(Q5_QUEUE, Message(msg_body))
        self.next_transaction_ids[Q5_QUEUE] += 1

    def __dispatch_reviews(self):
        output_queue_name = REVIEWS_QUEUE_PREFIX + str(self.conn_id)
        eof_reached = False
        items_received = []

        while not eof_reached:

            received = b""
            while len(received) < BATCH_SIZE_LEN:
                received += self.conn.recv(BATCH_SIZE_LEN - len(received))

            expected_length = int.from_bytes(received, byteorder="big")

            # Read next batch
            received = b""
            while len(received) < expected_length:
                to_read = expected_length - len(received)
                received += self.conn.recv(to_read)

            decoded = received.decode()
            received_data: dict = json.loads(decoded)

            items_received.extend(received_data["data"])


            while len(items_received) >= self.items_per_batch:
                to_send = items_received[: self.items_per_batch]
                items_received = items_received[self.items_per_batch :]
                self.__send_batch_reviews(to_send, False)
                logging.debug(
                    f"Succesfully sent {self.items_per_batch} items to queue {output_queue_name}"
                )

            eof_reached = received_data.get("EOF")
        # Send the remaining books and an EOF message
        self.__send_batch_reviews(items_received, True)

    def __send_batch_reviews(self, batch: list, eof_reached: bool):
        queue_name = REVIEWS_QUEUE_PREFIX + str(self.conn_id)
        body = {
            "transaction_id": self.next_transaction_ids[queue_name],
            "conn_id": self.conn_id,
            "data": batch,
            "queries": [5, 3, 4],
        }

        global counter
        counter += len(batch)
        
        if eof_reached:
            logging.info(f"Sending EOF to {queue_name} | Total reviews: {counter}")
            body["EOF"] = True



        message = Message(body)
        # logging.info(f"Sending {len(batch)} reviews to queue")
        self.messaging.send_to_queue(queue_name, message)
        self.next_transaction_ids[queue_name] += 1

    def __parse_decade(self, year: int) -> int:
        return year - (year % 10)

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


class Boundary:
    CONTROLLER_TYPE = "boundary"

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
