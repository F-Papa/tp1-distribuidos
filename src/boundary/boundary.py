from collections import defaultdict
import json
import logging
import multiprocessing
import threading
import socket
from threading import Semaphore
import time

from src.controller_state.controller_state import ControllerState
from src.messaging.goutong import Goutong
from src.messaging.goutong import Message
from src.utils.config_loader import Configuration

Q1_3_4_QUEUE = "date_filter_queue"
Q2_QUEUE = "decade_counter_queue"
Q5_QUEUE = "category_filter_queue"

REVIEWS_QUEUE_PREFIX = "review_joiner_reviews"
RESULTS_QUEUE_PREFIX = "results_"
MAX_CLIENTS = 3
BATCH_SIZE_LEN = 8
BEGIN_MSG = "BEGIN"
NUM_OF_QUERIES=5
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
        state: ControllerState
    ):
        self.conn = conn
        self.conn_id = conn_id
        self.__shutting_down = False
        self.results_queue = RESULTS_QUEUE_PREFIX + str(self.conn_id)
        self.items_per_batch = items_per_batch
        controller_id = f"{Boundary.CONTROLLER_TYPE}_{conn_id}"
        self.messaging: Goutong = messaging_module(
            sender_id=Boundary.CONTROLLER_TYPE + str(conn_id),
            host=messaging_host,
            port=messaging_port,
        )

        self.EOFs_received = 0
        self.reviews = b""
        self.books = b""
        self._state = state

    def _is_transaction_id_valid(self, msg: Message):
        transaction_id = msg.get("transaction_id")
        sender = msg.get("sender")
        expected_transaction_id = self._state.next_inbound_transaction_id(sender)

        return transaction_id == expected_transaction_id

    def _handle_invalid_transaction_id(self, messaging: Goutong, msg: Message):
        transaction_id = msg.get("transaction_id")
        sender = msg.get("sender")
        expected_transaction_id = self._state.next_inbound_transaction_id(sender)

        if transaction_id < expected_transaction_id:
            logging.info(
                f"Received Duplicate Transaction {transaction_id} from {sender}: "
                + msg.marshal()[:100]
            )
            # crash_maybe()
            messaging.ack_delivery(msg.delivery_id)

        elif transaction_id > expected_transaction_id:
            messaging.requeue(msg)
            logging.info(
                f"Requeueing out of order {transaction_id}, expected {str(expected_transaction_id)}"
            )
    
    def finish(self):
            try:
                self.conn.shutdown(socket.SHUT_RDWR)
            except:
                pass
            self.conn.close()
            try:
                self.messaging.close()
            except:
                logging.error("Couldn't close messaging")

    def handle_connection(self, semaphore: Semaphore):
        semaphore.acquire()
        self.handle_connection_aux()
        self.messaging.delete_queue(self.results_queue)
        self.finish()
        semaphore.release()

    def handle_connection_aux(self):
        bytes_sent = 0
        to_send = BEGIN_MSG.encode()
        
        try:
            sent = self.conn.send(to_send[bytes_sent:])
            bytes_sent += sent
            if not sent:
                raise Exception
        except:
            logging.error("Connection {self.conn_id} finished prematurely. No data received")
            return
    
        logging.info(f"[Conn: {self.conn_id}] Started Sending Data")
        # Send books to the messaging server
        error_dispatching_books = self.__dispatch_books()
        if error_dispatching_books:
            logging.error(f"Connection {self.conn_id} closed prematurely. Flushing Data from system")
            self._flush_books()
            self._flush_reviews()
            return

        # Send reviews to the messaging server
        error_dispatching_reviews = self.__dispatch_reviews()
        if error_dispatching_reviews:
            logging.error(f"Connection {self.conn_id} closed prematurely. Flushing Data from system")
            self._flush_reviews()
            return  

        logging.info(f"[Conn: {self.conn_id}] Finished sending data")
        # Listen for results
        
        self.messaging.set_callback(
            self.results_queue, self.forward_results, args=(), auto_ack=True
        )
        self.messaging.listen()

        logging.info(f"[Conn: {self.conn_id}] Finished")

    def forward_results(self, messaging: Goutong, msg: Message):
        if not self._is_transaction_id_valid(msg):
            self._handle_invalid_transaction_id(messaging, msg)
            return

        if msg.has_key("EOF"):
            self.EOFs_received +=1

        encoded_msg = msg.marshal().encode("utf-8")
        length = len(encoded_msg).to_bytes(BATCH_SIZE_LEN, byteorder="big")

        bytes_sent = 0
        to_send = length + encoded_msg
        while bytes_sent < len(to_send):
            bytes_sent += self.conn.send(to_send[bytes_sent:])
        self._state.inbound_transaction_committed(msg.get('sender'))

        if self.EOFs_received == NUM_OF_QUERIES:
            self.messaging.stop_consuming(msg.queue_name)
    def __dispatch_books(self):
        """Sends reviews to the system via queue. Returns True if any error or False otherwise"""
        eof_reached = False
        items_received = []

        while not eof_reached:
            buffer = b""

            while len(buffer) < BATCH_SIZE_LEN:
                try:
                    received = self.conn.recv(BATCH_SIZE_LEN - len(buffer))
                    buffer += received
                    if not received:
                        raise Exception
                except:
                    return True
                
            expected_length = int.from_bytes(buffer, byteorder="big")

            buffer = b""
                       
            # Read next batch
            while len(buffer) < expected_length:
                try:
                    to_read = expected_length - len(buffer)
                    received = self.conn.recv(to_read)
                    buffer += received
                    if not received:
                        raise Exception
                except:
                    return True

            decoded = buffer.decode()
            received_data: dict = json.loads(decoded)

            eof_reached = received_data.get("EOF")
            items_received.extend(received_data["data"])

            while len(items_received) >= self.items_per_batch:
                to_send = items_received[: self.items_per_batch]
                items_received = items_received[self.items_per_batch :]
                self.__send_batch_books(to_send, False)

        # Send the remaining books and an EOF message
        self.__send_batch_books(items_received, True)
        return False

    def _flush_books(self):
        msg_body = {
            "transaction_id": self._state.next_outbound_transaction_id(Q1_3_4_QUEUE),
            "conn_id": self.conn_id,
            "data": [],
            "queries": [1, 3, 4],
            "EOF": True,
        }

        self.messaging.send_to_queue(Q1_3_4_QUEUE, Message(msg_body))
        self._state.outbound_transaction_committed(Q1_3_4_QUEUE)

        msg_body = {
            "transaction_id": self._state.next_outbound_transaction_id(Q2_QUEUE),
            "conn_id": self.conn_id,
            "data": [],
            "queries": [2],
            "EOF": True,
        }  
        
        self.messaging.send_to_queue(Q2_QUEUE, Message(msg_body))
        self._state.outbound_transaction_committed(Q2_QUEUE)

        msg_body = {
            "transaction_id": self._state.next_outbound_transaction_id(Q5_QUEUE),
            "conn_id": self.conn_id,
            "data": [],
            "queries": [5],
            "EOF": True,
        }  
        
        self.messaging.send_to_queue(Q5_QUEUE, Message(msg_body))
        self._state.outbound_transaction_committed(Q5_QUEUE)
    
    def _flush_reviews(self):
        msg_body = {
            "transaction_id": self._state.next_outbound_transaction_id(REVIEWS_QUEUE_PREFIX),
            "conn_id": self.conn_id,
            "data": [],
            "queries": [5, 3, 4],
            "EOF": True,
        }

        self.messaging.send_to_queue(REVIEWS_QUEUE_PREFIX, Message(msg_body))
        self._state.outbound_transaction_committed(REVIEWS_QUEUE_PREFIX)
    
    # Encodes a batch of books to the messaging server encoded in the correct format
    def __send_batch_books(self, batch: list, eof_reached: bool):

        data_q1_3_4, data_q2, data_q5 = self.__separate_columns_by_query(batch)
        # Queries 1,3,4
        msg_body = {
            "transaction_id": self._state.next_outbound_transaction_id(Q1_3_4_QUEUE),
            "conn_id": self.conn_id,
            "data": data_q1_3_4,
            "queries": [1, 3, 4],
        }
        if eof_reached:
            msg_body["EOF"] = True
        self.messaging.send_to_queue(Q1_3_4_QUEUE, Message(msg_body))
        self._state.outbound_transaction_committed(Q1_3_4_QUEUE)
        # self.next_transaction_ids[Q1_3_4_QUEUE] += 1

        # Query 2
        msg_body = {
            "transaction_id": self._state.next_outbound_transaction_id(Q2_QUEUE),
            "conn_id": self.conn_id,
            "data": data_q2,
            "queries": [2],
        }
        if eof_reached:
            msg_body["EOF"] = True
        self.messaging.send_to_queue(Q2_QUEUE, Message(msg_body))
        self._state.outbound_transaction_committed(Q2_QUEUE)
        # self.next_transaction_ids[Q2_QUEUE] += 1

        # Query 5
        msg_body = {
            "transaction_id": self._state.next_outbound_transaction_id(Q5_QUEUE),
            "conn_id": self.conn_id,
            "data": data_q5,
            "queries": [5],
        }
        if eof_reached:
            msg_body["EOF"] = True
        self.messaging.send_to_queue(Q5_QUEUE, Message(msg_body))
        self._state.outbound_transaction_committed(Q5_QUEUE)
        # self.next_transaction_ids[Q5_QUEUE] += 1

    def __dispatch_reviews(self):
        """Sends reviews to the system via queue. Returns True if any error or False otherwise"""
        output_queue_name = REVIEWS_QUEUE_PREFIX
        eof_reached = False
        items_received = []

        while not eof_reached:

            buffer = b""
            while len(buffer) < BATCH_SIZE_LEN:
                try:
                    received = self.conn.recv(BATCH_SIZE_LEN - len(buffer))
                    buffer += received
                    if not received:
                        raise Exception
                except:
                    return True

            expected_length = int.from_bytes(buffer, byteorder="big")

            # Read next batch
            buffer = b""
            while len(buffer) < expected_length:
                to_read = expected_length - len(buffer)
                
                try:
                    received = self.conn.recv(to_read)
                    buffer += received
                    if not received:
                        raise Exception
                except:
                    return True

            decoded = buffer.decode()
            received_data: dict = json.loads(decoded)

            for item in received_data["data"]:
                if "review/score" in item:
                    item["review/score"] = float(item["review/score"])
                else:
                    logging.error(f"item: {item}")

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
        return False

    def __send_batch_reviews(self, batch: list, eof_reached: bool):
        queue_name = REVIEWS_QUEUE_PREFIX
        body = {
            "transaction_id": self._state.next_outbound_transaction_id(queue_name),
            "conn_id": self.conn_id,
            "data": batch,
            "queries": [5, 3, 4],
        }

        global counter
        counter += len(batch)
        
        if eof_reached:
            body["EOF"] = True



        message = Message(body)
        # logging.info(f"Sending {len(batch)} reviews to queue")
        self.messaging.send_to_queue(queue_name, message)
        self._state.outbound_transaction_committed(queue_name)
        # self.next_transaction_ids[queue_name] += 1

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
        self.processes: list[threading.Thread] = []
        self.client_connections = {}

    # Listen for incoming connections and spawn a new process to handle each connection
    def listen_for_connections(self):
        semaphore = Semaphore(MAX_CLIENTS)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("", self.server_port))
        sock.listen(self.backlog)
        controller_id = f"boundary{self.__conn_id}"
        

        while not self.__shutting_down:
            state = ControllerState(controller_id, "", "", {})
            new_sock, _ = sock.accept()

            for p in self.processes:
                if not p.is_alive:
                    p.join()

            logging.info(f"[Connection {self.__conn_id}] On hold")
            new_connection = ClientConnection(
                new_sock,
                self.__conn_id,
                self.items_per_batch,
                self.__messaging_module,
                self.__messaging_host,
                self.__messaging_port,
                state
            )

            self.client_connections[self.__conn_id] = new_connection
            p = threading.Thread(
                target=new_connection.handle_connection, args=(semaphore,)
            )
            self.processes.append(p)
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
