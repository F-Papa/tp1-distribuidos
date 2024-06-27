import csv
import json
import logging
import os
import socket
import signal
import common.parsing as parsing

# BOOKS_FILE = "../../data/test/books_data11.csv"
BOOKS_FILE = "../../data/books_data.csv"
# REVIEWS_FILE = "../../data/test/ratings_1K.csv"
REVIEWS_FILE = "../../data/test/Books_rating_reduced.csv"
REVIEWS_FILE = "../../data/Books_rating.csv"

BATCH_SIZE_LEN = 8
NUM_OF_QUERIES = 5
BEGIN_MSG = "BEGIN"

class ShuttingDown(Exception):
    def __init__(self):
        pass

class Client:
    def __init__(
        self, items_per_batch: int, server_host: str, server_port: int
    ) -> None:
        self.__items_per_batch = items_per_batch
        self.__server_host = server_host
        self.__server_port = server_port
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.__server_host, self.__server_port))
        self._sock = sock
        self._shutting_down = False

        # Create directories for results
        if not os.path.exists("results"):
            os.makedirs("results")

        for i in range(NUM_OF_QUERIES):
            with open(f"results/query_{i+1}.txt", "w") as file:
                pass

    def shutdown(self):
        logging.info("SIGTERM received. Initiating Graceful Shutdown.")
        self._shutting_down = True
        raise ShuttingDown

    def run(self):
        try:
            # Connect
            batch = []
            files = [BOOKS_FILE, REVIEWS_FILE]
            parsing_func = [parsing.parse_book_line, parsing.parse_review_line]

            print("Connection accepted. Waiting for server...")
            
            buffer = b""
            while len(buffer) < len(BEGIN_MSG) and not self._shutting_down:
                try:
                    recv = self._sock.recv(len(BEGIN_MSG) - len(buffer))
                    buffer += recv
                    if not recv:
                        raise Exception
                except:    
                    print("Connection error.")
                    buffer += recv
            
            if buffer.decode() != BEGIN_MSG:
                print("Unkown message received from server.")
                return

            
            # Send reviews
            for i in range(2):
                if self._shutting_down:
                    break
                lines_sent = 0
                print(f"Sending {files[i]}")
                with open(files[i], "r") as file:
                    reader = csv.DictReader(file)
                    for line in reader:
                        lines_sent += 1
                        batch.append(parsing_func[i](line))
                        if len(batch) == self.__items_per_batch:
                            if self._shutting_down:
                                break
                            self.__send_batch(batch, False)
                            batch.clear()

                    # Send EOF and remaining batch if any
                    if not self._shutting_down:
                        self.__send_batch(batch, True)
                        batch.clear()
                        print(f"Sent {lines_sent} lines from {files[i]}")

            if self._shutting_down:
                pass
            else:
                print("Data sent. Waiting for results...")
                self.__listen_for_results()
        except BrokenPipeError:
            logging.error(f"Connection to system lost. Shutting down Gracefully.")
        except ShuttingDown:
            pass
        finally:
            try:
                self._sock.shutdown(socket.SHUT_RDWR)
            except:
                pass
            self._sock.close()
            logging.info("Shutdown Completed.")

    def __listen_for_results(self):
        eof_count = 0
        num_of_results = {i+1: 0 for i in range(NUM_OF_QUERIES)}

        while eof_count < NUM_OF_QUERIES and not self._shutting_down:
            response = b""
            while len(response) < BATCH_SIZE_LEN:
                response += self._sock.recv(BATCH_SIZE_LEN)
            
            size = int.from_bytes(response, byteorder="big")
            response = b""
            while len(response) < size:
                response += self._sock.recv(size - len(response))
            
            response = json.loads(response.decode("utf-8"))
            queries = response["queries"]

            if "data" in response:
                for number in queries:
                    with open(f"results/query_{number}.txt", "a") as file:
                            for line in response["data"]:
                                file.write(json.dumps(line) + "\n")
                                num_of_results[number] += 1

            if "EOF" in response:
                for number in queries:
                    eof_count += 1
                    print(f"Query {number} finished: {num_of_results[number]} results")
                

    def __send_batch(self, lines, eof=False):

        msg = {"data": lines}
        if eof:
            msg["EOF"] = True
        encoded_msg = json.dumps(msg).encode("utf-8")
        size = len(encoded_msg).to_bytes(BATCH_SIZE_LEN, byteorder="big")
        
        to_send = size + encoded_msg
        bytes_sent = 0
        while bytes_sent < len(to_send) and not self._shutting_down:
            bytes_sent += self._sock.send(to_send[bytes_sent:])
        
        # print(f"Sent batch of {len(lines)} elements to server. EOF: {eof}")

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

    config = {
        "LOGGING_LEVEL": "INFO",
        "ITEMS_PER_BATCH": 5000,
        "MESSAGING_HOST": "localhost",
        "MESSAGING_PORT": 8080,
    }

    config_logging(config["LOGGING_LEVEL"])

    client = Client(config["ITEMS_PER_BATCH"], config["MESSAGING_HOST"], config["MESSAGING_PORT"])
    signal.signal(signal.SIGTERM, lambda sig, frame: client.shutdown())

    client.run()
