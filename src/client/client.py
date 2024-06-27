import csv
import json
import logging
import os
import socket
import sys
import threading
import time
import common.parsing as parsing
import chalk

# BOOKS_FILE = "../../data/test/books_data11.csv"
BOOKS_FILE = "../../data/books_data.csv"
# REVIEWS_FILE = "../../data/test/ratings_1K.csv"
REVIEWS_FILE = "../../data/test/Books_rating_reduced.csv"
# REVIEWS_FILE = "../../data/Books_rating.csv"

BATCH_SIZE_LEN = 8
NUM_OF_QUERIES = 5
BEGIN_MSG = "BEGIN"


class CLI():

    def __init__(self):
        self.start_time = None
        
    def print_info(self, ):
        yellow = chalk.yellow
        bold = chalk.bold
        uline = chalk.underline
        print(f"{bold(uline('Amazon Books Analyzer'))}")
        print()
        print(f"{bold('Query 1')}: {yellow('Books')} from the 'Computers' category published between 2000 and 2023 with 'distributed' in their title.")
        print(f"{bold('Query 2')}: {yellow('Authors')} who have published books in at least 10 different decades.")
        print(f"{bold('Query 3')}: {yellow('Title')} and {yellow('authors')} of books published in the 90's with at least 500 reviews.")
        print(f"{bold('Query 4')}: 10 best rated {yellow('books')} published in the 90's with at least 500 reviews.")
        print(f"{bold('Query 5')}: {yellow('Books')} from the 'Fiction' category among the 90th quantile of average review sentiment.")


    def show_results_file(self):
        print()
        print("Results can be found inside the directory named 'results'.")
        

    def print_credits(self, ):
        bold = chalk.bold
        uline = chalk.underline
        print()
        print(f"{bold(uline('Authors'))}:  Franco Papa and Andrés Moyano")
        print(f"2024, Faculty of Engineering, Univesity of Buenos Aires")

    def on_hold(self, ):
        green = chalk.green
        bold = chalk.bold
        print()
        print(f"{green(bold('Connected'))} succesfully.")
        print(f"You are on hold...", end='', flush=True)

    def sending_books(self, ):
        self.start_time = time.time()
        print("\b"*1000, end="", flush=True)
        print("Sending books...   ", end="", flush=True)

    def sending_reviews(self, ):
        print("\b"*1000, end="", flush=True)
        print("Sending Reviews...", end="", flush=True)

    def waiting_for_results(self, ):
        print("\b"*1000, end="", flush=True)
        print("Waiting for results...", end="", flush=True)

    def query_results(self, number: int, num_of_results: int):
        bold = chalk.bold
        seconds = time.time() - self.start_time # type: ignore
        hours = seconds // 3600
        seconds %= 3600
        minutes = seconds // 60
        seconds %= 60
    
    # Format with leading zeros
        hours_str = str(int(hours)).zfill(2)
        minutes_str = str(int(minutes)).zfill(2)
        seconds_str = str(int(seconds)).zfill(2)


        print("\b"*1000, end="", flush=True)
        print(bold(f"Query {number}:"), end='')
        print(f" {num_of_results} results. ({hours_str}:{minutes_str}:{seconds_str})")
    
    def error(self, text: str):
        bold = chalk.bold
        red = chalk.red
        yellow = chalk.red
        print(f"\n{red(bold('ERROR'))}: {yellow(text)}")

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

        # Create directories for results
        if not os.path.exists("results"):
            os.makedirs("results")

        for i in range(NUM_OF_QUERIES):
            with open(f"results/query_{i+1}.txt", "w") as file:
                pass

    def run(self, cli: CLI):
        # Connect

        cli.print_info()

        cli.on_hold()

        batch = []
        files = [BOOKS_FILE, REVIEWS_FILE]
        parsing_func = [parsing.parse_book_line, parsing.parse_review_line]

        
        buffer = b""
        while len(buffer) < len(BEGIN_MSG):
            try:
                recv = self._sock.recv(len(BEGIN_MSG) - len(buffer))
                buffer += recv
                if not recv:
                    raise Exception
            except:    
                cli.error("Connection failed.")
                return
        
        if buffer.decode() != BEGIN_MSG:
            cli.error('Unknown message received.')
            return

        sending = [cli.sending_books, cli.sending_reviews]
        # Send reviews
        for i in range(2):
            lines_sent = 0
            sending[i]()
            with open(files[i], "r") as file:
                reader = csv.DictReader(file)
                for line in reader:
                    lines_sent += 1
                    batch.append(parsing_func[i](line))
                    if len(batch) == self.__items_per_batch:
                        self.__send_batch(batch, False)
                        batch.clear()

                # Send EOF and remaining batch if any
                self.__send_batch(batch, True)
                batch.clear()

        print()
        self.__listen_for_results(cli)
        cli.show_results_file()
        cli.print_credits()

    def __listen_for_results(self, cli: CLI):
        eof_count = 0
        num_of_results = {i+1: 0 for i in range(NUM_OF_QUERIES)}

        while eof_count < NUM_OF_QUERIES:
            cli.waiting_for_results()
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
                    cli.query_results(number, num_of_results[number])
                

    def __send_batch(self, lines, eof=False):

        msg = {"data": lines}
        if eof:
            msg["EOF"] = True
        encoded_msg = json.dumps(msg).encode("utf-8")
        size = len(encoded_msg).to_bytes(BATCH_SIZE_LEN, byteorder="big")
        
        to_send = size + encoded_msg
        bytes_sent = 0
        while bytes_sent < len(to_send):
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
    client.run(CLI())



