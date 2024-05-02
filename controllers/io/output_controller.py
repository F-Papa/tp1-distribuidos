import logging
import time
from messaging.goutong import Goutong
import signal

from messaging.message import Message
from exceptions.shutting_down import ShuttingDown


class EndOfQuery(Exception):
    pass


INPUT_QUEUE = "results_queue"

NUMBER_OF_QUERIES = 5
eof_received = 0


def _display_results_q1(data: list):
    with open("results/query1.txt", "a") as f:
        for book in data:
            result = f"|Título: {book['title']}. |Autores: {book['authors']} |Editorial: {book['publisher']}"
            f.write(result + "\n")
            # logging.info("[Query 1] " + result)


def _display_results_q2(data: list):
    with open("results/query2.txt", "a") as f:
        for author in data:
            result = f"|Autor: {author}"
            f.write(result + "\n")
            # logging.info(f"[Query 2] " + result)


def _display_results_q3(data: list):
    with open("results/query3.txt", "a") as f:
        for review_info in data:
            result = (
                f"|Título: {review_info['title']} |Autores: {review_info['authors']}"
            )
            f.write(result + "\n")
            # logging.info("[Query 3] " + result)


def _display_results_q4(data: list):
    with open("results/query4.txt", "a") as f:
        for book in data:
            result = f"|Libro: {book['title']}"
            f.write(result + "\n")
            # logging.info(f"[Query 4] " + result)

def _display_results_q5(data: list):
    with open("results/query5.txt", "a") as f:
        for book in data:
            result = f"|Libro: {book['title']}"
            f.write(result + "\n")
            # logging.info(f"[Query 5] " + result)


def callback_display_results(messaging: Goutong, msg: Message, start_time):
    global eof_received
    if msg.has_key("EOF"):
        eof_received += 1
        end_time = time.time()
        logging.info(f"({eof_received}/{NUMBER_OF_QUERIES}) Queries completed in {end_time - start_time}s since start.")
        if eof_received == NUMBER_OF_QUERIES:
            raise EndOfQuery
        return

    # que informacion se muestra depende de la query.
    query_number = msg.get("query")
    data = msg.get("data")
    if query_number == 1:
        _display_results_q1(data)
    elif query_number == 2:
        _display_results_q2(data)
    elif query_number == 3:
        _display_results_q3(data)
    elif query_number == 4:
        _display_results_q4(data)
    elif query_number == 5:
        _display_results_q5(data)
    else:
        logging.info(f"Query {query_number} not supported")


def display_results(messaging: Goutong):
    start = time.time()
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Messaging Middleware
    messaging.add_queues(INPUT_QUEUE)
    messaging.set_callback(INPUT_QUEUE, callback_display_results, (start,))
    try:
        messaging.listen()
    except EndOfQuery:
        logging.info("Stopped listening for results")
