import logging
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
    for book in data:
        logging.info(
            f"[Query 1] |Título: {book['title']}. |Autores: {book['authors']} |Editorial: {book['publisher']}"
        )


def _display_results_q2(data: list):
    for author in data:
        logging.info(f"[Query 2] {author}")


def _display_results_q3(data: list):
    for review_info in data:
        logging.info(
            f"[Query 3] |Título: {review_info['title']} |Autores: {review_info['authors']}"
        )

def _display_results_q4(data: list):
    for book in data:
        logging.info(f"[Query 4] |Libro: {book['title']}")

def callback_display_results(messaging: Goutong, msg: Message):
    global eof_received
    if msg.has_key("EOF"):
        eof_received += 1
        logging.info(f"({eof_received}/{NUMBER_OF_QUERIES}) Queries completed.")
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
        pass
    else:
        logging.info(f"Query {query_number} not supported")


def display_results(messaging: Goutong):
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Messaging Middleware
    messaging.add_queues(INPUT_QUEUE)
    messaging.set_callback(INPUT_QUEUE, callback_display_results)
    try:
        messaging.listen()
    except EndOfQuery:
        logging.info("Stopped listening for results")
