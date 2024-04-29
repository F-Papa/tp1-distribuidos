import logging
from messaging.goutong import Goutong
import signal

from messaging.message import Message
from exceptions.shutting_down import ShuttingDown


class EndOfQuery(Exception):
    pass


INPUT_QUEUE = "results_queue"


def callback_display_results(messaging: Goutong, msg: Message):
    if msg.has_key("EOF"):
        logging.info("End of query")
        raise EndOfQuery

    # qué informacion se muestra depende de la query. por ahora solo el título del libro filtrado (query 1)
    titles = map(lambda book: book.get("title"), msg.get("data"))
    for title in titles:
        logging.info(title)


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
