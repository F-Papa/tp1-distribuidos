import logging
from messaging.goutong import Goutong
import sys

class EndOfQuery(Exception):
    pass

INPUT_QUEUE = 'results_queue'

def callback_display_results(channel, method, properties, body):
    msg = body.decode()
    if msg == "EOF":
        logging.info("End of query")
        raise EndOfQuery
    logging.info(msg)

def display_results():
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    # Messaging Middleware
    messaging = Goutong()
    messaging.add_queues(INPUT_QUEUE)
    messaging.set_callback(INPUT_QUEUE, callback_display_results)
    try:
        messaging.listen()
    except EndOfQuery:
        logging.info("Stopped listening for results")
    