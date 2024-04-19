import logging
from messaging.goutong import Goutong
import sys

INPUT_QUEUE = 'results_queue'

def callback_display_results(channel, method, properties, body):
    msg = body.decode()
    logging.info(msg)

def display_results():
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    # Messaging Middleware
    messaging = Goutong()
    messaging.add_queues(INPUT_QUEUE)
    messaging.set_callback(INPUT_QUEUE, callback_display_results)
    messaging.listen()

