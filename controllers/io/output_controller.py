import logging
from messaging.goutong import Goutong
import time

from messaging.message import Message

class EndOfQuery(Exception):
    pass

INPUT_QUEUE = 'results_queue'

def callback_display_results(messaging: Goutong, msg: Message):
    if msg.has_key("EOF"):
        logging.info("End of query")
        time.sleep(1)
        raise EndOfQuery
    
    # qué informacion se muestra depende de la query. por ahora solo el título del libro filtrado (query 1)
    title = msg.get("data").get("title")
    logging.info(title)

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
    