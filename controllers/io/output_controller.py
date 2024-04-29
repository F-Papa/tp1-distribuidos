import logging
from messaging.goutong import Goutong
import signal

from messaging.message import Message


class EndOfQuery(Exception):
    pass

class ShuttingDown(Exception):
    pass

INPUT_QUEUE = "results_queue"
CONTROL_GROUP = "CONTROL"

def sigterm_handler(messaging: Goutong, shutting_down):
    logging.info('SIGTERM received. Iitiating Graceful Shutdown.')
    shutting_down.value = 1
    msg = Message({"ShutDown": True})
    messaging.broadcast_to_group(CONTROL_GROUP, msg)

def callback_display_results(messaging: Goutong, msg: Message):
    if msg.has_key("EOF"):
        logging.info("End of query")
        raise EndOfQuery

    # qué informacion se muestra depende de la query. por ahora solo el título del libro filtrado (query 1)
    titles = map(lambda book: book.get("title"), msg.get("data"))
    for title in titles:
        logging.info(title)


def display_results(shutting_down):
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Messaging Middleware
    messaging = Goutong()

    control_queue_name = "boundary_control"
    messaging.add_queues(control_queue_name)
    messaging.add_broadcast_group(CONTROL_GROUP, [control_queue_name])
    messaging.set_callback(control_queue_name, callback_control, (shutting_down,))

    signal.signal(signal.SIGTERM, lambda sig, frame: sigterm_handler(messaging, shutting_down))

    if not shutting_down.value:
        messaging.add_queues(INPUT_QUEUE)
        messaging.set_callback(INPUT_QUEUE, callback_display_results)
    try:
        messaging.listen()
    except EndOfQuery:
        logging.info("Stopped listening for results")
    except ShuttingDown:
        logging.debug("Shutdown Message Received via Control Broadcast")
    
    logging.info("Output Controller Shutting Down")

def callback_control(messaging: Goutong, msg: Message, shutting_down):
    if msg.has_key("ShutDown"):
        shutting_down.value = True
        raise ShuttingDown
