from messaging.goutong import Goutong
from messaging.message import Message
import logging
import signal

from utils.config_loader import Configuration
from exceptions.shutting_down import ShuttingDown

FILTER_TYPE = "date_filter"
EOF_QUEUE = "date_filter_eof"
CONTROL_GROUP = "CONTROL"

shutting_down = False

# Graceful Shutdown
def sigterm_handler(messaging: Goutong):
    global shutting_down
    logging.info('SIGTERM received. Iitiating Graceful Shutdown.')
    shutting_down = True
    msg = Message({"ShutDown": True})
    messaging.broadcast_to_group(CONTROL_GROUP, msg)

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
    required = {
        "FILTER_NUMBER": int,
        "LOWER_BOUND": int,
        "UPPER_BOUND": int,
        "LOGGING_LEVEL": str,
        "ITEMS_PER_BATCH": int,
    }
    filter_config = Configuration.from_file(required, "config.ini")
    filter_config.update_from_env()
    filter_config.validate()

    config_logging(filter_config.get("LOGGING_LEVEL"))
    logging.info(filter_config)

    messaging = Goutong()

    control_queue_name = FILTER_TYPE + str(filter_config.get("FILTER_NUMBER")) + "_control"
    messaging.add_queues(control_queue_name)
    messaging.add_broadcast_group(CONTROL_GROUP, [control_queue_name])
    messaging.set_callback(control_queue_name, callback_control, ())

    signal.signal(signal.SIGTERM, lambda sig, frame: sigterm_handler(messaging))

    if not shutting_down:
        input_queue_name = FILTER_TYPE + str(filter_config.get("FILTER_NUMBER"))
        messaging.add_queues(input_queue_name)
        messaging.set_callback(input_queue_name, callback_filter, (filter_config,))
    
    if not shutting_down:
        try:
            messaging.listen()
        except ShuttingDown:
            logging.debug("Shutdown Message Received via Control Broadcast")
        finally:
            messaging.close()
    logging.info("Shutting Down.")


def callback_control(messaging: Goutong, msg: Message):
    global shutting_down
    if msg.has_key("ShutDown"):
        shutting_down = True
        raise ShuttingDown
            


def _send_batch(messaging: Goutong, batch: list, route: list):
    msg_content = {"data": batch, "route": route}
    msg = Message(msg_content)
    messaging.send_to_queue(route[0][0], msg)
    logging.debug(f"Sent Data to: {route[0][0]}")


def _send_EOF(messaging: Goutong, route: list):
    msg = Message({"EOF": True, "route": route})
    messaging.send_to_queue(EOF_QUEUE, msg)
    logging.debug(f"Sent EOF to: {EOF_QUEUE}")


def callback_filter(messaging: Goutong, msg: Message, config: Configuration):
    # logging.debug(f"Received: {msg.marshal()}")

    route = msg.get("route")
    _, params = route.pop(0)

    if msg.has_key("EOF"):
        # Forward EOF and Keep Consuming
        _send_EOF(messaging, route)
        return
    
    config.update("LOWER_BOUND", params[0])
    config.update("UPPER_BOUND", params[1])


    books = msg.get("data")
    batch = []

    for book in books:
        year = book.get("year")
        lower_bound = config.get("LOWER_BOUND")
        upper_bound = config.get("UPPER_BOUND")
        if lower_bound <= year <= upper_bound:
            if len(batch) < config.get("ITEMS_PER_BATCH"):
                batch.append(book)
            else:
                _send_batch(messaging, batch, route)
                batch = []
    if len(batch) > 0:
        _send_batch(messaging, batch, route)


if __name__ == "__main__":
    main()
