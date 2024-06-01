from collections import defaultdict
from src.messaging.goutong import Goutong
from src.utils.config_loader import Configuration
import logging
import signal
import os

from src.controller_state.controller_state import ControllerState


from src.messaging.message import Message
from src.exceptions.shutting_down import ShuttingDown

FILTER_TYPE = "category_filter"
EOF_QUEUE = "category_filter_eof"
CONTROL_GROUP = "CONTROL"

CATEGORY_Q1 = "Computers"
CATEGORY_Q5 = "Fiction"

OUTPUT_Q1_PREFIX = "results_"
OUTPUT_Q5_PREFIX = "books_queue_"

shutting_down = False


# Graceful Shutdown
def sigterm_handler(messaging: Goutong):
    global shutting_down
    logging.info("SIGTERM received. Initiating Graceful Shutdown.")
    shutting_down = True
    raise ShuttingDown
    # #msg = Message({"ShutDown": True})
    # messaging.broadcast_to_group(CONTROL_GROUP, msg)


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
        "LOGGING_LEVEL": str,
        "ITEMS_PER_BATCH": int,
        "FILTER_NUMBER": int,
    }

    filter_config = Configuration.from_file(required, "config.ini")
    filter_config.update_from_env()
    filter_config.validate()

    config_logging(filter_config.get("LOGGING_LEVEL"))
    logging.info(filter_config)

    # Load State
    controller_id = f"{FILTER_TYPE}_{filter_config.get('FILTER_NUMBER')}"

    extra_fields = {
        "books_received": [],
        "conn_id": 0,
        "queries": [],
        "EOF": False,
    }

    state = ControllerState(
        controller_id=controller_id,
        file_path=f"state/{controller_id}.json",
        temp_file_path=f"state/{controller_id}.tmp",
        extra_fields=extra_fields,
    )

    if os.path.exists(state.file_path):
        state.update_from_file(state.file_path)

    messaging = Goutong()


    # Set up queues
    control_queue_name = (
        FILTER_TYPE + str(filter_config.get("FILTER_NUMBER")) + "_control"
    )
    input_queue_name = FILTER_TYPE + str(filter_config.get("FILTER_NUMBER"))

    own_queues = [input_queue_name, control_queue_name, EOF_QUEUE]
    messaging.add_queues(*own_queues)

    #messaging.add_broadcast_group(CONTROL_GROUP, [control_queue_name])
    #messaging.set_callback(control_queue_name, callback_control, auto_ack=True)

    signal.signal(signal.SIGTERM, lambda sig, frame: sigterm_handler(messaging))

    # Main Flow
    try:
        if not state.committed and not shutting_down:
            handle_uncommited_transactions(messaging, state)
        while not shutting_down:
            main_loop(messaging, input_queue_name, state)
    except ShuttingDown:
        pass

    finally:
        logging.info("Shutting Down.")
        messaging.close()
        state.save_to_disk()

def handle_uncommited_transactions(messaging: Goutong, state: ControllerState):
    queries = state.get("queries")
    if state.get("books_received"):
        to_send = filter_data(state.get("books_received"), queries)
        _send_batch(
            messaging=messaging,
            batch=to_send,
            conn_id=state.get("conn_id"),
            queries=queries
        )
    if state.get("EOF"):
        logging.info("2")
        _send_EOF_by_queryID(messaging=messaging, connection_id=state.get("conn_id"), queries=queries)
    state.mark_transaction_committed()

def main_loop(messaging: Goutong, input_queue_name: str, state: ControllerState):
    messaging.set_callback(
        input_queue_name, callback_filter, auto_ack=False, args=(state,)
    )
    logging.debug(f"escucho {input_queue_name}")
    messaging.listen()

    if state.get("books_received"):
        queries = state.get("queries")
        to_send = filter_data(state.get("books_received"), queries)
        _send_batch(
            messaging=messaging,
            batch=to_send,
            conn_id=state.get("conn_id"),
            queries=queries,
        )
    if state.get("EOF"):
        queries = state.get("queries")
        connection_id = state.get("conn_id")
        logging.info("1")
        _send_EOF_by_queryID(messaging, connection_id, queries)

    state.mark_transaction_committed()

def _send_EOF_by_queryID(messaging: Goutong, connection_id: int, queries: list):
        logging.info(f"SENDING EOF TO {queries}")
        if 1 in queries:
            output_queue = OUTPUT_Q1_PREFIX + str(connection_id)
            _send_EOF(messaging, output_queue, connection_id, [1])
        if 5 in queries:
            output_queue = OUTPUT_Q5_PREFIX + str(connection_id)
            _send_EOF(messaging, output_queue, connection_id, [5])

def callback_control(messaging: Goutong, msg: Message):
    global shutting_down
    if msg.has_key("ShutDown"):
        shutting_down = True
        raise ShuttingDown


def _columns_for_query1(book: dict) -> dict:
    return {
        "title": book.get("title"),
        "authors": book["authors"],
        "publisher": book["publisher"],
    }


def _columns_for_query5(book: dict) -> dict:
    return {
        "title": book.get("title"),
    }

def _send_batch(messaging: Goutong, batch: list, conn_id: int, queries: list):
    if 1 in queries and batch:
        _send_batch_q1(messaging, batch, conn_id)
    elif 5 in queries and batch:
        _send_batch_q5(messaging, batch, conn_id)

def _send_batch_q1(messaging: Goutong, batch: list, connection_id: int):
    data = list(map(_columns_for_query1, batch))
    msg = Message({"conn_id": connection_id, "queries": [1], "data": data})
    output_queue = OUTPUT_Q1_PREFIX + str(connection_id)
    messaging.send_to_queue(output_queue, msg)
    logging.debug(f"Sent Data to: {output_queue}")


def _send_batch_q5(messaging: Goutong, batch: list, connection_id: int):
    data = list(map(_columns_for_query5, batch))
    msg = Message({"conn_id": connection_id, "queries": [5], "data": data})

    output_queue = OUTPUT_Q5_PREFIX + str(connection_id)
    messaging.add_queues(output_queue)
    messaging.send_to_queue(output_queue, msg)
    logging.debug(f"Sent Data to: {output_queue}")


def _send_EOF(messaging: Goutong, forward_to: str, connection_id: int, queries: list):
    msg = Message({"conn_id": connection_id, "EOF": True, "forward_to": [forward_to], "queries": queries})
    messaging.send_to_queue(EOF_QUEUE, msg)
    logging.debug(f"Sent EOF to: {EOF_QUEUE}")


def callback_filter(messaging: Goutong, msg: Message, state: ControllerState):
    # logging.debug(f"Received: {msg.marshal()}")
    transaction_id = msg.get("transaction_id")

    # Ignore duplicate transactions
    if transaction_id in state.transactions_received:
        messaging.ack_delivery(msg.delivery_id)
        logging.info(f"Received Duplicate Transaction {msg.get('transaction_id')}")
        return
    
    # Add new data to state
    eof = msg.has_key("EOF")
    books_received = msg.get("data") if msg.has_key("data") else []
    conn_id = msg.get("conn_id")
    transaction_id = msg.get("transaction_id")
    queries = msg.get("queries")

    state.set("books_received", books_received)
    state.set("conn_id", conn_id)
    state.set("queries", queries)
    state.set("EOF", eof)
    state.mark_transaction_received(transaction_id)
    state.save_to_disk()
    
    messaging.ack_delivery(msg.delivery_id)
    messaging.stop_consuming(msg.queue_name)
    logging.debug(f"no escucho mas queue {msg.queue_name}")

def filter_data(data: list, queries: list):
    filtered_data = []

    for book in data:
        categories = book.get("categories")

        if 1 in queries and CATEGORY_Q1 in categories:
            filtered_data.append(book)
        if 5 in queries and CATEGORY_Q5 in categories:
            filtered_data.append(book)

    if filtered_data:
        logging.info(f"Filtered {len(filtered_data)} items")
    return filtered_data

if __name__ == "__main__":
    main()
