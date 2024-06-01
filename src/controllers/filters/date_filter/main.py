from collections import defaultdict
from src.messaging.goutong import Goutong
from src.messaging.message import Message
import logging
import signal
import os

from src.controller_state.controller_state import ControllerState

from src.utils.config_loader import Configuration
from src.exceptions.shutting_down import ShuttingDown

FILTER_TYPE = "date_filter"
EOF_QUEUE = "date_filter_eof"
CONTROL_GROUP = "CONTROL"


UPPER_Q1 = 2023
LOWER_Q1 = 2000

UPPER_Q3_4 = 1999
LOWER_Q3_4 = 1990

OUTPUT_Q1 = "title_filter_queue"
OUTPUT_Q3_4_PREFIX = "books_queue_"

shutting_down = False


# Graceful Shutdown
def sigterm_handler(messaging: Goutong):
    global shutting_down
    logging.info("SIGTERM received. Initiating Graceful Shutdown.")
    shutting_down = True
    raise ShuttingDown
    # msg = Message({"ShutDown": True})
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
        "FILTER_NUMBER": int,
        "LOGGING_LEVEL": str,
        "ITEMS_PER_BATCH": int,
    }
    filter_config = Configuration.from_file(required, "config.ini")
    filter_config.update_from_env()
    filter_config.validate()

    config_logging(filter_config.get("LOGGING_LEVEL"))
    logging.info(filter_config)

    # Load State
    controller_id = f"{FILTER_TYPE}_{filter_config.get('FILTER_NUMBER')}"

    extra_fields = {
        "filtered_books_q1": [],
        "filtered_books_q3_4": [],
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

    # Set up the queues
    control_queue_name = (
        FILTER_TYPE + str(filter_config.get("FILTER_NUMBER")) + "_control"
    )
    input_queue_name = FILTER_TYPE + str(filter_config.get("FILTER_NUMBER"))

    own_queues = [input_queue_name, control_queue_name, EOF_QUEUE]
    messaging.add_queues(*own_queues)
    messaging.add_queues(OUTPUT_Q1)
    # messaging.add_broadcast_group(CONTROL_GROUP, [control_queue_name])
    # messaging.set_callback(control_queue_name, callback_control, auto_ack=True)

    signal.signal(signal.SIGTERM, lambda sig, frame: sigterm_handler(messaging))

    # Main Flow
    try:
        while not shutting_down:
            if not state.committed:
                handle_uncommited_transactions(messaging, state)
            get_next_message(
                messaging=messaging, input_queue_name=input_queue_name, state=state
            )
    except ShuttingDown:
        pass

    finally:
        logging.info("Shutting Down.")
        messaging.close()
        state.save_to_disk()


def handle_uncommited_transactions(messaging: Goutong, state: ControllerState):
    to_send_q1 = state.get("filtered_books_q1")
    to_send_q3_4 = state.get("filtered_books_q3_4")
    if to_send_q1 or to_send_q3_4:

        _send_batches(
            messaging=messaging,
            batch_q1=to_send_q1,
            batch_q3_4=to_send_q3_4,
            connection_id=state.get("conn_id"),
            queries=state.get("queries"),
            transaction_id=state.id_for_next_transaction(),
        )

    if state.get("EOF"):
        _send_EOF(
            messaging=messaging,
            connection_id=state.get("conn_id"),
            transaction_id=state.id_for_next_transaction(),
        )
    state.mark_transaction_committed()


def get_next_message(messaging: Goutong, input_queue_name: str, state: ControllerState):
    messaging.set_callback(
        input_queue_name, callback_filter, auto_ack=False, args=(state,)
    )
    messaging.listen()


def callback_control(messaging: Goutong, msg: Message):
    global shutting_down
    if msg.has_key("ShutDown"):
        shutting_down = True
        raise ShuttingDown


def _columns_for_query1(book: dict) -> dict:
    return {
        "title": book["title"],
        "authors": book["authors"],
        "publisher": book["publisher"],
        "categories": book["categories"],
    }


def _columns_for_query3_4(book: dict) -> dict:
    return {
        "title": book["title"],
        "authors": book["authors"],
    }


def _send_batches(
    messaging: Goutong,
    batch_q1: list,
    batch_q3_4: list,
    connection_id: int,
    queries: list,
    transaction_id: str,
):
    if batch_q1:
        _send_batch_q1(messaging, batch_q1, connection_id, transaction_id)
    if batch_q3_4:
        _send_batch_q3_4(messaging, batch_q3_4, connection_id, transaction_id)


def _send_batch_q1(
    messaging: Goutong, batch: list, connection_id: int, transaction_id: str
):
    data = list(map(_columns_for_query1, batch))
    msg = Message(
        {
            "transaction_id": transaction_id,
            "conn_id": connection_id,
            "queries": [1],
            "data": data,
        }
    )
    messaging.send_to_queue(OUTPUT_Q1, msg)
    # logging.debug(f"Sent Data to: {OUTPUT_Q1}")


def _send_batch_q3_4(
    messaging: Goutong, batch: list, connection_id: int, transaction_id: str
):
    data = list(map(_columns_for_query3_4, batch))
    msg = Message(
        {
            "transaction_id": transaction_id,
            "conn_id": connection_id,
            "queries": [3, 4],
            "data": data,
        }
    )
    output_queue = OUTPUT_Q3_4_PREFIX + str(connection_id)
    messaging.add_queues(output_queue)
    messaging.send_to_queue(output_queue, msg)
    # logging.debug(f"Sent Data to: {OUTPUT_Q3_4}")


def _send_EOF(messaging: Goutong, connection_id: int, transaction_id: str):
    output_q3_4 = OUTPUT_Q3_4_PREFIX + str(connection_id)
    msg = Message(
        {
            "transaction_id": transaction_id,
            "conn_id": connection_id,
            "queries": [1, 3, 4],
            "EOF": True,
            "forward_to": [OUTPUT_Q1, output_q3_4],
        }
    )
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
    filtered_books_q1, filtered_books_q3_4 = filter_data(books_received)

    conn_id = msg.get("conn_id")
    transaction_id = msg.get("transaction_id")
    queries = msg.get("queries")

    state.set("filtered_books_q1", filtered_books_q1)
    state.set("filtered_books_q3_4", filtered_books_q3_4)
    state.set("conn_id", conn_id)
    state.set("queries", queries)
    state.set("EOF", eof)
    state.set("committed", False)
    state.mark_transaction_received(transaction_id)
    state.save_to_disk()

    # Acknowledge message now that it's saved
    messaging.ack_delivery(msg.delivery_id)
    messaging.stop_consuming(msg.queue_name)
    logging.debug(f"no escucho mas queue {msg.queue_name}")


def filter_data(data: list):
    filtered_data_q1 = []
    filtered_data_q3_4 = []

    for book in data:
        year = book.get("year")
        if LOWER_Q1 <= year <= UPPER_Q1:
            filtered_data_q1.append(book)
        if LOWER_Q3_4 <= year <= UPPER_Q3_4:
            filtered_data_q3_4.append(book)

    if filtered_data_q1 or filtered_data_q3_4:
        logging.info(
            f"Filtered {len(filtered_data_q1) + len(filtered_data_q3_4)} items"
        )
    return (filtered_data_q1, filtered_data_q3_4)


if __name__ == "__main__":
    main()
