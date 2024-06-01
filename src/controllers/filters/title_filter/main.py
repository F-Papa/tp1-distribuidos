import json
import os
from src.controller_state.controller_state import ControllerState
from src.messaging.goutong import Goutong
from src.exceptions.shutting_down import ShuttingDown
import logging
import signal

from src.messaging.message import Message
from src.utils.config_loader import Configuration

FILTER_TYPE = "title_filter"
EOF_QUEUE = "title_filter_eof"
CONTROL_GROUP = "CONTROL"

KEYWORD_Q1 = "distributed"
OUTPUT_Q1 = "category_filter_queue"

shutting_down = False


# Graceful Shutdown
def sigterm_handler():
    global shutting_down
    logging.info("SIGTERM received. Initiating Graceful Shutdown.")
    shutting_down = True
    raise ShuttingDown


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

    # Set up queues
    control_queue_name = (
        FILTER_TYPE + str(filter_config.get("FILTER_NUMBER")) + "_control"
    )
    input_queue_name = FILTER_TYPE + str(filter_config.get("FILTER_NUMBER"))

    messaging = Goutong()
    messaging.add_queues(control_queue_name, input_queue_name, EOF_QUEUE, OUTPUT_Q1)

    # Set up signal handler
    signal.signal(signal.SIGTERM, lambda sig, frame: sigterm_handler())

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
    if state.get("books_received"):
        to_send = filter_data(state.get("books_received"))
        _send_batch_q1(
            messaging=messaging,
            batch=to_send,
            conn_id=state.get("conn_id"),
            queries=state.get("queries"),
        )
    if state.get("EOF"):
        _send_EOF(messaging=messaging, conn_id=state.get("conn_id"))
    state.mark_transaction_committed()


def main_loop(messaging: Goutong, input_queue_name: str, state: ControllerState):
    messaging.set_callback(
        input_queue_name, callback_title_filter, auto_ack=False, args=(state,)
    )
    logging.debug(f"escucho {input_queue_name}")
    messaging.listen()

    if state.get("books_received"):
        to_send = filter_data(state.get("books_received"))
        _send_batch_q1(
            messaging=messaging,
            batch=to_send,
            conn_id=state.get("conn_id"),
            queries=state.get("queries"),
        )

    if state.get("EOF"):
        _send_EOF(messaging, state.get("conn_id"))

    state.mark_transaction_committed()


def callback_title_filter(messaging: Goutong, msg: Message, state: ControllerState):
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

    state.set("books_received", books_received)
    state.set("conn_id", conn_id)
    state.set("queries", msg.get("queries"))
    state.set("EOF", eof)
    state.mark_transaction_received(transaction_id)
    state.save_to_disk()

    # Acknowledge message now that it's saved
    messaging.ack_delivery(msg.delivery_id)
    messaging.stop_consuming(msg.queue_name)
    logging.debug(f"no escucho mas queue {msg.queue_name}")


def filter_data(data: list):
    filtered_data = []

    for book in data:
        title = book.get("title")
        if KEYWORD_Q1.lower() in title.lower():
            filtered_data.append(book)

    if filtered_data:
        logging.info(f"Filtered {len(filtered_data)} items")
    return filtered_data


def _columns_for_query1(book: dict) -> dict:
    return {
        "title": book["title"],
        "authors": book["authors"],
        "publisher": book["publisher"],
        "categories": book["categories"],
    }


def _send_batch_q1(messaging: Goutong, batch: list, conn_id: int, queries: list[int]):
    if batch:
        data = list(map(_columns_for_query1, batch))
        msg_content = {"conn_id": conn_id, "queries": [1], "data": data}
        msg = Message(msg_content)
        messaging.send_to_queue(OUTPUT_Q1, msg)
        logging.debug("Sending Batch to Category Filter")


def _send_EOF(messaging: Goutong, conn_id: int):
    msg = Message(
        {"conn_id": conn_id, "EOF": True, "forward_to": [OUTPUT_Q1], "queries": [1]}
    )
    messaging.send_to_queue(EOF_QUEUE, msg)
    logging.debug(f"Sent EOF to: {EOF_QUEUE}")


if __name__ == "__main__":
    main()
