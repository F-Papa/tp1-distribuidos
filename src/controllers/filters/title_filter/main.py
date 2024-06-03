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
        "filtered_books": [],
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


def get_next_message(messaging: Goutong, input_queue_name: str, state: ControllerState):
    messaging.set_callback(
        input_queue_name, callback_title_filter, auto_ack=False, args=(state,)
    )
    messaging.listen()


def handle_uncommited_transactions(messaging: Goutong, state: ControllerState):
    if state.get("filtered_books"):
        _send_batch(
            messaging=messaging,
            batch=state.get("filtered_books"),
            conn_id=state.get("conn_id"),
            queries=state.get("queries"),
            transaction_id=state.id_for_next_transaction(),
        )
    if state.get("EOF"):
        _send_EOF(
            messaging=messaging,
            conn_id=state.get("conn_id"),
            transaction_id=state.id_for_next_transaction() + "_EOF",
        )

    state.mark_transaction_committed()


def callback_title_filter(messaging: Goutong, msg: Message, state: ControllerState):
    transaction_id = msg.get("transaction_id")

    # Ignore duplicate transactions
    if transaction_id in state.transactions_received:
        messaging.ack_delivery(msg.delivery_id)
        logging.info(
            f"Received Duplicate Transaction {msg.get('transaction_id')}: "
            + msg.marshal()[:100]
        )
        return

    # Add new data to state
    books_received = msg.get("data") if msg.has_key("data") else []
    filtered_books = filter_data(books_received)
    eof = msg.has_key("EOF")
    conn_id = msg.get("conn_id")
    transaction_id = msg.get("transaction_id")

    state.set("filtered_books", filtered_books)
    state.set("conn_id", conn_id)
    state.set("queries", msg.get("queries"))
    state.set("EOF", eof)
    state.set("committed", False)
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

    return filtered_data


def _columns_for_query1(book: dict) -> dict:
    return {
        "title": book["title"],
        "authors": book["authors"],
        "publisher": book["publisher"],
        "categories": book["categories"],
    }


def _send_batch(
    messaging: Goutong,
    batch: list,
    conn_id: int,
    queries: list[int],
    transaction_id: str,
):
    msg_content = {
        "transaction_id": transaction_id,
        "conn_id": conn_id,
        "queries": queries,
    }
    if batch:
        data = list(map(_columns_for_query1, batch))
        msg_content["data"] = data

    msg = Message(msg_content)
    messaging.send_to_queue(OUTPUT_Q1, msg)


def _send_EOF(messaging: Goutong, conn_id: int, transaction_id: str):
    msg = Message(
        {
            "transaction_id": transaction_id,
            "conn_id": conn_id,
            "EOF": True,
            "forward_to": [OUTPUT_Q1],
            "queries": [1],
        }
    )
    messaging.send_to_queue(EOF_QUEUE, msg)
    logging.debug(f"Sent EOF to: {EOF_QUEUE}")


if __name__ == "__main__":
    main()
