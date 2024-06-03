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

    messaging = Goutong()

    # Set up queues
    control_queue_name = (
        FILTER_TYPE + str(filter_config.get("FILTER_NUMBER")) + "_control"
    )
    input_queue_name = FILTER_TYPE + str(filter_config.get("FILTER_NUMBER"))

    messaging.add_queues(input_queue_name, control_queue_name, EOF_QUEUE)

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


def get_next_message(messaging: Goutong, input_queue_name: str, state: ControllerState):
    messaging.set_callback(
        input_queue_name, callback_filter, auto_ack=False, args=(state,)
    )
    messaging.listen()


def handle_uncommited_transactions(messaging: Goutong, state: ControllerState):
    queries = state.get("queries")
    filtered_books = state.get("filtered_books")
    if filtered_books:
        _send_batch(
            messaging=messaging,
            batch=filtered_books,
            conn_id=state.get("conn_id"),
            queries=queries,
            transaction_id=state.id_for_next_transaction(),
        )

    if state.get("EOF"):
        _send_EOF_by_queryID(
            messaging=messaging,
            connection_id=state.get("conn_id"),
            queries=queries,
            transaction_id=state.id_for_next_transaction() + "_EOF",
        )

    state.mark_transaction_committed()


def _send_EOF_by_queryID(
    messaging: Goutong, connection_id: int, queries: list, transaction_id: str
):
    logging.info(f"SENDING EOF TO {queries}")
    if 1 in queries:
        output_queue = OUTPUT_Q1_PREFIX + str(connection_id)
        _send_EOF(
            messaging,
            forward_to=output_queue,
            connection_id=connection_id,
            queries=[1],
            transaction_id=transaction_id,
        )
    if 5 in queries:
        output_queue = OUTPUT_Q5_PREFIX + str(connection_id)
        _send_EOF(
            messaging,
            forward_to=output_queue,
            connection_id=connection_id,
            queries=[5],
            transaction_id=transaction_id,
        )


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


def _send_batch(
    messaging: Goutong, batch: list, conn_id: int, queries: list, transaction_id: str
):
    if 1 in queries and batch:
        _send_batch_q1(messaging, batch, conn_id, transaction_id)
    if 5 in queries and batch:
        _send_batch_q5(messaging, batch, conn_id, transaction_id)


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
    output_queue = OUTPUT_Q1_PREFIX + str(connection_id)
    messaging.send_to_queue(output_queue, msg)
    logging.debug(f"Sent Data to: {output_queue}")


def _send_batch_q5(
    messaging: Goutong, batch: list, connection_id: int, transaction_id: str
):
    data = list(map(_columns_for_query5, batch))
    msg = Message(
        {
            "transaction_id": transaction_id,
            "conn_id": connection_id,
            "queries": [5],
            "data": data,
        }
    )

    output_queue = OUTPUT_Q5_PREFIX + str(connection_id)
    messaging.add_queues(output_queue)
    messaging.send_to_queue(output_queue, msg)
    logging.debug(f"Sent Data to: {output_queue}")


def _send_EOF(
    messaging: Goutong,
    forward_to: str,
    connection_id: int,
    queries: list,
    transaction_id: str,
):

    msg = Message(
        {
            "transaction_id": transaction_id,
            "conn_id": connection_id,
            "EOF": True,
            "forward_to": [forward_to],
            "queries": queries,
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
        logging.info(
            f"Received Duplicate Transaction {msg.get('transaction_id')}: "
            + msg.marshal()[:100]
        )
        return

    # Add new data to state
    eof = msg.has_key("EOF")
    books_received = msg.get("data") if msg.has_key("data") else []

    if 1 in msg.get("queries"):
        queries = [1]
    elif 5 in msg.get("queries"):
        queries = [5]
    else:
        to_show = {
            "transaction_id": msg.get("transaction_id"),
            "conn_id": msg.get("conn_id"),
            "queries": msg.get("queries"),
            "data": msg.get("data"),
            "EOF": msg.get("EOF"),
        }
        raise ValueError(f"Invalid queries: {to_show}")
    filtered_books = filter_data(books_received, queries)

    conn_id = msg.get("conn_id")
    transaction_id = msg.get("transaction_id")

    state.set("filtered_books", filtered_books)
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


def filter_data(data: list, queries: list):
    filtered_books = []

    for book in data:
        categories = book.get("categories")

        if 1 in queries and CATEGORY_Q1 in categories:
            filtered_books.append(book)
        if 5 in queries and CATEGORY_Q5 in categories:
            filtered_books.append(book)

    return filtered_books


if __name__ == "__main__":
    main()
