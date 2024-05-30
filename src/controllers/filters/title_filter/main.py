import json
import os
from src.messaging.goutong import Goutong
from src.exceptions.shutting_down import ShuttingDown
import logging
import signal

from src.messaging.message import Message
from src.utils.config_loader import Configuration
from src.exceptions.shutting_down import ShuttingDown

FILTER_TYPE = "title_filter"
EOF_QUEUE = "title_filter_eof"
CONTROL_GROUP = "CONTROL"

KEYWORD_Q1 = "distributed"
OUTPUT_Q1 = "category_filter_queue"

shutting_down = False


class FilterState:
    READY_MARKER = "=-#READY#-="

    def __init__(
        self,
        file_path: str,
        committed: bool,
        transactions_received: list[str],
        next_transaction: int,
        data: list,
        transaction_prefix: str,
        conn_id: int,
        queries: list[int],
        eof: bool,
    ):
        self.conn_id = conn_id
        self.queries = queries
        self.eof = eof
        self.file_path = file_path
        self.committed = committed
        self.transactions_received = transactions_received
        self.next_transaction = next_transaction
        self.data = data
        self.transaction_prefix = transaction_prefix

    @classmethod
    def create_default(cls, file_path: str, transaction_prefix: str):
        return FilterState(
            file_path=file_path,
            committed=True,
            transactions_received=[],
            next_transaction=1,
            data=[],
            transaction_prefix=transaction_prefix,
            conn_id=0,
            queries=[],
            eof=False,
        )

    @classmethod
    def load_or_init(cls, file_path: str, transaction_prefix: str):

        if not os.path.exists(file_path):
            logging.warning("State File Not Found. Creating New State.")
            return cls.create_default(file_path, transaction_prefix)

        loaded_state = dict()

        with open(file_path, "r") as f:
            lines = f.readlines()

        is_valid = len(lines) == 2 and lines[-1] == cls.READY_MARKER

        if not is_valid:
            raise Exception("Invalid State File")

        loaded_state = json.loads(lines[0])

        return cls(
            file_path,
            loaded_state["committed"],
            loaded_state["transactions_received"],
            loaded_state["next_transaction"],
            loaded_state["data"],
            transaction_prefix,
            loaded_state["conn_id"],
            loaded_state["queries"],
            loaded_state["eof"],
        )

    def is_transaction_received(self, transaction_id: str) -> bool:
        return transaction_id in self.transactions_received

    def id_for_next_transaction(self) -> str:
        return f"{self.transaction_prefix}{self.next_transaction}"

    def mark_transaction_received(self, transaction_id: str):
        self.transactions_received.append(transaction_id)
        self.save_to_disk()

    def new_data(
        self,
        data: list,
        transaction_id: str,
        queries: list[int],
        conn_id: int,
        eof: bool = False,
    ):
        self.data = data
        self.committed = False
        self.transactions_received.append(transaction_id)
        self.queries = queries
        self.conn_id = conn_id
        self.eof = eof
        self.save_to_disk()

    def mark_transaction_committed(self):
        self.committed = True
        self.next_transaction += 1
        self.save_to_disk()

    def save_to_disk(self):
        tmp_file = self.file_path + ".tmp"

        to_save = {
            "committed": self.committed,
            "transactions_received": self.transactions_received,
            "next_transaction": self.next_transaction,
            "data": self.data,
            "conn_id": self.conn_id,
            "queries": self.queries,
            "eof": self.eof,
        }

        # to_show = to_save.copy()
        # to_show["data"] = f'{len(to_show["data"])} items' if to_show["data"] else "None"
        # to_show["transactions_received"] = f'{len(to_show["transactions_received"])} items' if to_show["transactions_received"] else "None"

        # if None in to_save["transactions_received"]: 
        #     to_show["transactions_received"] += " + None"

        # logging.info(f"Saving State to Disk: {to_show}")

        with open(tmp_file, "w") as f:
            f.write(json.dumps(to_save) + "\n")
            f.write(self.READY_MARKER)

        os.replace(tmp_file, self.file_path)


# Graceful Shutdown
def sigterm_handler():
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
    filter_id = f"{FILTER_TYPE}_{filter_config.get('FILTER_NUMBER')}"
    state = FilterState.load_or_init("state/state_file", filter_id)

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


def handle_uncommited_transactions(messaging: Goutong, state: FilterState):
    if state.data:
        to_send = filter_data(state.data)

        _send_batch_q1(messaging, to_send, state.conn_id, state.queries)
    if state.eof:
        _send_EOF(messaging, state.conn_id)
    state.mark_transaction_committed()


def main_loop(messaging: Goutong, input_queue_name: str, state: FilterState):
    messaging.set_callback(
        input_queue_name, receive_new_data, auto_ack=False, args=(state,)
    )
    messaging.listen()
    if state.data:
        to_send = filter_data(state.data)
        _send_batch_q1(messaging, to_send, state.conn_id, state.queries)
    if state.eof:
        _send_EOF(messaging, state.conn_id)

    state.mark_transaction_committed()


def receive_new_data(messaging: Goutong, msg: Message, state: FilterState):

    # Any book has a title with the keyword "distributed"
    if msg.get("transaction_id") in state.transactions_received:
        messaging.ack_delivery(msg.delivery_id)
        logging.info(f"Received Duplicate Transaction {msg.get('transaction_id')}")
        return

    eof = msg.has_key("EOF")

    state.new_data(
        data=msg.get("data"),
        transaction_id=msg.get("transaction_id"),
        queries=msg.get("queries"),
        conn_id=msg.get("conn_id"),
        eof=eof,
    )

    messaging.ack_delivery(msg.delivery_id)
    messaging.stop_consuming(msg.queue_name)


def filter_data(data: list):
    to_return = []

    for book in data:
        title = book.get("title")
        if KEYWORD_Q1.lower() in title.lower():
            to_return.append(book)

    if to_return:
        logging.info(f"Filtered {len(to_return)} items")
    return to_return


# def callback_control(messaging: Goutong, msg: Message):
#     global shutting_down
#     if msg.has_key("ShutDown"):
#         shutting_down = True
#         raise ShuttingDown


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
        logging.info("Sending Batch to Category Filter")


def _send_EOF(messaging: Goutong, conn_id: int):
    msg = Message(
        {"conn_id": conn_id, "EOF": True, "forward_to": [OUTPUT_Q1], "queries": [1]}
    )
    messaging.send_to_queue(EOF_QUEUE, msg)
    logging.debug(f"Sent EOF to: {EOF_QUEUE}")


# def callback_filter(messaging: Goutong, msg: Message, config: Configuration):
#     # logging.debug(f"Received: {msg.marshal()}")

#     queries = msg.get("queries")
#     connection_id = msg.get("conn_id")

#     if msg.has_key("EOF"):
#         # Forward EOF and Keep Consuming
#         _send_EOF(messaging, connection_id)
#         return

#     books = msg.get("data")
#     batch_q1 = []

#     for book in books:
#         title = book.get("title")

#         # Query 1 (Only) Flow
#         if KEYWORD_Q1.lower() in title.lower():
#             batch_q1.append(book)
#             if len(batch_q1) >= config.get("ITEMS_PER_BATCH"):
#                 _send_batch_q1(messaging, batch_q1, connection_id)
#                 batch_q1.clear()

#     # Send remaining items
#     if batch_q1:
#         _send_batch_q1(messaging, batch_q1, connection_id)


if __name__ == "__main__":
    main()
