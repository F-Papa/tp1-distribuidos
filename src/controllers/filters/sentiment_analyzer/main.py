from src.messaging.goutong import Goutong
from src.utils.config_loader import Configuration
import logging
import signal
import os
from textblob import TextBlob  # type: ignore

from src.messaging.message import Message
from src.exceptions.shutting_down import ShuttingDown
from src.controller_state.controller_state import ControllerState


FILTER_TYPE = "sentiment_analyzer"

OUTPUT_QUEUE = "sentiment_average_queue"
EOF_QUEUE = OUTPUT_QUEUE  # "sentiment_analyzer_eof"

shutting_down = False


# Graceful Shutdown
def sigterm_handler(messaging: Goutong):
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
        "reviews_received": [],
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
        logging.info("Loading state from file...")
        state.update_from_file()

    messaging = Goutong(sender_id=controller_id)

    # Set up queues
    input_queue_name = FILTER_TYPE + str(filter_config.get("FILTER_NUMBER"))

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
    logging.debug("Handling possible pending commit")
    if state.get("reviews_received"):
        # logging.debug(f"ESTADO:{state}")
        to_send = analyze_reviews(state.get("reviews_received"))
        _send_batch(
            messaging=messaging,
            batch=to_send,
            conn_id=state.get("conn_id"),
            transaction_id=state.id_for_next_transaction(),
        )
    if state.get("EOF"):
        _send_EOF(
            messaging=messaging,
            conn_id=state.get("conn_id"),
            transaction_id=state.id_for_next_transaction() + "_EOF",
        )
    state.mark_transaction_committed()


def main_loop(messaging: Goutong, input_queue_name: str, state: ControllerState):
    messaging.set_callback(
        input_queue_name, callback_filter, auto_ack=False, args=(state,)
    )
    logging.debug(f"Escucho queue {input_queue_name}")
    messaging.listen()

    transaction_id = state.id_for_next_transaction()

    if state.get("reviews_received"):
        logging.debug("ANALIZO")
        to_send = analyze_reviews(state.get("reviews_received"))
        _send_batch(
            messaging=messaging,
            batch=to_send,
            conn_id=state.get("conn_id"),
            transaction_id=state.id_for_next_transaction(),
        )
        logging.debug("Mande al SIG")

    if state.get("EOF"):
        _send_EOF(
            messaging,
            state.get("conn_id"),
            transaction_id=state.id_for_next_transaction() + "_EOF",
        )

    state.mark_transaction_committed()


def _send_EOF(messaging: Goutong, conn_id: int, transaction_id: str):
    msg = Message(
        {
            "transaction_id": transaction_id,
            "conn_id": conn_id,
            "queries": [5],
            "EOF": True,
            "forward_to": [OUTPUT_QUEUE],
        }
    )
    messaging.send_to_queue(EOF_QUEUE, msg)
    logging.debug(f"Sent EOF to: {EOF_QUEUE}")


def _analyze_sentiment(text: str):
    blob = TextBlob(text)
    sentiment = blob.sentiment.polarity  # type: ignore
    return sentiment


def callback_filter(messaging: Goutong, msg: Message, state: ControllerState):
    transaction_id = msg.get("transaction_id")
    logging.debug(
        f"RECIBO MENSAJE CON ID {transaction_id} STATE: {state.transactions_received} "
    )
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
    reviews_received = msg.get("data") if msg.has_key("data") else []
    conn_id = msg.get("conn_id")
    queries = msg.get("queries")

    state.set("reviews_received", reviews_received)
    state.set("conn_id", conn_id)
    state.set("queries", queries)
    state.set("EOF", eof)
    state.mark_transaction_received(transaction_id)
    state.save_to_disk()

    # Acknowledge message now that it's saved
    logging.debug(f"HAGO ACK {msg.delivery_id} ")
    messaging.ack_delivery(msg.delivery_id)
    logging.debug(f"no escucho mas queue {msg.queue_name}")
    messaging.stop_consuming(msg.queue_name)


def analyze_reviews(reviews: list):
    analyzed_reviews = []

    for review in reviews:
        review_text = review["review/text"]
        sentiment = _analyze_sentiment(review_text)
        analyzed_reviews.append({"title": review["title"], "sentiment": sentiment})

    logging.debug(f"Analyzed {len(analyzed_reviews)} reviews")
    return analyzed_reviews


def _send_batch(messaging: Goutong, batch: list, conn_id: int, transaction_id: str):
    msg = Message(
        {
            "transaction_id": transaction_id,
            "conn_id": conn_id,
            "queries": [5],
            "data": batch,
        }
    )
    messaging.send_to_queue(OUTPUT_QUEUE, msg)
    logging.debug(f"Sent Data to: {OUTPUT_QUEUE}")


if __name__ == "__main__":
    main()
