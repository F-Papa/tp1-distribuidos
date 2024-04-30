import signal
from exceptions.shutting_down import ShuttingDown
from messaging.goutong import Goutong
from messaging.message import Message
import csv, os
import logging

DEFAULT_ITEMS_PER_BATCH = 500
CONTROL_GROUP = "CONTROL"
CATEGORY_FILTER = "category_filter_queue"


def sigterm_handler(messaging: Goutong):
    logging.info("Child process received SIGTERM. Initiating Graceful Shutdown.")
    msg = Message({"ShutDown": True})
    # messaging.broadcast_to_group(CONTROL_GROUP, msg)
    raise ShuttingDown


def _parse_year(year_str: str) -> int:
    if year_str == "":
        return 0
    year_str = year_str.replace("*", "")
    year_str = year_str.replace("?", "0")
    if "-" in year_str:
        return int(year_str.split("-")[0])
    else:
        return int(year_str)


def _parse_array(categories_str: str) -> list:
    categories_str = categories_str.replace("['", "")
    categories_str = categories_str.replace("']", "")
    return categories_str.split(" & ")


def _parse_decade(year: int) -> int:
    return year - (year % 10)


def _send_books_batch(messaging: Goutong, batch: list):
    # Queries 1, 3, 4
    queue = "date_filter_queue"
    data = list(map(_book_columns_for_queries1_3_4, batch))
    msg = Message({"query": 1, "data": data})
    messaging.send_to_queue(queue, msg)

    # Query 2
    queue = "decade_counter_queue"
    data = list(map(_book_columns_for_query2, batch))
    msg = Message({"query": 2, "data": data})
    messaging.send_to_queue(queue, msg)

    # Query 5
    queue = "category_filter_queue"
    data = list(map(_book_columns_for_query5, batch))
    msg = Message({"query": 5, "data": data})
    messaging.send_to_queue(queue, msg)

    logging.debug(f"Fed batch of {len(batch)} books.")


def _review_columns_for_queries3_4_5(review_data: dict) -> dict:
    return {
        "title": review_data["Title"],
        "review/score": review_data["review/score"],
        "review/text": review_data["review/text"],
    }


def _send_reviews_batch(messaging: Goutong, batch: list):
    # Queries 3, 4, 5
    queue = "joiner_reviews_queue"
    data = list(map(_review_columns_for_queries3_4_5, batch))
    msg = Message({"query": [3, 4, 5], "data": data})
    messaging.send_to_queue(queue, msg)

    logging.debug(f"Fed batch of {len(batch)} reviews.")


def feed_data(books_path: str, reviews_path: str, shutting_down):
    messaging = Goutong()
    items_per_batch = int(os.environ.get("ITEMS_PER_BATCH", DEFAULT_ITEMS_PER_BATCH))
    signal.signal(signal.SIGTERM, lambda sig, frame: sigterm_handler(messaging))
    try:
        _feed_books(books_path, messaging, items_per_batch, shutting_down)
        _feed_reviews(reviews_path, messaging, items_per_batch, shutting_down)
    except ShuttingDown:
        pass
    messaging.close()


def _book_columns_for_queries1_3_4(book_data: dict) -> dict:
    return {
        "title": book_data["Title"],
        "year": _parse_year(book_data["publishedDate"]),
        "categories": _parse_array(book_data["categories"]),
        "authors": _parse_array(book_data["authors"]),
        "publisher": book_data["publisher"],
    }


def _book_columns_for_query2(book_data: dict) -> dict:
    return {
        "decade": _parse_decade(_parse_year(book_data["publishedDate"])),
        "authors": _parse_array(book_data["authors"]),
    }


def _book_columns_for_query5(book_data: dict) -> dict:
    return {
        "title": book_data["Title"],
        "categories": _parse_array(book_data["categories"]),
    }


def _feed_books(
    books_path: str, messaging: Goutong, items_per_batch: int, shutting_down
):
    with open(books_path, newline="") as csvfile:
        batch = []
        reader = csv.DictReader(csvfile)

        for row in reader:
            if len(batch) < items_per_batch:
                batch.append(row)
            else:
                if shutting_down.value == 1:
                    break
                _send_books_batch(messaging, batch)
                batch = []
        if not shutting_down.value:
            if len(batch) > 0:
                _send_books_batch(messaging, batch)
        if not shutting_down.value:
            messaging.send_to_queue("date_filter_queue", Message({"EOF": True}))
            messaging.send_to_queue("decade_counter_queue", Message({"EOF": True}))
            messaging.send_to_queue("category_filter_queue", Message({"EOF": True}))


def _feed_reviews(
    reviews_path: str, messaging: Goutong, items_per_batch: int, shutting_down
):
    with open(reviews_path, newline="") as csvfile:
        batch = []
        reader = csv.DictReader(csvfile)

        for row in reader:
            if len(batch) < items_per_batch:
                batch.append(row)
            else:
                if shutting_down.value == 1:
                    break
                _send_reviews_batch(messaging, batch)
                batch = []
        if not shutting_down.value:
            if len(batch) > 0:
                _send_reviews_batch(messaging, batch)
        if not shutting_down.value:
            messaging.send_to_queue("joiner_reviews_queue", Message({"EOF": True}))
