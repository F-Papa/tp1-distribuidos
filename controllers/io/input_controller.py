import signal
from exceptions.shutting_down import ShuttingDown
from messaging.goutong import Goutong
from messaging.message import Message
import csv, os
import logging

DEFAULT_ITEMS_PER_BATCH = 50
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


def _send_batch(messaging: Goutong, batch: list):
    # Queries 1, 3, 4
    queue = "date_filter_queue"
    data = list(map(_columns_for_queries1_3_4, batch))
    msg = Message({"data": data})
    messaging.send_to_queue(queue, msg)

    # Query 2
    # queue = "authors_decades_queue"
    # data = list(map(_columns_for_query2, batch))
    # msg = Message({"data": data})
    # messaging.send_to_queue(queue, msg)

    # Query 5
    # queue = "category_filter_queue"
    # data = list(map(_columns_for_query5, batch))
    # msg = Message({"data": data})
    # messaging.send_to_queue(queue, msg)

    logging.debug(f"Fed batch of {len(batch)} items.")


def _declare_queues(messaging: Goutong):
    messaging.add_queues("title_filter_queue")
    messaging.add_queues("title_filter_eof")

    messaging.add_queues("date_filter_queue")
    messaging.add_queues("date_filter_eof")

    messaging.add_queues("category_filter_queue")
    messaging.add_queues("category_filter_eof")


def feed_data(books_path: str, reviews_path: str, shutting_down):
    messaging = Goutong()
    items_per_batch = int(os.environ.get("ITEMS_PER_BATCH", DEFAULT_ITEMS_PER_BATCH))
    signal.signal(signal.SIGTERM, lambda sig, frame: sigterm_handler(messaging))
    try:
        _feed_data_aux(books_path, messaging, items_per_batch, shutting_down)
    except ShuttingDown:
        pass
    messaging.close()


def _columns_for_queries1_3_4(book_data: dict) -> dict:
    return {
        "title": book_data["Title"],
        "year": _parse_year(book_data["publishedDate"]),
        "categories": _parse_array(book_data["categories"]),
        "authors": _parse_array(book_data["authors"]),
    }


def _columns_for_query2(book_data: dict) -> dict:
    return {
        "decade": _parse_decade(_parse_year(book_data["publishedDate"])),
        "authors": _parse_array(book_data["authors"]),
    }


def _columns_for_query5(book_data: dict) -> dict:
    return {
        "title": book_data["Title"],
        "categories": _parse_array(book_data["categories"]),
    }


def _feed_data_aux(
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
                _send_batch(messaging, batch)
                batch = []
        if not shutting_down.value:
            if len(batch) > 0:
                _send_batch(messaging, batch)
        if not shutting_down.value:
            messaging.send_to_queue(CATEGORY_FILTER, Message({"EOF": True}))


"""
# # Query1
# def distributed_computer_books(books_path: str, shutting_down):
#     messaging = Goutong()
#     signal.signal(
#         signal.SIGTERM,
#         lambda sig, frame: sigterm_handler(
#             messaging,
#         ),
#     )
#     logging.info(
#         "Executing query: Distributed Computer Books Between Years 2000 and 2023"
#     )
#     try:
#         distributed_computer_books_aux(books_path, messaging, shutting_down)
#     except ShuttingDown:
#         pass
#     finally:
#         messaging.close()
#         logging.info("Child process terminated successfully")


# def distributed_computer_books_aux(books_path: str, messaging: Goutong, shutting_down):
#     if shutting_down.value == 0:
#         # Messaging Middleware
#         items_per_batch = int(
#             os.environ.get("ITEMS_PER_BATCH", DEFAULT_ITEMS_PER_BATCH)
#         )

#         messaging.add_queues(CATEGORY_FILTER)
#         _declare_queues(messaging)

#     if shutting_down.value == 0:
#         with open(books_path, newline="") as csvfile:
#             batch = []
#             reader = csv.DictReader(csvfile)

#             for row in reader:
#                 if len(batch) < items_per_batch:
#                     title = row["Title"]
#                     year = _parse_year(row["publishedDate"])
#                     categories = _parse_array(row["categories"])

#                     batch.append(
#                         {"title": title, "year": year, "categories": categories}
#                     )
#                 else:
#                     if shutting_down.value == 1:
#                         break
#                     _send_batch(messaging, batch, CATEGORY_FILTER)
#                     batch = []
#             if not shutting_down.value:
#                 if len(batch) > 0:
#                     _send_batch(messaging, batch, CATEGORY_FILTER)
#             if not shutting_down.value:
#                 messaging.send_to_queue(CATEGORY_FILTER, Message({"EOF": True}))
"""