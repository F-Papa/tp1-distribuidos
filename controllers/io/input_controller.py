from messaging.goutong import Goutong
from messaging.message import Message
import csv, os
import logging

OUTPUT_QUEUE = "title_filter_queue"
DEFAULT_ITEMS_PER_BATCH = 50


def _parse_year(year_str: str) -> int:
    if year_str == "":
        return 0
    year_str = year_str.replace("*", "")
    year_str = year_str.replace("?", "0")
    if "-" in year_str:
        return int(year_str.split("-")[0])
    else:
        return int(year_str)


def _parse_categories(categories_str: str) -> list:
    categories_str = categories_str.replace("['", "")
    categories_str = categories_str.replace("']", "")
    return categories_str.split(" & ")


def _send_batch(messaging: Goutong, batch: list, route: list):
    msg_content = {"data": batch, "route": route}
    msg = Message(msg_content)
    messaging.send_to_queue(OUTPUT_QUEUE, msg)
    logging.debug(f"Passed: {msg.marshal()}")


def _declare_queues(messaging: Goutong):
    messaging.add_queues("title_filter_queue")
    messaging.add_queues("title_filter1")
    messaging.add_queues("title_filter2")
    messaging.add_queues("title_filter3")
    messaging.add_queues("title_filter_eof")

    messaging.add_queues("date_filter_queue")
    messaging.add_queues("date_filter1")
    messaging.add_queues("date_filter2")
    messaging.add_queues("date_filter3")
    messaging.add_queues("date_filter_eof")

    messaging.add_queues("category_filter_queue")
    messaging.add_queues("category_filter1")
    messaging.add_queues("category_filter2")
    messaging.add_queues("category_filter3")
    messaging.add_queues("category_filter_eof")


# Query1
def distributed_computer_books(books_path: str):
    # Messaging Middleware
    items_per_batch = int(os.environ.get("ITEMS_PER_BATCH", DEFAULT_ITEMS_PER_BATCH))
    messaging = Goutong()
    messaging.add_queues(OUTPUT_QUEUE)
    _declare_queues(messaging)
    route = [
        "title_filter_queue",
        "date_filter_queue",
        "category_filter_queue",
        "results_queue",
    ]

    with open(books_path, newline="") as csvfile:
        batch = []
        reader = csv.DictReader(csvfile)

        for row in reader:
            if len(batch) < items_per_batch:
                title = row["Title"]
                year = _parse_year(row["publishedDate"])
                categories = _parse_categories(row["categories"])

                batch.append({"title": title, "year": year, "categories": categories})
            else:
                _send_batch(messaging, batch, route)
                batch = []
        if len(batch) > 0:
            _send_batch(messaging, batch, route)

        messaging.send_to_queue(OUTPUT_QUEUE, Message({"EOF": True, "route": route}))


# Query2
def query2():
    pass


# Query3
def query3():
    pass


# Query4
def query4():
    pass


# Query5
def query5():
    pass
