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


def _send_batch(messaging: Goutong, batch: list):
    msg_content = {"data": batch}
    msg = Message(msg_content)
    messaging.send_to_queue(OUTPUT_QUEUE, msg)
    logging.debug(f"Passed: {msg.marshal()}")


# Query1
def distributed_computer_books(books_path: str):
    # Messaging Middleware
    items_per_batch = int(os.environ.get("ITEMS_PER_BATCH", DEFAULT_ITEMS_PER_BATCH))
    messaging = Goutong()
    messaging.add_queues(OUTPUT_QUEUE)

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
                _send_batch(messaging, batch)
                batch = []
        if len(batch) > 0:
            _send_batch(messaging, batch)

        messaging.send_to_queue(OUTPUT_QUEUE, Message({"EOF": True}))


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
