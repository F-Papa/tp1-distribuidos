from messaging.goutong import Goutong
from messaging.message import Message
import csv, json

TITLE_FILTER_QUEUE = 'title_filter_queue'

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

# Query1
def distributed_computer_books(books_path: str):
    # Messaging Middleware
    messaging = Goutong()
    messaging.add_queues(TITLE_FILTER_QUEUE)
    
    with open(books_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            title = row["Title"]
            year = _parse_year(row["publishedDate"])
            categories = _parse_categories(row["categories"])
            book_data = {"title": title, "year": year, "categories": categories}
            msg_content = {"data": book_data}
            msg = Message(msg_content)
            messaging.send_to_queue(TITLE_FILTER_QUEUE, msg)
        
        messaging.send_to_queue(TITLE_FILTER_QUEUE, Message({"EOF":True}))	

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