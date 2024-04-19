from messaging.goutong import Goutong
import csv, json

OUTPUT_QUEUE = 'books_queue'

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
    messaging.add_queues(OUTPUT_QUEUE)
    
    with open(books_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            title = row["Title"]
            date = _parse_year(row["publishedDate"])
            categories = _parse_categories(row["categories"])
            msg = json.dumps({"title": title, "date": date, "categories": categories})
            messaging.send_to_queue(OUTPUT_QUEUE, msg)
        messaging.send_to_queue(OUTPUT_QUEUE, "EOF")

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