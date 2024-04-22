from os import environ
from messaging.goutong import Goutong
import json
import logging

INPUT_QUEUE = 'title_filter_queue'
OUTPUT_QUEUE = 'date_filter_queue'

class FilterConfig():
    def __init__(self, title_keyword: str, logging_level: str):
        self.title_keyword = title_keyword
        self.logging_level = logging_level

def get_config_from_env() -> FilterConfig:
    required = ["TITLE_KEYWORD"]

    for key in required:
        if not environ.get(key):
            raise ValueError(f"Missing required environment variable: {key}")

    if not environ.get("LOGGING_LEVEL"):
        logging.warning("No logging level specified, defaulting to ERROR")

    return FilterConfig(
        title_keyword=environ.get("TITLE_KEYWORD", ""),
        logging_level=environ.get("LOGGING_LEVEL", "ERROR")
    )

def config_logging(filter_config: FilterConfig):
    # Filter logging
    level = getattr(logging, filter_config.logging_level)
    logging.basicConfig(level=level, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    
    # Hide pika logs
    pika_logger = logging.getLogger('pika')
    pika_logger.setLevel(logging.ERROR)

def main():
    filter_config = get_config_from_env()
    config_logging(filter_config)   
    logging.info("Filter is up and running!")
    
    messaging = Goutong()
    messaging.add_queues(INPUT_QUEUE, OUTPUT_QUEUE)
    messaging.set_callback(INPUT_QUEUE, callback_filter, (messaging, filter_config))
    messaging.listen()
    
def callback_filter(channel, method, properties, body, messaging: Goutong, config: FilterConfig):
    msg = body.decode()
    
    logging.debug(f"Received: {msg}")
    # Forward EOF and Keep Consuming
    if msg == "EOF":
        messaging.send_to_queue(OUTPUT_QUEUE, "EOF")
        return
    
    book_data = json.loads(msg)
    title = book_data["title"]

    # Filter
    if config.title_keyword in title.lower():
        messaging.send_to_queue(OUTPUT_QUEUE, json.dumps(book_data))
        logging.debug(f"Passed: {book_data}")

if __name__ == "__main__":
    main()