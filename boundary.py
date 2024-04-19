import argparse
import multiprocessing
import logging
import multiprocessing.process

from os import environ
from controllers.io import output_controller, input_controller

OUTPUT_QUEUE = 'books_queue'

class BoundaryConfig():
    def __init__(self, logging_level: str):
        self.logging_level = logging_level

def get_config_from_env() -> BoundaryConfig:
    if not environ.get("LOGGING_LEVEL"):
        logging.warning("No logging level specified, defaulting to ERROR")

    return BoundaryConfig(
        logging_level=environ.get("LOGGING_LEVEL", "ERROR")
    )

def config_logging(boundary_config: BoundaryConfig):
    # Boundary Loggign
    level = getattr(logging, boundary_config.logging_level)
    logging.basicConfig(level=level, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
   
    # Hide pika logs
    pika_logger = logging.getLogger('pika')
    pika_logger.setLevel(logging.ERROR)

def main(book_path: str, reviews_path: str):
    boundary_config = get_config_from_env()
    config_logging(boundary_config)

    child_process = multiprocessing.Process(target=input_controller.distributed_computer_books, args=(book_path,))
    child_process.start()
    
    # Listen for results
    output_controller.display_results()
    child_process.join()


if __name__ == '__main__':
    #Boundary Arguments
    parser = argparse.ArgumentParser(description='Amazon Reviews Query System')
    parser.add_argument('--books', type=str, help='Path to books entity', required=True)
    parser.add_argument('--reviews', type=str, help='Path to reviews entity', required=True)
    books_path = parser.parse_args().books
    reviews_path = parser.parse_args().reviews

    # Determine which query to run
    # TODO!

    main(books_path, reviews_path)
    

    