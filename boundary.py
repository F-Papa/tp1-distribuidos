import argparse
import multiprocessing
import logging
import multiprocessing.process

from os import environ
import signal
from exceptions.shutting_down import ShuttingDown
from controllers.io import output_controller, input_controller
from messaging.goutong import Goutong
from messaging.message import Message
from time import sleep 


CONTROL_GROUP = "CONTROL"


def callback_control(messaging: Goutong, msg: Message, shutting_down):
    if msg.has_key("ShutDown"):
        shutting_down.value = True
        raise ShuttingDown


def sigterm_handler(messaging: Goutong, shutting_down):
    logging.info("Main process received SIGTERM. Initiating craceful shutdown.")
    shutting_down.value = 1
    msg = Message({"ShutDown": True})
    # messaging.broadcast_to_group(CONTROL_GROUP, msg)
    raise ShuttingDown


class BoundaryConfig:
    def __init__(self, logging_level: str):
        self.logging_level = logging_level


def get_config_from_env() -> BoundaryConfig:
    if not environ.get("LOGGING_LEVEL"):
        logging.warning("No logging level specified, defaulting to ERROR")

    return BoundaryConfig(logging_level=environ.get("LOGGING_LEVEL", "ERROR"))


def config_logging(boundary_config: BoundaryConfig):
    # Boundary Logging
    level = getattr(logging, boundary_config.logging_level)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Hide pika logs
    pika_logger = logging.getLogger("pika")
    pika_logger.setLevel(logging.ERROR)


def main(book_path: str, reviews_path: str):
    boundary_config = get_config_from_env()
    config_logging(boundary_config)
    shutting_down = multiprocessing.Value("i", 0)

    messaging = Goutong()
    control_queue_name = "boundary_control"
    messaging.add_queues(control_queue_name)
    messaging.set_callback(control_queue_name, callback_control, (shutting_down,))
    messaging.add_broadcast_group(CONTROL_GROUP, [control_queue_name])

    signal.signal(
        signal.SIGTERM,
        lambda sig, frame: sigterm_handler(messaging, shutting_down),
    )

    child_process = multiprocessing.Process(
        target=input_controller.feed_data, args=(book_path, reviews_path, shutting_down)
    )

    for i in range(1, 6):
        with open(f"results/query{i}.txt", "w") as f:
            pass

    # Send data to system
    child_process.start()
    try:
        # Listen for results
        output_controller.display_results(messaging)
    except ShuttingDown:
        pass
    finally:
        child_process.join()
        messaging.close()
        logging.info("Parent process terminated successfully.")


def request_query_number() -> str:
    query_number = None

    while query_number not in ["1", "2", "3", "4", "5"]:
        # Determine which query to run
        print("\n------------------------------\n")
        print("Select a query to run: [1:5]")
        print(
            '1. Título, autores y editoriales de los libros de categoría "Computers"\
            entre 2000 y 2023 que contengan "distributed" en su título.'
        )
        print("2. Autores con títulos publicados en al menos 10 décadas distintas")
        print(
            "3. Títulos y autores de libros publicados en los 90' con al menos 500 reseñas."
        )
        print(
            "4. 10 libros con mejor rating promedio entre aquellos publicados en los 90’ con al menos 500 reseñas."
        )
        print(
            '5. Títulos en categoría "Fiction" cuyo sentimiento de reseña promedio esté enel percentil 90 más alto.'
        )

        query_number = input("Enter the number of the query you want to run:")

        if query_number not in ["1", "2", "3", "4", "5"]:
            print("Elección Incorrecta. Elija un número entre 1 y 5")
    return query_number


if __name__ == "__main__":
    # Boundary Arguments
    parser = argparse.ArgumentParser(description="Amazon Reviews Query System")
    parser.add_argument("--books", type=str, help="Path to books entity", required=True)
    parser.add_argument(
        "--reviews", type=str, help="Path to reviews entity", required=True
    )
    books_path = parser.parse_args().books
    reviews_path = parser.parse_args().reviews

    main(books_path, reviews_path)
