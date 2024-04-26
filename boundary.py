import argparse
import multiprocessing
import logging
import multiprocessing.process

from os import environ
from controllers.io import output_controller, input_controller


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
    # Boundary Logging
    level = getattr(logging, boundary_config.logging_level)
    logging.basicConfig(level=level, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
   
    # Hide pika logs
    pika_logger = logging.getLogger('pika')
    pika_logger.setLevel(logging.ERROR)

def main(book_path: str, reviews_path: str, query_id: str):
    boundary_config = get_config_from_env()
    config_logging(boundary_config)

    # segun la query determinar qué funcion y argumentos corresponden a su controller
    if query_id == "1":
        target = input_controller.distributed_computer_books
        args = (book_path,)
    elif query_id == "2":
        target = input_controller.query2
        args = ()
    elif query_id == "3":
        target = input_controller.query3
        args = ()
    elif query_id == "4":
        target = input_controller.query4
        args = ()
    else:
        target = input_controller.query5
        args = ()

    child_process = multiprocessing.Process(target=target, args=args)
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
    print("\n------------------------------\n")
    print("Select a query to run: [1:5]")
    print('1. Título, autores y editoriales de los libros de categoría "Computers"\
           entre 2000 y 2023 que contengan "distributed" en su título.')
    print('2. Autores con títulos publicados en al menos 10 décadas distintas')
    print("3. Títulos y autores de libros publicados en los 90' con al menos 500 reseñas.")
    print("4. 10 libros con mejor rating promedio entre aquellos publicados en los 90’ con al menos 500 reseñas.")
    print('5. Títulos en categoría "Fiction" cuyo sentimiento de reseña promedio esté enel percentil 90 más alto.')

    query_choice = input("Enter the number of the query you want to run:")

    if query_choice in ["1", "2", "3", "4", "5"]:
        main(books_path, reviews_path, query_choice)
    else:
        print("Elección Incorrecta. Elija un número entre 1 y 5")

    


    
    

    