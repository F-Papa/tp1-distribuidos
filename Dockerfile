FROM python:3.9.7-slim

RUN pip install pika

COPY ./src/controllers/io/input_controller.py ./controllers/io/input_controller.py
COPY ./src/controllers/io/output_controller.py ./controllers/io/output_controller.py
COPY ./src/messaging ./messaging
COPY ./src/exceptions ./exceptions
COPY ./boundary.py ./boundary.py

# ENTRYPOINT ["/bin/sh"]
ENTRYPOINT python boundary.py --books books.csv --reviews reviews.csv
