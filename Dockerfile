FROM python:3.9.7-slim

RUN pip install pika

COPY ./controllers/io/input_controller.py ./controllers/io/input_controller.py
COPY ./controllers/io/output_controller.py ./controllers/io/output_controller.py
COPY ./messaging ./messaging
COPY ./exceptions ./exceptions
COPY ./boundary.py ./boundary.py

# ENTRYPOINT ["/bin/sh"]
ENTRYPOINT python boundary.py --books books.csv --reviews reviews.csv