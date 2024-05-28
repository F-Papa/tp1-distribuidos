FROM python:3.9.7-slim

RUN pip install pika

COPY ./src/controllers/io/input/input_controller.py input_controller.py
COPY ./src/messaging ./src/messaging
COPY ./src/exceptions ./src/exceptions

COPY ./src/utils ./src/utils

# ENTRYPOINT ["/bin/sh"]
ENTRYPOINT python input_controller.py
