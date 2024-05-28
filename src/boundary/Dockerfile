FROM python:3.9.7-slim

RUN pip install pika

COPY ./src/boundary/boundary.py boundary.py
COPY ./src/messaging ./src/messaging
COPY ./src/utils ./src/utils

# ENTRYPOINT ["/bin/sh"]
ENTRYPOINT python boundary.py
