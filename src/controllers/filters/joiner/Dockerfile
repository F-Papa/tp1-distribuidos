FROM python:3.9.7-slim

RUN pip install pika

COPY ./src/controllers/filters/joiner/main.py ./main.py
COPY ./src/messaging ./src/messaging
COPY ./src/exceptions ./src/exceptions
COPY ./src/utils ./src/utils
COPY ./src/data_store/data_store.py ./src/data_store/data_store.py

ENTRYPOINT ["python", "main.py"]
# ENTRYPOINT ["/bin/sh"]