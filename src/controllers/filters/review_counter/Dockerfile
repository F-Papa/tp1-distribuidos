FROM python:3.9.7-slim

RUN pip install pika

COPY ./src/controllers/filters/review_counter/main.py ./main.py
COPY ./src/messaging ./src/messaging
COPY ./src/exceptions ./src/exceptions
COPY ./src/utils ./src/utils
COPY ./src/data_access/data_access.py ./src/data_access/data_access.py
COPY ./src/controller_state ./src/controller_state

ENTRYPOINT ["python", "main.py"]
# ENTRYPOINT ["/bin/sh"]