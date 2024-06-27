FROM python:3.9.7-slim

RUN pip install pika

COPY ./src/controllers/filters/review_counter/review_counter.py ./review_counter.py
COPY ./src/messaging ./src/messaging
COPY ./src/exceptions ./src/exceptions
COPY ./src/utils ./src/utils
COPY ./src/controller_state ./src/controller_state
COPY ./src/controllers/common ./src/controllers/common

ENTRYPOINT ["python", "review_counter.py"]
# ENTRYPOINT ["/bin/sh"]