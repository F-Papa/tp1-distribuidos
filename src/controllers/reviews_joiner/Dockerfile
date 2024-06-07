FROM python:3.9.7-slim

RUN pip install pika

COPY ./src/controllers/reviews_joiner/reviews_joiner.py ./reviews_joiner.py
COPY ./src/messaging ./src/messaging
COPY ./src/exceptions ./src/exceptions
COPY ./src/utils ./src/utils
COPY ./src/controller_state ./src/controller_state

ENTRYPOINT ["python", "reviews_joiner.py"]
# ENTRYPOINT ["/bin/sh"]