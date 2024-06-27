FROM python:3.9.7-slim

RUN pip install pika

COPY ./src/controllers/review_joiner/review_joiner.py ./review_joiner.py
COPY ./src/controllers/common/healthcheck_handler.py ./src/controllers/common/healthcheck_handler.py
COPY ./src/messaging ./src/messaging
COPY ./src/exceptions ./src/exceptions
COPY ./src/utils ./src/utils
COPY ./src/controller_state ./src/controller_state

ENTRYPOINT ["python", "review_joiner.py"]
# ENTRYPOINT ["/bin/sh"]