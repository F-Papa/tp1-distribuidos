FROM python:3.9.7-slim

RUN pip install pika
RUN pip install -U textblob
RUN python -m textblob.download_corpora

COPY ./src/controllers/filters/sentiment_analyzer/main.py ./main.py
COPY ./src/controllers/common/healthcheck_handler.py ./src/controllers/common/healthcheck_handler.py
COPY ./src/messaging ./src/messaging
COPY ./src/exceptions ./src/exceptions
COPY ./src/utils ./src/utils
COPY ./src/controller_state ./src/controller_state


ENTRYPOINT ["python", "main.py"]
# ENTRYPOINT ["/bin/sh"]