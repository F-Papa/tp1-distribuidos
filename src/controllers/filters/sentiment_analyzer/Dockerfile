FROM python:3.9.7-slim

RUN pip install pika
RUN pip install -U textblob
RUN python -m textblob.download_corpora

COPY ./src/controllers/filters/sentiment_analyzer/main.py ./main.py
COPY ./src/messaging ./messaging
COPY ./src/exceptions ./exceptions
COPY ./src/utils ./utils

ENTRYPOINT ["python", "main.py"]
# ENTRYPOINT ["/bin/sh"]