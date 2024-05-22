FROM python:3.9.7-slim

RUN pip install pika

COPY ./src/controllers/filters/date_filter/main.py ./main.py
COPY ./src/messaging ./messaging
COPY ./src/exceptions ./exceptions
COPY ./src/utils ./utils

ENTRYPOINT ["python", "main.py"]
# ENTRYPOINT ["/bin/sh"]