FROM python:3.9.7-slim

RUN pip install pika

COPY ./src/controllers/proxy_barrier/main.py ./main.py
COPY ./src/messaging ./src/messaging
COPY ./src/exceptions ./src/exceptions
COPY ./src/utils ./src/utils

ENTRYPOINT ["python", "main.py"]
# ENTRYPOINT ["/bin/sh"]