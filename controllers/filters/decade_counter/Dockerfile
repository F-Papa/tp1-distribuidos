FROM python:3.9.7-slim

RUN pip install pika

COPY ./controllers/filters/decade_counter/main.py ./main.py
COPY ./messaging ./messaging
COPY ./exceptions ./exceptions
COPY ./utils ./utils

ENTRYPOINT ["python", "main.py"]
# ENTRYPOINT ["/bin/sh"]