FROM python:3.9.7-slim

RUN pip install pika

COPY ./controllers/proxy_barrier/main.py ./main.py
COPY ./messaging ./messaging
COPY ./exceptions ./exceptions
COPY ./utils ./utils

ENTRYPOINT ["python", "main.py"]
# ENTRYPOINT ["/bin/sh"]