FROM python:3.9.7-slim

RUN pip install pika

COPY ./controllers/filters/date_filter/main.py ./main.py
COPY ./messaging ./messaging
COPY ./utils ./utils

ENTRYPOINT ["python", "main.py"]
# ENTRYPOINT ["/bin/sh"]