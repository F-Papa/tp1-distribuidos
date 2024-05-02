FROM python:3.9.7-slim

RUN pip install pika

COPY ./controllers/filters/review_counter/main.py ./main.py
COPY ./messaging ./messaging
COPY ./exceptions ./exceptions
COPY ./utils ./utils
COPY ./data_access/data_access.py ./data_access/data_access.py

ENTRYPOINT ["python", "main.py"]
# ENTRYPOINT ["/bin/sh"]