FROM python:3.9.7-slim

RUN pip install pika

COPY ./src/controllers/filters/joiner/main.py ./main.py
COPY ./src/messaging ./messaging
COPY ./src/exceptions ./exceptions
COPY ./src/utils ./utils
COPY ./src/data_access/data_access.py ./data_access/data_access.py

ENTRYPOINT ["python", "main.py"]
# ENTRYPOINT ["/bin/sh"]