FROM python:3.9.7-slim

RUN pip install pika

COPY ./src/controllers/filters/category_filter/main.py ./main.py
COPY ./src/controllers/common/healthcheck_handler.py ./src/controllers/common/healthcheck_handler.py
COPY ./src/messaging ./src/messaging
COPY ./src/exceptions ./src/exceptions
COPY ./src/utils ./src/utils
COPY ./src/controller_state ./src/controller_state


ENTRYPOINT ["python", "main.py"]
# ENTRYPOINT ["/bin/sh"]