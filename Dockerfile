FROM python:3.9.7-slim

RUN pip install pika

COPY ./controllers/io/input_controller.py ./controllers/io/input_controller.py
COPY ./controllers/io/output_controller.py ./controllers/io/output_controller.py
COPY ./messaging/goutong.py ./messaging/goutong.py
COPY ./boundary.py ./main.py

# ENTRYPOINT ["/bin/sh"]
ENTRYPOINT python main.py --books books.csv --reviews reviews.csv