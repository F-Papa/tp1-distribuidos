FROM python:3.9.7-slim

RUN pip install pika

COPY ./controllers/filters/category_filter/main.py ./main.py
COPY ./messaging ./messaging

ENTRYPOINT ["python", "main.py"]
# ENTRYPOINT ["/bin/sh"]