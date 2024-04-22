FROM python:3.9.7-slim


COPY ./controllers/filters/title_filter/main.py ./main.py
COPY ./messaging/goutong.py ./messaging/goutong.py
RUN pip install pika

ENTRYPOINT ["python", "main.py"]
# ENTRYPOINT ["/bin/sh"]