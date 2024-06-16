FROM python:3.9.7-slim

COPY ./src/controllers/medic/medic.py ./medic.py
COPY ./src/utils/config_loader.py ./src/utils/config_loader.py

RUN pip install docker
RUN apt-get update && apt-get install -y docker.io

ENTRYPOINT ["python", "medic.py"] 