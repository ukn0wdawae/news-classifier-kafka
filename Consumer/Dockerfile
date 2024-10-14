FROM python:3.8-slim

# Install apt dependencies
RUN apt update

# Install requirements
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . /consumer

WORKDIR /consumer
ENTRYPOINT python KafkaConsumer.py