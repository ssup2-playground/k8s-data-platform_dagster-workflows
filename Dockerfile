FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY ./src/workflows workflows
COPY ./src/weather weather