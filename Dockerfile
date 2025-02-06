FROM python:3.11-slim

RUN pip install --upgrade pip
RUN pip install \
        dagster \
        dagster-postgres \
        dagster-k8s

COPY pipelines /