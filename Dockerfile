FROM python:3.11-slim
COPY --from=ghcr.io/astral-sh/uv:0.7.6 /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml pyproject.toml
COPY requirements.txt requirements.txt
RUN uv add -r requirements.txt

COPY ./src/workflows workflows
COPY ./src/weather weather