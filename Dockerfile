FROM python:3.11-slim
COPY --from=ghcr.io/astral-sh/uv:0.7.6 /uv /uvx /bin/

WORKDIR /app

RUN apt-get update \
 && apt-get install -y --no-install-recommends git ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml pyproject.toml
COPY uv.lock uv.lock
RUN uv sync

COPY ./src/workflows workflows
COPY ./src/utils utils

ENV PATH="/app/.venv/bin:${PATH}"