FROM python:3.11-slim
COPY --from=ghcr.io/astral-sh/uv:0.7.6 /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml pyproject.toml
COPY uv.lock uv.lock
RUN uv sync

COPY ./src/workflows workflows
COPY ./src/weather weather

ENV PATH="/app/.venv/bin:${PATH}"