FROM python:3.12

WORKDIR /app
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
COPY pyproject.toml ./
COPY uv.lock ./
RUN uv sync --frozen --no-cache --no-dev
COPY ingest.py ingest.py
ENTRYPOINT ["uv", "run", "--no-dev", "ingest.py"]
