FROM python:3.11-slim

WORKDIR /app

# Install ingestion dependencies only (no Spark, no API)
COPY pyproject.toml .
RUN pip install --no-cache-dir ".[ingestion]"

COPY ingestion/ ./ingestion/

# TODO: pass --dt, --bronze-bucket via env or entrypoint args
# TODO: add non-root user + ENTRYPOINT once ingester.py is implemented

CMD ["python", "-m", "ingestion.ingester"]
