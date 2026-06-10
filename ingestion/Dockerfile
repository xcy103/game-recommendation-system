FROM python:3.11-slim

WORKDIR /app

# Install dependencies before copying source for better layer caching
COPY pyproject.toml .
COPY ingestion/ ./ingestion/
RUN pip install --no-cache-dir ".[ingestion]"

# Non-root user
RUN useradd -r -u 1001 -g root ingester && chown -R ingester:root /app
USER ingester

# Args passed at runtime by Airflow BashOperator:
#   --dt YYYY-MM-DD --project PROJECT_ID --environment dev
ENTRYPOINT ["python", "-m", "ingestion.ingester"]
