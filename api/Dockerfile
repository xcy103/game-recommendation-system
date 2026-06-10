FROM python:3.11-slim

WORKDIR /app

# Install only API dependencies (no PySpark/Spark overhead)
COPY pyproject.toml .
RUN pip install --no-cache-dir ".[api]"

COPY api/ ./api/

ENV PORT=8080
EXPOSE 8080

# TODO: add non-root user for security
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8080"]
