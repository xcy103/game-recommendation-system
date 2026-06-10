#!/usr/bin/env bash
# local_run.sh — run the full pipeline end-to-end on local sample data
# DESIGN.md §12: "run pipeline end-to-end locally on sample data"
#
# Usage:
#   ./scripts/local_run.sh [--dt YYYY-MM-DD]
#
# Prerequisites:
#   - Python 3.11+ with dev deps: pip install -e ".[dev]"
#   - Java 11+ on PATH (for local PySpark)
#   - gcloud authenticated: gcloud auth application-default login
#
# What this script does:
#   1. Ingest fixture data into local /tmp/bronze/ (no real API calls)
#   2. Run PySpark transform_silver.py on that data → /tmp/silver/
#   3. Load silver Parquet into BigQuery staging (requires GCP creds)
#   4. Run dbt to build gold star schema
#   5. Train ALS recommender → write game_recommendations to BQ
#
# Each step is gated so you can run partial pipeline for development.

set -euo pipefail

DT="${1:-$(date +%Y-%m-%d)}"
BRONZE_LOCAL="/tmp/steam-bronze"
SILVER_LOCAL="/tmp/steam-silver"

echo "=== Steam Reviews Platform — Local Run ==="
echo "Date partition: ${DT}"
echo ""

# ---------------------------------------------------------------------------
# Step 1: Ingestion
# ---------------------------------------------------------------------------
echo "[1/5] Ingestion → ${BRONZE_LOCAL}"
# TODO:
# python -m ingestion.ingester \
#   --dt "${DT}" \
#   --appids-file scripts/sample_appids.txt \
#   --use-fixtures \
#   --output-dir "${BRONZE_LOCAL}"
echo "      TODO — skipping"

# ---------------------------------------------------------------------------
# Step 2: Spark bronze → silver
# ---------------------------------------------------------------------------
echo "[2/5] PySpark transform → ${SILVER_LOCAL}"
# TODO:
# spark-submit spark/transform_silver.py \
#   --dt "${DT}" \
#   --bronze-path "${BRONZE_LOCAL}" \
#   --silver-path "${SILVER_LOCAL}"
echo "      TODO — skipping"

# ---------------------------------------------------------------------------
# Step 3: Load silver → BQ staging
# ---------------------------------------------------------------------------
echo "[3/5] Load Parquet → BigQuery steam_staging_dev.stg_reviews"
# TODO:
# bq load --source_format=PARQUET \
#   --time_partitioning_field=dt \
#   steam-reviews-platform:steam_staging_dev.stg_reviews \
#   "${SILVER_LOCAL}/reviews/dt=${DT}/*.parquet"
echo "      TODO — skipping"

# ---------------------------------------------------------------------------
# Step 4: dbt gold build
# ---------------------------------------------------------------------------
echo "[4/5] dbt run + test"
# TODO:
# cd dbt && dbt run --profiles-dir . && dbt test --profiles-dir .
echo "      TODO — skipping"

# ---------------------------------------------------------------------------
# Step 5: ALS recommender
# ---------------------------------------------------------------------------
echo "[5/5] Train ALS → game_recommendations"
# TODO:
# spark-submit spark/recommender.py \
#   --dt "${DT}" \
#   --silver-bucket steam-reviews-platform-silver-dev \
#   --gold-dataset steam_gold_dev \
#   --project steam-reviews-platform
echo "      TODO — skipping"

echo ""
echo "=== Local run complete (scaffold — see TODO comments above) ==="
