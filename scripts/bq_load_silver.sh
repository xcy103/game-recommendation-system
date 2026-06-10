#!/usr/bin/env bash
# scripts/bq_load_silver.sh
#
# Load silver Parquet files for a given dt into BigQuery stg_reviews.
#
# The table is pre-created by Terraform with time_partitioning.field="dt" (DATE).
# Because `dt` is stored as a real DATE column inside the Parquet files (NOT as
# a Hive directory partition), bq load reads it natively — no hive-partitioning
# flags needed.
#
# COST:
#   bq load is FREE for data loaded from GCS (slot usage billed on-demand only
#   if the project uses on-demand pricing, which is $0 for flat-rate).
#   Storage billed at standard BQ rates per GB per month.
#
# Usage:
#   DT=2024-06-10 bash scripts/bq_load_silver.sh
set -euo pipefail

# ── Config (override via env vars) ──────────────────────────────────────────
PROJECT_ID="${PROJECT_ID:-steam-reviews-platform}"
REGION="${REGION:-us-central1}"
ENVIRONMENT="${ENVIRONMENT:-dev}"
DT="${DT:-$(date +%Y-%m-%d)}"

SILVER_BUCKET="gs://${PROJECT_ID}-silver-${ENVIRONMENT}"
DATASET="steam_staging_${ENVIRONMENT}"
TABLE="stg_reviews"

# BQ partition decorator: load into the specific day partition.
# This is faster and allows targeted re-loads without touching other partitions.
PARTITION="${DT//-/}"    # 2024-06-10 → 20240610
TARGET="${PROJECT_ID}:${DATASET}.${TABLE}\$${PARTITION}"
SOURCE="${SILVER_BUCKET}/reviews/dt=${DT}/*.parquet"

echo "================================================================="
echo " BigQuery load — silver → stg_reviews"
echo "   dt          : ${DT}"
echo "   source      : ${SOURCE}"
echo "   target      : ${TARGET}"
echo "================================================================="

echo "[1/2] Checking silver files exist..."
FILE_COUNT=$(gcloud storage ls "${SILVER_BUCKET}/reviews/dt=${DT}/*.parquet" 2>/dev/null | wc -l | tr -d ' ')
if [[ "${FILE_COUNT}" -eq 0 ]]; then
    echo "ERROR: No Parquet files found at ${SILVER_BUCKET}/reviews/dt=${DT}/"
    echo "       Run dataproc_submit.sh first."
    exit 1
fi
echo "      -> Found ${FILE_COUNT} Parquet file(s)"

echo "[2/2] Running bq load..."
bq load \
    --project_id="${PROJECT_ID}" \
    --location="${REGION}" \
    --source_format=PARQUET \
    --replace=true \
    --schema_update_option=ALLOW_FIELD_RELAXATION \
    "${TARGET}" \
    "${SOURCE}"

echo ""
echo "Load complete."
echo ""
echo "Verify with:"
echo "  bq query --project_id=${PROJECT_ID} \\"
echo "    \"SELECT COUNT(*) AS n, DATE(dt) AS dt FROM ${DATASET}.${TABLE} WHERE dt = '${DT}' GROUP BY dt\""
