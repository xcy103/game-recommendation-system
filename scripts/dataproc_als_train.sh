#!/usr/bin/env bash
# scripts/dataproc_als_train.sh
#
# Three-step ALS training pipeline:
#   1. bq extract  dim_users → GCS Parquet  (user credentials)
#   2. Dataproc    spark/recommender.py (ALS train → GCS Parquet output)
#   3. bq load     GCS Parquet → game_recommendations  (user credentials)
#
# Why bq extract/load instead of the BigQuery Spark connector:
#   The Spark SA has access to GCS (silver + staging) but NOT to steam_gold_dev
#   in BigQuery. Rather than Terraform-applying new IAM now, we use the user's
#   own gcloud credentials (which have project-owner access) for the BQ steps.
#   The Terraform IAM fix is already committed in terraform/modules/iam/main.tf
#   and will take effect on the next `terraform apply`.
#
# COST ESTIMATE (271 K users × 101 games × 290 K interactions):
#   ALS rank=50, maxIter=10 on Dataproc Serverless default (4 vCPUs / 16 GB)
#   Estimated wall time : ~12–18 minutes
#   vCPU cost           : 4 vCPUs × 0.25 h × $0.048/vCPU-h  ≈ $0.05
#   Memory cost         : 16 GB  × 0.25 h × $0.006/GB-h     ≈ $0.02
#   Total estimate      : ~$0.07–$0.15
#
# Prerequisites:
#   - Dataproc Terraform module applied (staging bucket, IAM)
#   - dbt models run (dim_users must exist in BigQuery)
#   - gcloud auth application-default login
#
# Usage:
#   DT=2026-06-10 bash scripts/dataproc_als_train.sh
set -euo pipefail

# ── Config ────────────────────────────────────────────────────────────────────
PROJECT_ID="${PROJECT_ID:-steam-reviews-platform}"
REGION="${REGION:-us-central1}"
ENVIRONMENT="${ENVIRONMENT:-dev}"
DT="${DT:-$(date +%Y-%m-%d)}"
TOP_N="${TOP_N:-10}"
GOLD_DATASET="${GOLD_DATASET:-steam_gold_dev}"

SILVER_BUCKET="${PROJECT_ID}-silver-${ENVIRONMENT}"
STAGING_BUCKET_NAME="${PROJECT_ID}-dataproc-staging-${ENVIRONMENT}"
STAGING_BUCKET="gs://${STAGING_BUCKET_NAME}"
SPARK_SA="steam-spark-${ENVIRONMENT}@${PROJECT_ID}.iam.gserviceaccount.com"

DIM_USERS_GCS="${STAGING_BUCKET}/dim_users/${DT}/"
RECS_GCS="${STAGING_BUCKET}/recommendations/${DT}/"

BATCH_ID="als-train-${DT//\-/}-$(date +%H%M%S)"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "================================================================="
echo " ALS training pipeline"
echo "   dt             : ${DT}"
echo "   silver bucket  : gs://${SILVER_BUCKET}"
echo "   staging        : ${STAGING_BUCKET}"
echo "   gold dataset   : ${GOLD_DATASET}"
echo "   top-N          : ${TOP_N}"
echo "   batch id       : ${BATCH_ID}"
echo "================================================================="
echo ""
echo "  Estimated cost  : \$0.07 – \$0.15"
echo "  Estimated time  : 12 – 18 minutes"
echo ""

# ── Step 1: bq extract dim_users → GCS ─────────────────────────────────────
echo "[1/4] Exporting dim_users from BigQuery to GCS..."
bq extract \
    --destination_format=PARQUET \
    --compression=SNAPPY \
    "${PROJECT_ID}:${GOLD_DATASET}.dim_users" \
    "${DIM_USERS_GCS}dim_users_*.parquet"
echo "      -> exported to ${DIM_USERS_GCS}"

# ── Step 2: Upload recommender.py ───────────────────────────────────────────
echo "[2/4] Uploading spark/recommender.py to ${STAGING_BUCKET}/jobs/als/${DT}/..."
cd "${REPO_ROOT}"
gcloud storage cp spark/recommender.py \
    "${STAGING_BUCKET}/jobs/als/${DT}/recommender.py" \
    --quiet
echo "      -> uploaded"

# ── Step 3: Submit Dataproc Serverless batch ─────────────────────────────────
echo "[3/4] Submitting batch ${BATCH_ID}..."
gcloud dataproc batches submit pyspark \
    "${STAGING_BUCKET}/jobs/als/${DT}/recommender.py" \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --batch="${BATCH_ID}" \
    --service-account="${SPARK_SA}" \
    --staging-bucket="${STAGING_BUCKET_NAME}" \
    --properties="spark.sql.shuffle.partitions=8,spark.executor.memory=8g" \
    -- \
    --dt="${DT}" \
    --silver-bucket="${SILVER_BUCKET}" \
    --staging-bucket="${STAGING_BUCKET_NAME}" \
    --dim-users-path="${DIM_USERS_GCS}" \
    --top-n="${TOP_N}"

echo "      -> Dataproc batch complete"

# ── Step 4: bq load GCS → game_recommendations ──────────────────────────────
echo "[4/4] Loading recommendations from GCS to BigQuery..."
bq load \
    --source_format=PARQUET \
    --replace=true \
    "${PROJECT_ID}:${GOLD_DATASET}.game_recommendations" \
    "${RECS_GCS}*.parquet"
echo "      -> loaded to ${PROJECT_ID}:${GOLD_DATASET}.game_recommendations"

echo ""
echo "Done. Verify with:"
echo "  bq query --nouse_legacy_sql \\"
echo "    'SELECT COUNT(*) AS rows, COUNT(DISTINCT user_sk) AS users"
echo "     FROM \`${PROJECT_ID}.${GOLD_DATASET}.game_recommendations\`'"
