#!/usr/bin/env bash
# scripts/dataproc_submit.sh
#
# Submit spark/transform_silver.py to Dataproc Serverless batch.
# Packages the spark/ module as a zip so `from spark.sentiment import ...`
# resolves correctly in the remote Python environment.
#
# COST GUARD (DESIGN §15):
#   Dataproc Serverless bills per vCPU-second while the batch runs.
#   Billing stops automatically on job completion — zero idle charges.
#   Estimated cost:
#     smoke test (~1 500 rows, 2 vCPUs ~30s) : < $0.005
#     full run   (~10 M rows,  2 vCPUs ~5min): ~ $0.50–$1.00
#
# Prerequisites:
#   - terraform apply with enable_dataproc=true (staging bucket + IAM)
#   - gcloud auth application-default login
#   - pip install vaderSentiment pyarrow (installed at runtime on Dataproc)
#
# Usage:
#   DT=2024-06-10 bash scripts/dataproc_submit.sh
#   DT=2024-06-10 MAX_PAGES=5 bash scripts/dataproc_submit.sh   # smoke test
set -euo pipefail

# ── Config (override via env vars) ──────────────────────────────────────────
PROJECT_ID="${PROJECT_ID:-steam-reviews-platform}"
REGION="${REGION:-us-central1}"
ENVIRONMENT="${ENVIRONMENT:-dev}"
DT="${DT:-$(date +%Y-%m-%d)}"

BRONZE_BUCKET="gs://${PROJECT_ID}-bronze-${ENVIRONMENT}"
SILVER_BUCKET="gs://${PROJECT_ID}-silver-${ENVIRONMENT}"
STAGING_BUCKET_NAME="${PROJECT_ID}-dataproc-staging-${ENVIRONMENT}"
STAGING_BUCKET="gs://${STAGING_BUCKET_NAME}"
SPARK_SA="steam-spark-${ENVIRONMENT}@${PROJECT_ID}.iam.gserviceaccount.com"

# Unique batch ID so concurrent submissions don't collide
BATCH_ID="silver-transform-${DT//\-/}-$(date +%H%M%S)"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "================================================================="
echo " Dataproc Serverless — silver transform"
echo "   dt           : ${DT}"
echo "   bronze bucket: ${BRONZE_BUCKET}"
echo "   silver bucket: ${SILVER_BUCKET}"
echo "   staging      : ${STAGING_BUCKET}"
echo "   batch id     : ${BATCH_ID}"
echo "================================================================="

# ── 1. Package spark/ module + vaderSentiment into a single zip ─────────────
# vaderSentiment is pure Python (no C extensions) so we bundle it directly
# instead of relying on spark.dataproc.pip.packages (unreliable on default runtime).
echo "[1/4] Packaging spark/ module + vaderSentiment..."
cd "${REPO_ROOT}"
TMP_ZIP="/tmp/spark_pkg_$$.zip"

# Find vaderSentiment in the active venv/system Python path
VADER_DIR=$(python3 -c "import vaderSentiment, os; print(os.path.dirname(vaderSentiment.__file__))" 2>/dev/null \
    || .venv/bin/python -c "import vaderSentiment, os; print(os.path.dirname(vaderSentiment.__file__))")
if [[ -z "${VADER_DIR}" ]]; then
    echo "ERROR: vaderSentiment not found. Run: pip install vaderSentiment==3.3.2"
    exit 1
fi
echo "      vaderSentiment: ${VADER_DIR}"

zip -q "${TMP_ZIP}" \
    spark/__init__.py \
    spark/sentiment.py \
    spark/build_matrix.py

# Add vaderSentiment package (lexicon .txt files must be included)
cd "$(dirname "${VADER_DIR}")"
zip -qr "${TMP_ZIP}" vaderSentiment/
cd "${REPO_ROOT}"
echo "      -> ${TMP_ZIP}"

# ── 2. Upload job files to staging bucket ───────────────────────────────────
echo "[2/4] Uploading job files to ${STAGING_BUCKET}/jobs/${DT}/..."
gcloud storage cp "${TMP_ZIP}" \
    "${STAGING_BUCKET}/jobs/${DT}/spark.zip" \
    --quiet
gcloud storage cp spark/transform_silver.py \
    "${STAGING_BUCKET}/jobs/${DT}/transform_silver.py" \
    --quiet
rm -f "${TMP_ZIP}"
echo "      -> uploaded"

# ── 3. Submit Dataproc Serverless batch ─────────────────────────────────────
echo "[3/4] Submitting batch job ${BATCH_ID}..."
gcloud dataproc batches submit pyspark \
    "${STAGING_BUCKET}/jobs/${DT}/transform_silver.py" \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --batch="${BATCH_ID}" \
    --service-account="${SPARK_SA}" \
    --staging-bucket="${STAGING_BUCKET_NAME}" \
    --py-files="${STAGING_BUCKET}/jobs/${DT}/spark.zip" \
    --properties="spark.sql.shuffle.partitions=8" \
    -- \
    --input="${BRONZE_BUCKET}" \
    --output="${SILVER_BUCKET}" \
    --dt="${DT}" \
    --language="english"

# gcloud waits for completion by default (no --async flag used)
echo "[4/4] Batch completed successfully."
echo ""
echo "Silver Parquet written to: ${SILVER_BUCKET}/reviews/dt=${DT}/"
echo "Next step: run scripts/bq_load_silver.sh DT=${DT}"
