"""
Daily Airflow DAG: steam_reviews_pipeline (DESIGN.md §9).

Orchestrates the full batch pipeline once per day:

  ingest_reviews
      └─▶ spark_transform_silver
              └─▶ load_silver_to_bq
                      └─▶ dbt_run
                              └─▶ dbt_test
                                      └─▶ train_recommender
                                                  └─▶ publish_recommendations_to_bq

Design principles:
  - All tasks keyed by {{ ds }} (logical execution date = bronze partition dt).
    Re-running a date overwrites only that partition; other partitions are untouched.
  - Task failures leave all previous partitions intact.
  - Composer SA (steam-composer-dev) impersonates per-stage SAs at runtime.

Local development (docker/docker-compose-airflow.yml):
  Set LOCAL_MODE=true in the container environment.
  Each BashOperator will echo what it would run instead of executing cloud calls.
  This lets you verify DAG structure and dependency order without GCP access.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# ── Pipeline constants (match terraform/terraform.tfvars) ─────────────────
_PROJECT  = "steam-reviews-platform"
_REGION   = "us-central1"
_ENV      = "dev"
_GOLD     = f"steam_gold_{_ENV}"
_SILVER   = f"{_PROJECT}-silver-{_ENV}"
_STAGING  = f"{_PROJECT}-dataproc-staging-{_ENV}"
_SPARK_SA = f"steam-spark-{_ENV}@{_PROJECT}.iam.gserviceaccount.com"

# Shared env injected into every BashOperator.
# DT uses Airflow Jinja rendering (BashOperator.template_fields includes "env").
# LOCAL_MODE is read from the container environment at parse time so it
# propagates into the subprocess even though BashOperator replaces os.environ.
_BASE_ENV: dict[str, str] = {
    "DT":          "{{ ds }}",   # rendered to YYYY-MM-DD at runtime
    "PROJECT_ID":  _PROJECT,
    "REGION":      _REGION,
    "ENVIRONMENT": _ENV,
    "GOLD":        _GOLD,
    "SILVER":      _SILVER,
    "STAGING":     _STAGING,
    "SPARK_SA":    _SPARK_SA,
    "LOCAL_MODE":  os.getenv("LOCAL_MODE", "false"),
}

# Local-mode guard reused at the top of every bash_command.
# When LOCAL_MODE=true, tasks print a dry-run message and exit 0.
_DRY_RUN_GUARD = """\
if [[ "${LOCAL_MODE:-false}" == "true" ]]; then
    echo "[dry-run] ${TASK_NAME}  dt=${DT:-<unset>}"
    exit 0
fi
"""

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="steam_reviews_pipeline",
    description="Daily Bronze→Silver→BQ Staging→dbt Gold→ALS Recommendations",
    schedule="0 6 * * *",        # 06:00 UTC daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,            # one active run at a time; prevents overlapping partitions
    default_args=DEFAULT_ARGS,
    tags=["steam", "batch", "medallion"],
    doc_md=__doc__,
) as dag:

    # ── Task 1: Ingest Steam reviews → bronze GCS ──────────────────────────
    # Runs ingestion/ingester.py for the logical date partition.
    # Output: gs://{PROJECT_ID}-bronze-{ENV}/reviews/dt=DT/appid=*/part-*.json
    ingest_reviews = BashOperator(
        task_id="ingest_reviews",
        env={**_BASE_ENV, "TASK_NAME": "ingest_reviews"},
        bash_command=_DRY_RUN_GUARD + """\
set -euo pipefail
cd /opt/airflow
python -m ingestion.ingester \
  --dt "${DT}" \
  --project "${PROJECT_ID}" \
  --environment "${ENVIRONMENT}"
""",
    )

    # ── Task 2: bronze → silver transform on Dataproc Serverless ──────────
    # Runs scripts/dataproc_submit.sh: package spark/, submit PySpark batch.
    # Output: gs://{PROJECT_ID}-silver-{ENV}/reviews/dt=DT/*.parquet
    spark_transform_silver = BashOperator(
        task_id="spark_transform_silver",
        env={**_BASE_ENV, "TASK_NAME": "spark_transform_silver"},
        bash_command=_DRY_RUN_GUARD + """\
set -euo pipefail
bash /opt/airflow/scripts/dataproc_submit.sh
""",
    )

    # ── Task 3: load silver Parquet → BigQuery stg_reviews ────────────────
    # Runs scripts/bq_load_silver.sh: bq load into DT partition.
    load_silver_to_bq = BashOperator(
        task_id="load_silver_to_bq",
        env={**_BASE_ENV, "TASK_NAME": "load_silver_to_bq"},
        bash_command=_DRY_RUN_GUARD + """\
set -euo pipefail
bash /opt/airflow/scripts/bq_load_silver.sh
""",
    )

    # ── Task 4: dbt run — build gold star schema ───────────────────────────
    # Runs all dbt models: dim_games, dim_users, fact_reviews, etc.
    dbt_run = BashOperator(
        task_id="dbt_run",
        env={**_BASE_ENV, "TASK_NAME": "dbt_run"},
        bash_command=_DRY_RUN_GUARD + """\
set -euo pipefail
cd /opt/airflow/dbt
dbt run --profiles-dir . --target dev --no-version-check
""",
    )

    # ── Task 5: dbt test — data quality gate ──────────────────────────────
    # Runs schema tests: not_null, unique, relationships, custom positive_rate.
    # Pipeline halts here if data quality fails.
    dbt_test = BashOperator(
        task_id="dbt_test",
        env={**_BASE_ENV, "TASK_NAME": "dbt_test"},
        bash_command=_DRY_RUN_GUARD + """\
set -euo pipefail
cd /opt/airflow/dbt
dbt test --profiles-dir . --target dev --no-version-check
""",
    )

    # ── Task 6: Train ALS recommender on Dataproc Serverless ──────────────
    # Steps:
    #   1. bq extract dim_users → GCS Parquet (user credentials via ADC)
    #   2. Upload spark/recommender.py to staging bucket
    #   3. Submit Dataproc Serverless batch (writes recs Parquet to GCS)
    # Output: gs://{STAGING}/recommendations/DT/*.parquet
    train_recommender = BashOperator(
        task_id="train_recommender",
        env={**_BASE_ENV, "TASK_NAME": "train_recommender", "TOP_N": "10"},
        execution_timeout=timedelta(hours=1),
        bash_command=_DRY_RUN_GUARD + """\
set -euo pipefail
DIM_USERS_GCS="gs://${STAGING}/dim_users/${DT}/"

# 1. Export dim_users from BQ gold → GCS (used by Spark to resolve user_sk)
bq extract \
    --destination_format=PARQUET \
    --compression=SNAPPY \
    "${PROJECT_ID}:${GOLD}.dim_users" \
    "${DIM_USERS_GCS}dim_users_*.parquet"

# 2. Upload recommender.py to staging
gcloud storage cp /opt/airflow/spark/recommender.py \
    "gs://${STAGING}/jobs/als/${DT}/recommender.py" \
    --quiet

# 3. Submit Dataproc Serverless batch
BATCH_ID="als-train-${DT//-/}-$(date +%H%M%S)"
gcloud dataproc batches submit pyspark \
    "gs://${STAGING}/jobs/als/${DT}/recommender.py" \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --batch="${BATCH_ID}" \
    --service-account="${SPARK_SA}" \
    --staging-bucket="${STAGING}" \
    --properties="spark.sql.shuffle.partitions=8,spark.executor.memory=8g" \
    -- \
    --dt="${DT}" \
    --silver-bucket="${SILVER}" \
    --staging-bucket="${STAGING}" \
    --dim-users-path="${DIM_USERS_GCS}" \
    --top-n="${TOP_N}"
""",
    )

    # ── Task 7: load recommendations Parquet → game_recommendations ───────
    # Loads the GCS Parquet written by train_recommender into BQ gold.
    # WRITE_TRUNCATE on the whole table (single daily batch; all ranks for all users).
    publish_recommendations_to_bq = BashOperator(
        task_id="publish_recommendations_to_bq",
        env={**_BASE_ENV, "TASK_NAME": "publish_recommendations_to_bq"},
        bash_command=_DRY_RUN_GUARD + """\
set -euo pipefail
bq load \
    --project_id="${PROJECT_ID}" \
    --location="${REGION}" \
    --source_format=PARQUET \
    --replace=true \
    "${PROJECT_ID}:${GOLD}.game_recommendations" \
    "gs://${STAGING}/recommendations/${DT}/*.parquet"

echo "Published recommendations for ${DT} → ${PROJECT_ID}:${GOLD}.game_recommendations"
""",
    )

    # ── Dependency chain ───────────────────────────────────────────────────
    (
        ingest_reviews
        >> spark_transform_silver
        >> load_silver_to_bq
        >> dbt_run
        >> dbt_test
        >> train_recommender
        >> publish_recommendations_to_bq
    )
