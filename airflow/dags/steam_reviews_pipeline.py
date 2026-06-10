"""
Daily Airflow DAG: steam_reviews_pipeline (DESIGN.md §9).

Orchestrates the full end-to-end batch pipeline:

  ingest_reviews
      └─▶ spark_transform_silver
              └─▶ load_silver_to_bq
                      └─▶ dbt_run
                              └─▶ dbt_test
                                      └─▶ train_recommender
                                                  └─▶ publish_recommendations_to_bq

Design principles:
  - All tasks are idempotent and keyed by logical_date (ds).
    Re-running a date overwrites its partition cleanly.
  - Task failures leave previous partitions intact (no partial overwrites of
    other dates).
  - Composer SA (steam-composer-dev) impersonates per-stage SAs at runtime.

TODO: implement all operators and wire the DAG.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.models.dag import DAG

# TODO: import GCP operators
# from airflow.providers.google.cloud.operators.dataproc import ...
# from airflow.providers.google.cloud.transfers.gcs_to_bigquery import ...

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="steam_reviews_pipeline",
    description="Daily Bronze→Silver→BQ Staging→dbt Gold→ALS Recommendations",
    schedule="0 6 * * *",       # 06:00 UTC daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,           # prevent overlapping date partitions
    default_args=DEFAULT_ARGS,
    tags=["steam", "batch", "medallion"],
) as dag:
    # TODO: define tasks
    # ingest_reviews = ...
    # spark_transform_silver = ...
    # load_silver_to_bq = ...
    # dbt_run = ...
    # dbt_test = ...
    # train_recommender = ...
    # publish_recommendations_to_bq = ...
    #
    # Task ordering:
    # ingest_reviews >> spark_transform_silver >> load_silver_to_bq
    # >> dbt_run >> dbt_test >> train_recommender >> publish_recommendations_to_bq
    pass
