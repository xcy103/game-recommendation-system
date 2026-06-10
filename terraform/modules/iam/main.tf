# ---------------------------------------------------------------------------
# Service accounts — one per pipeline stage (DESIGN §10, §15: least privilege)
# ---------------------------------------------------------------------------

resource "google_service_account" "ingester" {
  account_id   = "steam-ingester-${var.environment}"
  display_name = "Steam Ingester [${var.environment}]"
  description  = "Runs the rate-limited Steam API ingester; writes raw JSON to bronze bucket."
  project      = var.project_id
}

resource "google_service_account" "spark" {
  account_id   = "steam-spark-${var.environment}"
  display_name = "Steam Spark / Dataproc [${var.environment}]"
  description  = "Runs PySpark bronze→silver transforms on Dataproc; loads silver Parquet to BQ staging."
  project      = var.project_id
}

resource "google_service_account" "dbt" {
  account_id   = "steam-dbt-${var.environment}"
  display_name = "Steam dbt [${var.environment}]"
  description  = "Runs dbt gold star schema builds in BigQuery."
  project      = var.project_id
}

resource "google_service_account" "composer" {
  account_id   = "steam-composer-${var.environment}"
  display_name = "Steam Composer / Airflow [${var.environment}]"
  description  = "Cloud Composer (Airflow) orchestrator; impersonates other SAs to trigger pipeline steps."
  project      = var.project_id
}

resource "google_service_account" "api" {
  account_id   = "steam-api-${var.environment}"
  display_name = "Steam Recommender API [${var.environment}]"
  description  = "Cloud Run FastAPI service; read-only access to gold BigQuery dataset."
  project      = var.project_id
}

# ---------------------------------------------------------------------------
# Ingester SA — bronze bucket access only
# Needs objectAdmin (read checkpoints + write new JSON); no project-level roles.
# ---------------------------------------------------------------------------
resource "google_storage_bucket_iam_member" "ingester_bronze_admin" {
  bucket = var.bronze_bucket_name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.ingester.email}"
}

# ---------------------------------------------------------------------------
# Spark SA — reads bronze, writes silver, loads to BQ staging
# ---------------------------------------------------------------------------
resource "google_storage_bucket_iam_member" "spark_bronze_viewer" {
  bucket = var.bronze_bucket_name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.spark.email}"
}

resource "google_storage_bucket_iam_member" "spark_silver_admin" {
  bucket = var.silver_bucket_name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.spark.email}"
}

# dataEditor on staging: create/update stg_reviews table rows
resource "google_bigquery_dataset_iam_member" "spark_staging_editor" {
  project    = var.project_id
  dataset_id = var.staging_dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.spark.email}"
}

# Spark ALS job: read dim_users from gold (user_sk mapping) + write game_recommendations
resource "google_bigquery_dataset_iam_member" "spark_gold_viewer" {
  project    = var.project_id
  dataset_id = var.gold_dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.spark.email}"
}

resource "google_bigquery_dataset_iam_member" "spark_gold_editor" {
  project    = var.project_id
  dataset_id = var.gold_dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.spark.email}"
}

# jobUser: required to run BQ load jobs (project-level, minimum needed)
resource "google_project_iam_member" "spark_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.spark.email}"
}

# dataproc.worker: allows job submission on Dataproc clusters / Serverless
resource "google_project_iam_member" "spark_dataproc_worker" {
  project = var.project_id
  role    = "roles/dataproc.worker"
  member  = "serviceAccount:${google_service_account.spark.email}"
}

# ---------------------------------------------------------------------------
# dbt SA — reads BQ staging, writes BQ gold
# ---------------------------------------------------------------------------
resource "google_bigquery_dataset_iam_member" "dbt_staging_viewer" {
  project    = var.project_id
  dataset_id = var.staging_dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.dbt.email}"
}

resource "google_bigquery_dataset_iam_member" "dbt_gold_editor" {
  project    = var.project_id
  dataset_id = var.gold_dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.dbt.email}"
}

resource "google_project_iam_member" "dbt_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.dbt.email}"
}

# ---------------------------------------------------------------------------
# Composer SA — orchestrates all steps; must impersonate other SAs
# ---------------------------------------------------------------------------
resource "google_project_iam_member" "composer_worker" {
  project = var.project_id
  role    = "roles/composer.worker"
  member  = "serviceAccount:${google_service_account.composer.email}"
}

# Allows Composer to submit Dataproc jobs
resource "google_project_iam_member" "composer_dataproc_editor" {
  project = var.project_id
  role    = "roles/dataproc.editor"
  member  = "serviceAccount:${google_service_account.composer.email}"
}

# Allows Airflow operators to run BQ jobs (e.g. GCSToBigQueryOperator)
resource "google_project_iam_member" "composer_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.composer.email}"
}

# Allows Composer to impersonate other pipeline SAs (serviceAccountUser)
resource "google_service_account_iam_member" "composer_impersonate_ingester" {
  service_account_id = google_service_account.ingester.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.composer.email}"
}

resource "google_service_account_iam_member" "composer_impersonate_spark" {
  service_account_id = google_service_account.spark.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.composer.email}"
}

resource "google_service_account_iam_member" "composer_impersonate_dbt" {
  service_account_id = google_service_account.dbt.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.composer.email}"
}

# ---------------------------------------------------------------------------
# API SA — Cloud Run read-only access to gold BQ dataset
# ---------------------------------------------------------------------------
resource "google_bigquery_dataset_iam_member" "api_gold_viewer" {
  project    = var.project_id
  dataset_id = var.gold_dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.api.email}"
}

resource "google_project_iam_member" "api_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.api.email}"
}
