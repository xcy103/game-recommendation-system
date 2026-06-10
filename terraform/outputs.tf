output "bronze_bucket_name" {
  description = "GCS bucket for raw bronze JSON (DESIGN §4)"
  value       = module.storage.bronze_bucket_name
}

output "silver_bucket_name" {
  description = "GCS bucket for cleaned silver Parquet (DESIGN §4)"
  value       = module.storage.silver_bucket_name
}

output "staging_dataset_id" {
  description = "BigQuery staging dataset (stg_ models)"
  value       = module.bigquery.staging_dataset_id
}

output "gold_dataset_id" {
  description = "BigQuery gold dataset (star schema)"
  value       = module.bigquery.gold_dataset_id
}

output "ingester_sa_email" {
  description = "Service account for the Steam API ingester"
  value       = module.iam.ingester_sa_email
}

output "spark_sa_email" {
  description = "Service account for PySpark / Dataproc jobs"
  value       = module.iam.spark_sa_email
}

output "dbt_sa_email" {
  description = "Service account for dbt gold builds"
  value       = module.iam.dbt_sa_email
}

output "composer_sa_email" {
  description = "Service account for Cloud Composer"
  value       = module.iam.composer_sa_email
}

output "api_sa_email" {
  description = "Service account for the Cloud Run recommender API"
  value       = module.iam.api_sa_email
}

output "composer_airflow_uri" {
  description = "Airflow web UI URL (only set when enable_composer = true)"
  value       = var.enable_composer ? module.composer[0].airflow_uri : "Composer disabled — set enable_composer=true to deploy"
}

output "cloudrun_service_url" {
  description = "Recommender API URL (only set when enable_cloudrun = true)"
  value       = var.enable_cloudrun ? module.cloudrun[0].service_url : "Cloud Run disabled — set enable_cloudrun=true to deploy"
}
