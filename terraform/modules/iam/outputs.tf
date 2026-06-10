output "ingester_sa_email" {
  value = google_service_account.ingester.email
}

output "ingester_sa_name" {
  value = google_service_account.ingester.name
}

output "spark_sa_email" {
  value = google_service_account.spark.email
}

output "spark_sa_name" {
  value = google_service_account.spark.name
}

output "dbt_sa_email" {
  value = google_service_account.dbt.email
}

output "dbt_sa_name" {
  value = google_service_account.dbt.name
}

output "composer_sa_email" {
  value = google_service_account.composer.email
}

output "composer_sa_name" {
  value = google_service_account.composer.name
}

output "api_sa_email" {
  value = google_service_account.api.email
}

output "api_sa_name" {
  value = google_service_account.api.name
}
