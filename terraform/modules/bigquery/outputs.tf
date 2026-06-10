output "staging_dataset_id" {
  value = google_bigquery_dataset.staging.dataset_id
}

output "staging_dataset_project" {
  value = google_bigquery_dataset.staging.project
}

output "gold_dataset_id" {
  value = google_bigquery_dataset.gold.dataset_id
}

output "gold_dataset_project" {
  value = google_bigquery_dataset.gold.project
}

output "stg_reviews_table_id" {
  value = google_bigquery_table.stg_reviews.table_id
}

output "game_recommendations_table_id" {
  value = google_bigquery_table.game_recommendations.table_id
}
