output "bronze_bucket_name" {
  value = google_storage_bucket.bronze.name
}

output "bronze_bucket_url" {
  value = google_storage_bucket.bronze.url
}

output "silver_bucket_name" {
  value = google_storage_bucket.silver.name
}

output "silver_bucket_url" {
  value = google_storage_bucket.silver.url
}
