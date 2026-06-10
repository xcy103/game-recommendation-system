output "staging_bucket_name" {
  description = "GCS bucket used for Dataproc staging (temp files, job packages)"
  value       = google_storage_bucket.dataproc_staging.name
}

output "staging_bucket_url" {
  description = "gs:// URI for the Dataproc staging bucket"
  value       = "gs://${google_storage_bucket.dataproc_staging.name}"
}
