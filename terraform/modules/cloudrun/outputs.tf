output "service_url" {
  description = "Public HTTPS URL for the recommender API"
  value       = google_cloud_run_v2_service.recommender_api.uri
}

output "service_name" {
  value = google_cloud_run_v2_service.recommender_api.name
}
