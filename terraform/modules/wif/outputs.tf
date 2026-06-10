output "workload_identity_provider" {
  description = "Full WIF provider resource name. Set as GCP_WORKLOAD_IDENTITY_PROVIDER in GitHub secrets."
  value       = google_iam_workload_identity_pool_provider.github.name
}

output "github_ci_sa_email" {
  description = "GitHub CI service account email. Set as GCP_SERVICE_ACCOUNT in GitHub secrets."
  value       = google_service_account.github_ci.email
}

output "artifact_registry_url" {
  description = "Base URL for Docker images: {url}/ingester:{tag}"
  value       = "${google_artifact_registry_repository.containers.location}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.containers.repository_id}"
}
