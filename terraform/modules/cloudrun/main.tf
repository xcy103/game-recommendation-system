# DESIGN §8: FastAPI recommender demo on Cloud Run (scales to zero).
# Gated via enable_cloudrun=true at root.
resource "google_cloud_run_v2_service" "recommender_api" {
  name     = "steam-recommender-api-${var.environment}"
  location = var.region
  project  = var.project_id

  # Ingress: allow all external traffic (unauthenticated for demo purposes)
  ingress = "INGRESS_TRAFFIC_ALL"

  template {
    service_account = var.api_sa_email

    scaling {
      # Scale to zero when idle — eliminates idle costs entirely
      min_instance_count = 0
      max_instance_count = 3
    }

    containers {
      image = var.container_image

      ports {
        container_port = 8080
      }

      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
        cpu_idle = true # Only charge CPU during request processing
      }

      env {
        name  = "ENVIRONMENT"
        value = var.environment
      }

      env {
        name  = "GCP_PROJECT"
        value = var.project_id
      }
    }
  }

  labels = {
    environment = var.environment
    managed_by  = "terraform"
  }
}

# Allow unauthenticated invocations for the demo endpoint
resource "google_cloud_run_v2_service_iam_member" "public_invoker" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.recommender_api.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}
