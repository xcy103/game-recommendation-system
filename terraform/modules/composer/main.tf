# DESIGN §9: Composer is the most expensive component (~$300+/month).
# Gated via enable_composer=true at root. Keep false during development.
resource "google_composer_environment" "this" {
  name    = "steam-composer-${var.environment}"
  region  = var.region
  project = var.project_id

  config {
    software_config {
      image_version = var.composer_image_version

      # Airflow config overrides
      airflow_config_overrides = {
        "core-dags_are_paused_at_creation" = "True"
        "core-max_active_runs_per_dag"     = "1"
      }

      pypi_packages = {
        "apache-airflow-providers-google"   = ">=10.0.0"
        "apache-airflow-providers-cncf-kubernetes" = ">=7.0.0"
      }

      env_variables = {
        ENVIRONMENT = var.environment
      }
    }

    # Composer 2 workload sizing — SMALL keeps costs minimal
    workloads_config {
      scheduler {
        cpu        = 0.5
        memory_gb  = 1.875
        storage_gb = 1
        count      = 1
      }
      web_server {
        cpu        = 0.5
        memory_gb  = 1.875
        storage_gb = 1
      }
      worker {
        cpu        = 0.5
        memory_gb  = 1.875
        storage_gb = 1
        min_count  = 1
        max_count  = 3
      }
    }

    environment_size = "ENVIRONMENT_SIZE_SMALL"

    node_config {
      service_account = var.composer_sa_email
    }
  }

  labels = {
    environment = var.environment
    managed_by  = "terraform"
  }
}
