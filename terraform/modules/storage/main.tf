# DESIGN §4: Bronze — raw API JSON, append-only, source of truth for replay
resource "google_storage_bucket" "bronze" {
  name          = var.bronze_bucket
  location      = var.region
  project       = var.project_id
  storage_class = "STANDARD"

  # Allow `terraform destroy` to wipe bucket contents in non-prod
  force_destroy = var.environment != "prod"

  uniform_bucket_level_access = true

  # Prevent accidental public exposure
  public_access_prevention = "enforced"

  versioning {
    # Bronze is append-only; versioning adds cost with no benefit here
    enabled = false
  }

  # Move to NEARLINE after 30 days (cheaper storage, still warm enough for replays)
  lifecycle_rule {
    condition {
      age = var.bronze_nearline_transition_days
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  # Delete stale raw data to cap storage cost (DESIGN §15)
  lifecycle_rule {
    condition {
      age = var.bronze_retention_days
    }
    action {
      type = "Delete"
    }
  }

  labels = {
    environment = var.environment
    layer       = "bronze"
    managed_by  = "terraform"
  }
}

# DESIGN §4: Silver — schema-enforced, deduplicated Parquet, partitioned by dt
resource "google_storage_bucket" "silver" {
  name          = var.silver_bucket
  location      = var.region
  project       = var.project_id
  storage_class = "STANDARD"

  force_destroy = var.environment != "prod"

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = false
  }

  # Silver data is valuable longer (BigQuery load target), but still prunable
  lifecycle_rule {
    condition {
      age = var.silver_retention_days
    }
    action {
      type = "Delete"
    }
  }

  labels = {
    environment = var.environment
    layer       = "silver"
    managed_by  = "terraform"
  }
}
