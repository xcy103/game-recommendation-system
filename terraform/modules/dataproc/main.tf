# ---------------------------------------------------------------------------
# Dataproc Serverless — infrastructure prerequisites (DESIGN §6, §10, §15)
#
# Actual batch jobs are submitted via gcloud (see scripts/dataproc_submit.sh),
# NOT via Terraform. Terraform only provisions the long-lived prerequisites:
#   - A GCS staging bucket for Dataproc temp files / job packages
#   - IAM bindings the Spark SA needs to run Serverless batches
#
# COST GUARD (DESIGN §15):
#   Dataproc Serverless bills per vCPU-second while a batch is running.
#   Billing stops automatically when the job exits — no idle charges.
#   Estimated cost per transform run:
#     smoke test  (~1 500 rows) : < $0.01
#     full run    (~10 M rows)  : ~ $0.50–$2.00 depending on runtime
#   The staging bucket itself costs ~$0/month when empty (lifecycle auto-cleans).
# ---------------------------------------------------------------------------

resource "google_storage_bucket" "dataproc_staging" {
  name                        = var.staging_bucket
  project                     = var.project_id
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  # Allow terraform destroy to wipe bucket in non-prod
  force_destroy = var.environment != "prod"

  # COST GUARD: auto-delete Dataproc temp files and uploaded job packages
  # after 3 days so the bucket stays near-empty between runs.
  lifecycle_rule {
    condition {
      age = 3
    }
    action {
      type = "Delete"
    }
  }
}

# ---------------------------------------------------------------------------
# IAM: Spark SA needs objectAdmin on the staging bucket (read/write temp files)
# ---------------------------------------------------------------------------
resource "google_storage_bucket_iam_member" "spark_sa_staging_admin" {
  bucket = google_storage_bucket.dataproc_staging.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${var.spark_sa_email}"
}

# ---------------------------------------------------------------------------
# IAM: Dataproc Serverless requires the batch SA to be able to act as itself.
# https://cloud.google.com/dataproc-serverless/docs/concepts/iam
# (roles/dataproc.worker is already granted project-wide in the IAM module)
# ---------------------------------------------------------------------------
resource "google_service_account_iam_member" "spark_sa_self_user" {
  service_account_id = "projects/${var.project_id}/serviceAccounts/${var.spark_sa_email}"
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${var.spark_sa_email}"
}
