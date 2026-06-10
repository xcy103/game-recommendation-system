# ---------------------------------------------------------------------------
# Workload Identity Federation — GitHub Actions → GCP (no SA key files)
# DESIGN.md §11: "Step 3 — WIF: GitHub → GCP auth via Workload Identity Federation"
#
# Resources created:
#   - WIF pool:     projects/.../locations/global/workloadIdentityPools/github-actions-{env}
#   - WIF provider: .../providers/github-oidc  (OIDC, scoped to var.github_repo)
#   - CI SA:        steam-github-ci-{env}  (no key file — auth via WIF only)
#   - AR repo:      {region}-docker.pkg.dev/{project}/steam-{env}
# ---------------------------------------------------------------------------

# One pool per environment; the pool name becomes part of the principalSet URI
resource "google_iam_workload_identity_pool" "github" {
  workload_identity_pool_id = "github-actions-${var.environment}"
  display_name              = "GitHub Actions [${var.environment}]"
  description               = "Allows GitHub Actions to authenticate to GCP without a SA key file."
  project                   = var.project_id
}

# GitHub OIDC provider — restricted to one repo via attribute_condition
resource "google_iam_workload_identity_pool_provider" "github" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.github.workload_identity_pool_id
  workload_identity_pool_provider_id = "github-oidc"
  display_name                       = "GitHub OIDC"
  project                            = var.project_id

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }

  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.actor"      = "assertion.actor"
    "attribute.repository" = "assertion.repository"
  }

  # Only tokens from this specific repo are accepted
  attribute_condition = "attribute.repository == '${var.github_repo}'"
}

# Dedicated CI/CD service account — receives no key file, auth via WIF only
resource "google_service_account" "github_ci" {
  account_id   = "steam-github-ci-${var.environment}"
  display_name = "Steam GitHub CI [${var.environment}]"
  description  = "Used by GitHub Actions via WIF for image push, terraform plan, and DAG deploy."
  project      = var.project_id
}

# Bind: any GitHub Actions token from var.github_repo may impersonate this SA
resource "google_service_account_iam_member" "wif_binding" {
  service_account_id = google_service_account.github_ci.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github.name}/attribute.repository/${var.github_repo}"
}

# ---------------------------------------------------------------------------
# CI SA permissions
# ---------------------------------------------------------------------------

# Push container images to Artifact Registry
resource "google_project_iam_member" "ci_ar_writer" {
  project = var.project_id
  role    = "roles/artifactregistry.writer"
  member  = "serviceAccount:${google_service_account.github_ci.email}"
}

# Read all project resources — required for `terraform plan` to diff current state
resource "google_project_iam_member" "ci_viewer" {
  project = var.project_id
  role    = "roles/viewer"
  member  = "serviceAccount:${google_service_account.github_ci.email}"
}

# roles/viewer excludes storage.buckets.getIamPolicy; securityReviewer adds it
# so that `terraform plan` can refresh bucket IAM binding resources.
resource "google_project_iam_member" "ci_security_reviewer" {
  project = var.project_id
  role    = "roles/iam.securityReviewer"
  member  = "serviceAccount:${google_service_account.github_ci.email}"
}

# Read-only access to Terraform remote state — allows `terraform plan` in CI
# without requiring a full state admin role.
resource "google_storage_bucket_iam_member" "ci_tfstate_viewer" {
  bucket = var.tfstate_bucket
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.github_ci.email}"
}

# ---------------------------------------------------------------------------
# Artifact Registry Docker repository
# ---------------------------------------------------------------------------
resource "google_artifact_registry_repository" "containers" {
  location      = var.region
  repository_id = "steam-${var.environment}"
  description   = "Docker images for the Steam Reviews platform [${var.environment}]"
  format        = "DOCKER"
  project       = var.project_id
}
