terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ---------------------------------------------------------------------------
# Enable required GCP APIs
# ---------------------------------------------------------------------------
resource "google_project_service" "apis" {
  for_each = toset([
    "storage.googleapis.com",
    "bigquery.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "dataproc.googleapis.com",
    "composer.googleapis.com",
    "run.googleapis.com",
    "artifactregistry.googleapis.com",
    "cloudbuild.googleapis.com",
    "billingbudgets.googleapis.com",
    "cloudresourcemanager.googleapis.com",
  ])

  project                    = var.project_id
  service                    = each.value
  disable_on_destroy         = false
  disable_dependent_services = false
}

# ---------------------------------------------------------------------------
# Storage module — bronze (raw JSON) + silver (Parquet) buckets
# ---------------------------------------------------------------------------
module "storage" {
  source = "./modules/storage"

  project_id                      = var.project_id
  region                          = var.region
  environment                     = var.environment
  bronze_bucket                   = "${var.project_id}-bronze-${var.environment}"
  silver_bucket                   = "${var.project_id}-silver-${var.environment}"
  bronze_retention_days           = var.bronze_retention_days
  bronze_nearline_transition_days = var.bronze_nearline_transition_days
  silver_retention_days           = var.silver_retention_days

  depends_on = [google_project_service.apis]
}

# ---------------------------------------------------------------------------
# BigQuery module — staging and gold datasets + key tables
# ---------------------------------------------------------------------------
module "bigquery" {
  source = "./modules/bigquery"

  project_id  = var.project_id
  region      = var.region
  environment = var.environment

  depends_on = [google_project_service.apis]
}

# ---------------------------------------------------------------------------
# IAM module — least-privilege service accounts for each pipeline stage
# ---------------------------------------------------------------------------
module "iam" {
  source = "./modules/iam"

  project_id         = var.project_id
  environment        = var.environment
  bronze_bucket_name = module.storage.bronze_bucket_name
  silver_bucket_name = module.storage.silver_bucket_name
  staging_dataset_id = module.bigquery.staging_dataset_id
  gold_dataset_id    = module.bigquery.gold_dataset_id

  depends_on = [module.storage, module.bigquery]
}

# ---------------------------------------------------------------------------
# Dataproc module (gated — default off)
# Only the staging bucket + IAM bindings are created here.
# Batch jobs are submitted on-demand via scripts/dataproc_submit.sh.
# COST GUARD (DESIGN §15): billing is per-job, not per-idle-resource.
# ---------------------------------------------------------------------------
module "dataproc" {
  count  = var.enable_dataproc ? 1 : 0
  source = "./modules/dataproc"

  project_id     = var.project_id
  region         = var.region
  environment    = var.environment
  spark_sa_email = module.iam.spark_sa_email
  staging_bucket = "${var.project_id}-dataproc-staging-${var.environment}"

  depends_on = [module.iam, google_project_service.apis]
}

# ---------------------------------------------------------------------------
# Composer module (gated — default off, ~$300+/month)
# ---------------------------------------------------------------------------
module "composer" {
  count  = var.enable_composer ? 1 : 0
  source = "./modules/composer"

  project_id             = var.project_id
  region                 = var.region
  environment            = var.environment
  composer_sa_email      = module.iam.composer_sa_email
  composer_image_version = var.composer_image_version

  depends_on = [module.iam, google_project_service.apis]
}

# ---------------------------------------------------------------------------
# Cloud Run module (gated — default off, scales to zero)
# ---------------------------------------------------------------------------
module "cloudrun" {
  count  = var.enable_cloudrun ? 1 : 0
  source = "./modules/cloudrun"

  project_id      = var.project_id
  region          = var.region
  environment     = var.environment
  api_sa_email    = module.iam.api_sa_email
  container_image = var.cloudrun_image

  depends_on = [module.iam, google_project_service.apis]
}

# ---------------------------------------------------------------------------
# Budget alert (only created when billing_account_id is set)
# DESIGN §15: "Set a GCP budget alert. Use the $300 free trial credit."
# ---------------------------------------------------------------------------
resource "google_billing_budget" "budget" {
  count = var.billing_account_id != "" ? 1 : 0

  billing_account = var.billing_account_id
  display_name    = "steam-reviews-${var.environment}"

  budget_filter {
    projects = ["projects/${var.project_id}"]
  }

  amount {
    specified_amount {
      currency_code = "USD"
      units         = tostring(var.monthly_budget_usd)
    }
  }

  # 50% — early warning
  threshold_rules {
    threshold_percent = 0.5
    spend_basis       = "CURRENT_SPEND"
  }

  # 80% — action required
  threshold_rules {
    threshold_percent = 0.8
    spend_basis       = "CURRENT_SPEND"
  }

  # 100% — at limit
  threshold_rules {
    threshold_percent = 1.0
    spend_basis       = "CURRENT_SPEND"
  }

}
