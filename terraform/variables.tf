variable "project_id" {
  type        = string
  description = "GCP project ID"
}

variable "region" {
  type        = string
  description = "GCP region for all resources"
  default     = "us-central1"
}

variable "environment" {
  type        = string
  description = "Deployment environment: dev or prod"
  default     = "dev"
  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "environment must be 'dev' or 'prod'."
  }
}

# --- Feature gates (off by default to control cost) ---

variable "enable_dataproc" {
  type        = bool
  description = "Create Dataproc Serverless prerequisites (staging bucket + IAM). Required before running spark jobs on GCP."
  default     = false
}

variable "enable_composer" {
  type        = bool
  description = "Create Cloud Composer environment. Expensive (~$300+/month) — keep false unless actively demoing."
  default     = false
}

variable "enable_cloudrun" {
  type        = bool
  description = "Deploy recommender FastAPI service on Cloud Run. Requires a built image."
  default     = false
}

# --- Budget alert ---

variable "billing_account_id" {
  type        = string
  description = "GCP billing account ID (format: XXXXXX-XXXXXX-XXXXXX). Leave empty to skip budget alert."
  default     = ""
}

variable "monthly_budget_usd" {
  type        = number
  description = "Monthly budget threshold in USD. Alerts fire at 50%, 80%, 100%."
  default     = 50
}

# --- Storage lifecycle ---

variable "bronze_retention_days" {
  type        = number
  description = "Days after which raw bronze JSON objects are deleted."
  default     = 90
}

variable "bronze_nearline_transition_days" {
  type        = number
  description = "Days after which bronze objects transition to NEARLINE storage class."
  default     = 30
}

variable "silver_retention_days" {
  type        = number
  description = "Days after which silver Parquet objects are deleted."
  default     = 180
}

# --- Cloud Run (only used when enable_cloudrun = true) ---

variable "cloudrun_image" {
  type        = string
  description = "Container image URI for the recommender API. Placeholder; set when api/ image is built."
  default     = "us-docker.pkg.dev/cloudrun/container/hello"
}

# --- Composer (only used when enable_composer = true) ---

variable "composer_image_version" {
  type        = string
  description = "Composer 2 image version. Check: gcloud composer images list --region=us-central1"
  default     = "composer-2.6.6-airflow-2.7.3"
}
