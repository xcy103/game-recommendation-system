variable "project_id" {
  type        = string
  description = "GCP project ID"
}

variable "region" {
  type        = string
  description = "GCP region (used for Artifact Registry repository location)"
}

variable "environment" {
  type        = string
  description = "Deployment environment (dev or prod)"
}

variable "github_repo" {
  type        = string
  description = "GitHub repository in org/name format, e.g. xcy103/game-recommendation-system"
}

variable "tfstate_bucket" {
  type        = string
  description = "GCS bucket that holds the Terraform remote state (read access granted to CI SA for terraform plan)"
}
