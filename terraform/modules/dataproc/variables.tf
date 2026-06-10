variable "project_id" {
  type = string
}

variable "region" {
  type = string
}

variable "environment" {
  type = string
}

variable "staging_bucket" {
  type        = string
  description = "GCS bucket name for Dataproc temp files (job JARs, staging artifacts)"
}

variable "spark_sa_email" {
  type        = string
  description = "Email of the Spark / Dataproc service account"
}
