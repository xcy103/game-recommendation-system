variable "project_id" {
  type = string
}

variable "environment" {
  type = string
}

variable "bronze_bucket_name" {
  type        = string
  description = "Bronze bucket name (for IAM bindings)"
}

variable "silver_bucket_name" {
  type        = string
  description = "Silver bucket name (for IAM bindings)"
}

variable "staging_dataset_id" {
  type        = string
  description = "BigQuery staging dataset ID"
}

variable "gold_dataset_id" {
  type        = string
  description = "BigQuery gold dataset ID"
}
