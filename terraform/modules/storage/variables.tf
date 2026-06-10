variable "project_id" {
  type = string
}

variable "region" {
  type = string
}

variable "environment" {
  type = string
}

variable "bronze_bucket" {
  type        = string
  description = "Name for the bronze (raw JSON) GCS bucket"
}

variable "silver_bucket" {
  type        = string
  description = "Name for the silver (Parquet) GCS bucket"
}

variable "bronze_retention_days" {
  type        = number
  description = "Days before bronze objects are deleted"
  default     = 90
}

variable "bronze_nearline_transition_days" {
  type        = number
  description = "Days before bronze objects transition to NEARLINE"
  default     = 30
}

variable "silver_retention_days" {
  type        = number
  description = "Days before silver objects are deleted"
  default     = 180
}
