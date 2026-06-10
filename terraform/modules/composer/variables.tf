variable "project_id" {
  type = string
}

variable "region" {
  type = string
}

variable "environment" {
  type = string
}

variable "composer_sa_email" {
  type        = string
  description = "Service account email for the Composer environment"
}

variable "composer_image_version" {
  type        = string
  description = "Composer 2 image version (e.g. composer-2.6.6-airflow-2.7.3)"
}
