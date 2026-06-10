variable "project_id" {
  type = string
}

variable "region" {
  type = string
}

variable "environment" {
  type = string
}

variable "api_sa_email" {
  type        = string
  description = "Service account email for the Cloud Run service"
}

variable "container_image" {
  type        = string
  description = "Container image URI for the recommender API"
}
