terraform {
  backend "gcs" {
    bucket = "steam-reviews-platform-tfstate"
    prefix = "terraform/state"
  }
}
