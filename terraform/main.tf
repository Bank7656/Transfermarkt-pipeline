terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.25.0"
    }
  }
}

provider "google" {
  # credentials = file(var.credentials)
  project = var.project_id
  region  = var.region
}

resource "google_storage_bucket" "football_bucket" {
  name          = var.bucket_name
  location      = var.location
  force_destroy = true
  storage_class               = var.storage_class
  uniform_bucket_level_access = true
  lifecycle_rule {
    condition {
      age = 365
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      age = 365
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}