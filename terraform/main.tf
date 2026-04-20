terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.25.0"
    }
  }
}

provider "google" {
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

# BigQuery Dataset for the Data Warehouse
resource "google_bigquery_dataset" "dwh_dataset" {
  dataset_id  = "transfermarkt_dwh"
  project     = "transfermarkt-pipeline"
  location    = "asia-southeast3" 
  description = "Data warehouse for Transfermarkt football analytics"
  delete_contents_on_destroy = true 
}