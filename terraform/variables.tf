variable "GCP_PROJECT_ID" {
    description = "Project ID"
    type=string
    default = "transfermarkt-pipeline"
}

variable "GCP_BUCKET_NAME" {
    description = "The Google cloud storage bucket name"
    type=string
    default = "football-data-storage-6532"
}

variable "BQ_DATASET" {
    description = "The Big Query dataset name"
    type=string
    default = "transfermarkt_dwh"
}

variable "region" {
    description = "The Google cloud region"
    type=string
    default = "asia-southeast3"
}

variable "location" {
    description = "Location"
    type=string
    default = "asia-southeast3"
}


variable "storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}