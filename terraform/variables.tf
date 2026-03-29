variable "project_id" {
    description = "Project ID"
    type=string
    default = "transfermarkt-pipeline"
}

variable "credentials" {
  type = string
  default = "../secrets/google_credentials.json"
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

variable "bucket_name" {
    description = "The Google cloud storage bucket name"
    type=string
    default = "football-data-storage-6532"
}

variable "storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}