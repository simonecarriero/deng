variable "project" {
  description = "Project"
}

variable "credentials" {
  description = "My Credentials"
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "gcs_bucket_name" {
  description = "NYC taxi data"
  default     = "deng-nyc-taxi-data"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

variable "bq_dataset_name" {
  description = "NYC taxi data"
  default     = "deng-nyc-taxi-data"
}
