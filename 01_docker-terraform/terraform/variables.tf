variable "credentials" {
  description = "GCP Credentials File Path"
  default     = "/mnt/d/DEV/DEZoocamp2026/01_docker-terraform/terraform/keys/nodal-nirvana-485604-k6-1b5c8f98c203.json"
  #ex: if you have a directory where this file is called keys with your service account json file
  #saved there as my-creds.json you could use default = "./keys/my-creds.json"
}


variable "project" {
  description = "Project"
  default     = "nodal-nirvana-485604-k6"
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default     = "europe-west8"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default     = "EU"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  #Update the below to what you want your dataset to be called
  default     = "DEZoocamp_demo_dataset"
}

variable "gcs_bucket_name" {
  description = "DEZooCamp Demo bucket Name"
  #Update the below to a unique bucket name
  default     = "terraform-demo-485604-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}