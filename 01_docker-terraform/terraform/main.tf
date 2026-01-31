terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  project     = "nodal-nirvana-485604-k6"
  region      = "europe-west8"
}

resource "google_storage_bucket" "demo-bucket" {
  name          = "terraform-485604-demo-bucket"
  location      = "EU"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 3
    }
    action {
      type = "Delete"
    }
  }
}