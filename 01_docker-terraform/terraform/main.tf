terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  credentials = "./keys/nodal-nirvana-485604-k6-1f7b1a5f5f8d.json"
  project     = "nodal-nirvana-485604-k6"
  region      = "us-central1"
}