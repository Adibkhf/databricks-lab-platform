terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.44"
    }

    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.126"
    }
  }
}