provider "google" {
  project = "databricks-lab-dev"
  region  = "europe-west1"
}

provider "databricks" {
  profile = "DEV"
}