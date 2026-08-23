# Dossier géré entièrement par Terraform dans le Workspace Databricks.
resource "databricks_directory" "terraform_demo" {
  path = "/Shared/terraform-demo"
}