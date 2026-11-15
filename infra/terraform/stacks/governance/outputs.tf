# Outputs wire the Databricks Asset Bundle and the GitHub environments. None of
# them is sensitive: there is no secret anywhere in this stack.

output "workspace_url" {
  description = "URL of the Databricks workspace. DATABRICKS_HOST for the CLI and the bundle."
  value       = var.workspace_url
}

output "catalog_name" {
  description = "Name of the environment's Unity Catalog catalog."
  value       = databricks_catalog.this.name
}

output "schema_names" {
  description = "Schemas created in the catalog."
  value       = sort([for schema in databricks_schema.this : schema.name])
}

output "runner_application_id" {
  description = "Application ID of the service principal jobs run as. Use it as run_as.service_principal_name in the bundle."
  value       = databricks_service_principal.this["runner"].application_id
}

output "deployer_application_id" {
  description = "Application ID of the CI/CD service principal. DATABRICKS_CLIENT_ID for GitHub Actions OIDC authentication."
  value       = databricks_service_principal.this["deployer"].application_id
}

output "group_names" {
  description = "Display names of the role groups, keyed by role (platform_admins, data_engineers, data_analysts)."
  value       = local.group_display_names
}

output "secret_scope_name" {
  description = "Name of the Databricks secret scope for source-system credentials."
  value       = databricks_secret_scope.sources.name
}

output "github_oidc_subject" {
  description = "The sub claim the deployer's federation policy trusts. A GitHub Actions job must present exactly this subject."
  value       = databricks_service_principal_federation_policy.deployer_github.oidc_policy.subject
}

output "sql_warehouse_id" {
  description = "ID of the analytics SQL warehouse, or null when enable_sql_warehouse is false."
  value       = one(databricks_sql_endpoint.analytics[*].id)
}
