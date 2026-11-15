locals {
  # abfss://<container>@<account>.dfs.<suffix>, built from the endpoint Azure
  # reports rather than a hard-coded DNS suffix.
  storage_urls = {
    for key, container in azurerm_storage_container.this :
    key => "abfss://${container.name}@${azurerm_storage_account.lake.primary_dfs_host}"
  }
}

output "resource_group_name" {
  description = "Name of the resource group that holds the environment."
  value       = azurerm_resource_group.this.name
}

output "storage_account_name" {
  description = "Name of the data lake storage account."
  value       = azurerm_storage_account.lake.name
}

output "storage_account_id" {
  description = "Resource ID of the data lake storage account."
  value       = azurerm_storage_account.lake.id
}

output "managed_storage_url" {
  description = "abfss URL of the container that backs Unity Catalog managed storage."
  value       = local.storage_urls["managed"]
}

output "landing_storage_url" {
  description = "abfss URL of the raw landing zone container."
  value       = local.storage_urls["landing"]
}

output "access_connector_id" {
  description = "Resource ID of the Databricks access connector that Unity Catalog uses as its storage credential."
  value       = azurerm_databricks_access_connector.this.id
}

output "access_connector_principal_id" {
  description = "Object ID of the access connector's system-assigned managed identity."
  value       = azurerm_databricks_access_connector.this.identity[0].principal_id
}

output "workspace_url" {
  description = "URL of the Databricks workspace, including the https:// scheme."
  value       = "https://${azurerm_databricks_workspace.this.workspace_url}"
}

output "workspace_id" {
  description = "Numeric ID of the workspace in the Databricks control plane."
  value       = azurerm_databricks_workspace.this.workspace_id
}

output "workspace_resource_id" {
  description = "Azure resource ID of the Databricks workspace."
  value       = azurerm_databricks_workspace.this.id
}

output "key_vault_name" {
  description = "Name of the Key Vault that holds source-system credentials."
  value       = azurerm_key_vault.this.name
}

output "key_vault_uri" {
  description = "Data-plane URI of the Key Vault."
  value       = azurerm_key_vault.this.vault_uri
}

output "governance_inputs" {
  description = "Everything the governance stack needs from this stack, shaped as a tfvars object. Write it with: terraform output -json governance_inputs > ../governance/environments/<env>.platform.tfvars.json"
  value = {
    platform_environment  = var.environment
    workspace_url         = "https://${azurerm_databricks_workspace.this.workspace_url}"
    workspace_id          = azurerm_databricks_workspace.this.workspace_id
    workspace_resource_id = azurerm_databricks_workspace.this.id
    access_connector_id   = azurerm_databricks_access_connector.this.id
    managed_storage_url   = local.storage_urls["managed"]
    landing_storage_url   = local.storage_urls["landing"]
  }
}
