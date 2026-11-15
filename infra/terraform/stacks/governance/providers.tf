# Two configurations of the same provider. Neither holds a credential:
# authentication comes from the environment (az login on a workstation,
# ARM_* variables or OIDC in a pipeline).

# Workspace level: Unity Catalog objects, grants, secret scope, SQL warehouse.
# This is the default configuration, so resources without a provider argument
# use it. Do not add account_id here; the provider rejects it at workspace level.
provider "databricks" {
  host                        = var.workspace_url
  azure_workspace_resource_id = var.workspace_resource_id
}

# Account level: groups, service principals, workspace assignment, service
# principal roles and the OIDC federation policy.
provider "databricks" {
  alias      = "account"
  host       = var.databricks_account_host
  account_id = var.databricks_account_id
}
