locals {
  name_stem = "${var.prefix}-${var.environment}"

  # Storage account and Key Vault names are global, hence the suffix. The
  # validation rules on prefix, environment and name_suffix keep both within
  # Azure's limits (24 characters; storage accounts allow no hyphens).
  names = {
    resource_group         = "rg-${local.name_stem}"
    storage_account        = "st${var.prefix}${var.environment}${var.name_suffix}"
    key_vault              = "kv-${local.name_stem}-${var.name_suffix}"
    access_connector       = "dbac-${local.name_stem}"
    workspace              = "dbw-${local.name_stem}"
    workspace_managed_data = "rg-${local.name_stem}-dbw-managed"
  }

  # Containers in the data lake. "managed" backs the Unity Catalog catalog
  # (managed tables); "landing" receives raw files from source systems.
  containers = {
    managed = "unity-catalog"
    landing = "landing"
  }

  tags = merge(var.tags, {
    project     = var.project
    environment = var.environment
    managed_by  = "terraform"
  })

  # Providers of the resources in this stack, plus the ones a Databricks
  # workspace deployment needs registered in the subscription.
  required_resource_providers = [
    "Microsoft.Compute",
    "Microsoft.Databricks",
    "Microsoft.KeyVault",
    "Microsoft.ManagedIdentity",
    "Microsoft.Network",
    "Microsoft.Storage",
  ]
}
