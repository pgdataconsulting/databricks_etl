provider "azurerm" {
  subscription_id = var.subscription_id

  # Shared Key access is disabled on the data lake, so any data-plane call the
  # provider makes must use Microsoft Entra ID.
  storage_use_azuread = true

  # azurerm 5 registers no resource providers unless told to. Register only the
  # ones this stack and the Databricks workspace deployment need.
  resource_providers_to_register = var.register_resource_providers ? local.required_resource_providers : []

  features {
    key_vault {
      # Purge protection is enabled on the vault, so a purge on destroy would
      # fail. Destroy soft-deletes; re-apply recovers the vault.
      purge_soft_delete_on_destroy    = false
      recover_soft_deleted_key_vaults = true
    }

    resource_group {
      prevent_deletion_if_contains_resources = true
    }
  }
}
