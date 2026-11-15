# Key Vault: the system of record for source-system credentials. Access is
# through Azure RBAC only; Terraform never reads or writes a secret value, so
# none ends up in state.

resource "azurerm_key_vault" "this" {
  # checkov:skip=CKV2_AZURE_32: No private endpoint: this stack creates no virtual network to put one in. var.key_vault_firewall restricts the data plane to listed networks.
  name                = local.names.key_vault
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name            = "standard"
  tags                = local.tags

  rbac_authorization_enabled = true
  purge_protection_enabled   = true
  soft_delete_retention_days = 90

  network_acls {
    default_action             = var.key_vault_firewall == null ? "Allow" : "Deny"
    bypass                     = "None"
    ip_rules                   = try(var.key_vault_firewall.allowed_ip_ranges, [])
    virtual_network_subnet_ids = try(var.key_vault_firewall.allowed_subnet_ids, [])
  }
}

resource "azurerm_role_assignment" "key_vault_secrets_officer" {
  for_each = toset(var.key_vault_secrets_officer_principal_ids)

  scope                = azurerm_key_vault.this.id
  role_definition_name = "Key Vault Secrets Officer"
  principal_id         = each.value
  description          = "Maintains source-system credentials for the ${local.name_stem} environment."
}
