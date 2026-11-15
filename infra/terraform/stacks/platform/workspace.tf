# Azure Databricks workspace. Premium is required for Unity Catalog, access
# control and Private Link.

resource "azurerm_databricks_workspace" "this" {
  # checkov:skip=CKV_AZURE_158: Public access is the default because Private Link needs a VNet-injected workspace and private endpoints that this stack does not create. Set workspace_public_network_access_enabled = false together with workspace_vnet_injection.
  # checkov:skip=CKV2_AZURE_48: The DBFS root is encrypted with Microsoft-managed keys. Table data is not stored there: the catalog's managed storage is the data lake account.
  name                        = local.names.workspace
  resource_group_name         = azurerm_resource_group.this.name
  location                    = azurerm_resource_group.this.location
  sku                         = "premium"
  managed_resource_group_name = local.names.workspace_managed_data
  tags                        = local.tags

  public_network_access_enabled         = var.workspace_public_network_access_enabled
  network_security_group_rules_required = try(var.workspace_vnet_injection.required_nsg_rules, null)

  custom_parameters {
    # Secure cluster connectivity: cluster nodes get no public IP addresses.
    no_public_ip = true

    # Only set for a VNet-injected workspace; null leaves the network managed
    # by Azure Databricks.
    virtual_network_id                                   = try(var.workspace_vnet_injection.virtual_network_id, null)
    public_subnet_name                                   = try(var.workspace_vnet_injection.public_subnet_name, null)
    private_subnet_name                                  = try(var.workspace_vnet_injection.private_subnet_name, null)
    public_subnet_network_security_group_association_id  = try(var.workspace_vnet_injection.public_subnet_nsg_association_id, null)
    private_subnet_network_security_group_association_id = try(var.workspace_vnet_injection.private_subnet_nsg_association_id, null)
  }
}
