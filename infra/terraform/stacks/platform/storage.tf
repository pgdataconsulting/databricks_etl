# Data lake: ADLS Gen2 account with one container for Unity Catalog managed
# storage and one for the raw landing zone. Unity Catalog reaches it through
# the managed identity of the access connector; nothing uses account keys.

resource "azurerm_databricks_access_connector" "this" {
  name                = local.names.access_connector
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  tags                = local.tags

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_storage_account" "lake" {
  # checkov:skip=CKV_AZURE_59: Databricks compute reaches the lake over its public endpoint unless the workspace is VNet-injected, and this stack creates no network. Authorization is Entra ID only; var.storage_firewall restricts networks.
  # checkov:skip=CKV2_AZURE_33: No private endpoint, for the same reason: this stack creates no virtual network to put one in.
  # checkov:skip=CKV_AZURE_36: Deliberate. The trusted-services bypass would let every access connector in the tenant through the firewall; only this stack's connector is allowed, as a resource instance.
  # checkov:skip=CKV2_AZURE_1: Encrypted at rest with Microsoft-managed keys. Customer-managed keys need a key hierarchy this stack does not own.
  # checkov:skip=CKV_AZURE_33: The account has no queues, and queue logging is no longer an argument of this resource in azurerm 5.
  name                = local.names.storage_account
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  tags                = local.tags

  account_kind             = "StorageV2"
  account_tier             = "Standard"
  account_replication_type = var.storage_replication_type
  is_hns_enabled           = true

  # Transport and authorization: TLS 1.2, HTTPS only, no anonymous access, and
  # Microsoft Entra ID as the only way to authorize a request.
  min_tls_version                 = "TLS1_2"
  https_traffic_only_enabled      = true
  allow_nested_items_to_be_public = false
  shared_access_key_enabled       = false
  default_to_oauth_authentication = true
  local_user_enabled              = false

  blob_properties {
    # Blob versioning is not available with a hierarchical namespace, so soft
    # delete is the recovery mechanism for files and containers.
    delete_retention_policy {
      days = var.storage_soft_delete_retention_days
    }

    container_delete_retention_policy {
      days = var.storage_soft_delete_retention_days
    }
  }

  network_rules {
    default_action             = var.storage_firewall == null ? "Allow" : "Deny"
    bypass                     = ["None"]
    ip_rules                   = try(var.storage_firewall.allowed_ip_ranges, [])
    virtual_network_subnet_ids = try(var.storage_firewall.allowed_subnet_ids, [])

    # Lets Unity Catalog through the firewall as this specific access connector
    # instead of trusting every Azure service.
    private_link_access {
      endpoint_resource_id = azurerm_databricks_access_connector.this.id
    }
  }
}

resource "azurerm_storage_container" "this" {
  # checkov:skip=CKV2_AZURE_21: Blob read logging needs a Log Analytics workspace or a log storage account, which this stack does not create.
  for_each = local.containers

  name                  = each.value
  storage_account_id    = azurerm_storage_account.lake.id
  container_access_type = "private"
}

# The one role Unity Catalog needs to read and write managed tables and landing
# files. Scoped to this storage account, not the resource group.
resource "azurerm_role_assignment" "access_connector_blob_data" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.this.identity[0].principal_id
  principal_type       = "ServicePrincipal"
  description          = "Unity Catalog access to the ${local.name_stem} data lake through the Databricks access connector."
}
