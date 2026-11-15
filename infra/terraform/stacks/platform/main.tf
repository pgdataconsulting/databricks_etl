# Platform stack: the Azure resources one environment of the ETL framework runs
# on. Identities, Unity Catalog objects and grants live in the governance stack;
# jobs and the wheel are deployed by the Databricks Asset Bundle.

data "azurerm_client_config" "current" {}

resource "azurerm_resource_group" "this" {
  name     = local.names.resource_group
  location = var.location
  tags     = local.tags
}
