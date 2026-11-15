# Unity Catalog objects for one environment: how storage is reached (storage
# credential, external locations) and where data lives (catalog, schemas).
# Workspace-level provider; the identity that applies this stack becomes the
# owner of every object.

resource "databricks_storage_credential" "this" {
  name           = "${local.catalog_name}_access_connector"
  comment        = "Managed identity of the ${var.environment} Databricks access connector. Managed by Terraform."
  isolation_mode = local.storage_isolation_mode
  force_destroy  = var.force_destroy

  azure_managed_identity {
    access_connector_id = var.access_connector_id
  }
}

locals {
  external_locations = {
    managed = {
      url     = var.managed_storage_url
      comment = "Managed storage of the ${local.catalog_name} catalog. Managed by Terraform."
    }
    landing = {
      url     = var.landing_storage_url
      comment = "Raw landing zone for files delivered by source systems. Managed by Terraform."
    }
  }
}

resource "databricks_external_location" "this" {
  for_each = local.external_locations

  name            = "${local.catalog_name}_${each.key}"
  url             = each.value.url
  credential_name = databricks_storage_credential.this.name
  comment         = each.value.comment
  isolation_mode  = local.storage_isolation_mode
  force_destroy   = var.force_destroy
}

resource "databricks_catalog" "this" {
  name    = local.catalog_name
  comment = "Data of the ${var.environment} environment of the ETL framework. Managed by Terraform."

  # Managed tables are stored under the environment's own container instead of
  # the metastore root, which keeps environments physically separate.
  storage_root   = databricks_external_location.this["managed"].url
  isolation_mode = local.catalog_isolation_mode
  force_destroy  = var.force_destroy
}

resource "databricks_schema" "this" {
  for_each = local.schemas

  catalog_name  = databricks_catalog.this.name
  name          = each.key
  comment       = each.value
  force_destroy = var.force_destroy
}
