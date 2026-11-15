# The access model. One authoritative databricks_grants resource per securable:
# Terraform owns the complete grant list, so a privilege granted by hand in the
# UI is removed on the next apply and the table in the README stays true.
#
# | Principal        | Catalog        | Schemas                                           |
# |------------------|----------------|---------------------------------------------------|
# | platform-admins  | ALL_PRIVILEGES | inherited                                         |
# | runner           | USE_CATALOG    | read/write on all                                 |
# | data-engineers   | USE_CATALOG    | read/write on all (read only when read_only)      |
# | data-analysts    | USE_CATALOG    | read on gold                                      |
# | deployer         | none           | none: it deploys jobs and never touches data      |

locals {
  # Grant principals: groups by display name, service principals by
  # application ID. The deployer is deliberately absent.
  principals = {
    platform_admins = local.group_display_names["platform_admins"]
    data_engineers  = local.group_display_names["data_engineers"]
    data_analysts   = local.group_display_names["data_analysts"]
    runner          = databricks_service_principal.this["runner"].application_id
  }

  schema_read       = ["USE_SCHEMA", "SELECT"]
  schema_read_write = ["USE_SCHEMA", "SELECT", "MODIFY", "CREATE_TABLE"]

  catalog_grants = {
    platform_admins = ["ALL_PRIVILEGES"]
    runner          = ["USE_CATALOG"]
    data_engineers  = ["USE_CATALOG"]
    data_analysts   = ["USE_CATALOG"]
  }

  # schema => role => privileges. Roles with nothing to grant on a schema are
  # dropped, so analysts only appear on gold.
  schema_grants = {
    for schema in keys(local.schemas) : schema => {
      for role, privileges in {
        runner         = local.schema_read_write
        data_engineers = local.engineers_read_write ? local.schema_read_write : local.schema_read
        data_analysts  = schema == "gold" ? local.schema_read : []
      } : role => privileges if length(privileges) > 0
    }
  }

  # Reading raw files by path needs READ_FILES on the landing location. The
  # managed location gets no grants: it is reached through tables only.
  landing_grants = {
    for role, privileges in {
      runner         = ["READ_FILES"]
      data_engineers = local.engineers_read_write ? ["READ_FILES"] : []
    } : role => privileges if length(privileges) > 0
  }
}

resource "databricks_grants" "catalog" {
  catalog = databricks_catalog.this.name

  dynamic "grant" {
    for_each = local.catalog_grants

    content {
      principal  = local.principals[grant.key]
      privileges = grant.value
    }
  }
}

resource "databricks_grants" "schema" {
  for_each = local.schema_grants

  schema = databricks_schema.this[each.key].id

  dynamic "grant" {
    for_each = each.value

    content {
      principal  = local.principals[grant.key]
      privileges = grant.value
    }
  }
}

resource "databricks_grants" "landing" {
  external_location = databricks_external_location.this["landing"].id

  dynamic "grant" {
    for_each = local.landing_grants

    content {
      principal  = local.principals[grant.key]
      privileges = grant.value
    }
  }
}
