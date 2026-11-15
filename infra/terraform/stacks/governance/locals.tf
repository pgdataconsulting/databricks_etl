locals {
  catalog_name = "${var.prefix}_${var.environment}"

  # Role groups are per workload, not per environment: the same three groups
  # are assigned to every environment's workspace with different rights.
  group_names = merge(
    {
      platform_admins = "${var.prefix}-platform-admins"
      data_engineers  = "${var.prefix}-data-engineers"
      data_analysts   = "${var.prefix}-data-analysts"
    },
    var.group_name_overrides,
  )

  # Service principals are per environment, so a credential issued for one
  # environment is useless in another.
  service_principal_names = {
    runner   = "${var.prefix}-runner-${var.environment}"
    deployer = "${var.prefix}-deployer-${var.environment}"
  }

  # Schemas of the catalog. etl_audit and etl_state are written by the
  # framework itself (run, step and error audit tables; watermarks).
  schemas = {
    bronze     = "Source data as ingested, before cleansing."
    silver     = "Cleansed and conformed data."
    gold       = "Curated data for reporting and analytics."
    quarantine = "Rows rejected by data quality rules."
    etl_audit  = "Run, step and error audit tables written by the ETL framework."
    etl_state  = "ETL framework state, including incremental watermarks."
  }

  # The single switch behind every difference between production and
  # non-production access for data engineers.
  engineers_read_write = var.data_engineer_access == "read_write"

  # An explicit subject wins over the composed one: Databricks matches the
  # sub claim exactly and GitHub has two formats for it (see the README).
  github_environment = coalesce(var.github_environment, var.environment)
  github_oidc_subject = coalesce(
    var.github_oidc_subject,
    "repo:${var.github_organization}/${var.github_repository}:environment:${local.github_environment}",
  )
  github_oidc_audiences = var.github_oidc_audiences == null ? [var.databricks_account_id] : var.github_oidc_audiences

  # Catalogs and storage objects name the same choice differently.
  catalog_isolation_mode = var.isolate_to_workspace ? "ISOLATED" : "OPEN"
  storage_isolation_mode = var.isolate_to_workspace ? "ISOLATION_MODE_ISOLATED" : "ISOLATION_MODE_OPEN"
}
