# Tests of the access model. Both provider configurations are mocked, so this
# runs without credentials and creates nothing:
#
#   terraform init -backend=false
#   terraform test

mock_provider "databricks" {}

mock_provider "databricks" {
  alias = "account"

  # IDs must be numeric strings: workspace assignment takes them as numbers.
  mock_resource "databricks_group" {
    defaults = {
      id = "1001"
    }
  }

  mock_data "databricks_group" {
    defaults = {
      id = "1001"
    }
  }

  mock_resource "databricks_service_principal" {
    defaults = {
      id               = "2001"
      application_id   = "11111111-2222-3333-4444-555555555555"
      acl_principal_id = "servicePrincipals/11111111-2222-3333-4444-555555555555"
    }
  }
}

variables {
  databricks_account_id = "00000000-0000-0000-0000-000000000000"
  environment           = "dev"
  platform_environment  = "dev"
  workspace_url         = "https://adb-1234567890123456.7.azuredatabricks.net"
  workspace_id          = "1234567890123456"
  workspace_resource_id = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg-etl-dev/providers/Microsoft.Databricks/workspaces/dbw-etl-dev"
  access_connector_id   = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg-etl-dev/providers/Microsoft.Databricks/accessConnectors/dbac-etl-dev"
  managed_storage_url   = "abfss://unity-catalog@stetldevd1x7.dfs.core.windows.net"
  landing_storage_url   = "abfss://landing@stetldevd1x7.dfs.core.windows.net"
  github_organization   = "example-org"
  github_repository     = "example-repo"
}

run "non_production" {
  command = apply

  variables {
    data_engineer_access = "read_write"
  }

  assert {
    condition     = databricks_catalog.this.name == "etl_dev"
    error_message = "The catalog must be named <prefix>_<environment>."
  }

  assert {
    condition     = toset(keys(databricks_schema.this)) == toset(["bronze", "silver", "gold", "quarantine", "etl_audit", "etl_state"])
    error_message = "The catalog must contain exactly the six framework schemas."
  }

  # Catalog: admins, runner, engineers, analysts. No fifth principal, so the
  # deployer has nothing.
  assert {
    condition     = length(databricks_grants.catalog.grant) == 4
    error_message = "Exactly four principals may hold catalog privileges."
  }

  assert {
    condition     = one([for g in databricks_grants.catalog.grant : g.privileges if g.principal == "etl-platform-admins"]) == toset(["ALL_PRIVILEGES"])
    error_message = "platform-admins must hold ALL_PRIVILEGES on the catalog."
  }

  assert {
    condition = alltrue([
      for g in databricks_grants.catalog.grant :
      g.privileges == toset(["USE_CATALOG"]) if g.principal != "etl-platform-admins"
    ])
    error_message = "Everyone except platform-admins must hold USE_CATALOG only on the catalog."
  }

  # Runner: read/write on every schema.
  assert {
    condition = alltrue([
      for schema, grants in databricks_grants.schema :
      one([for g in grants.grant : g.privileges if g.principal == "11111111-2222-3333-4444-555555555555"]) == toset(["USE_SCHEMA", "SELECT", "MODIFY", "CREATE_TABLE"])
    ])
    error_message = "The runner must hold USE_SCHEMA, SELECT, MODIFY and CREATE_TABLE on every schema."
  }

  # Engineers: read/write on every schema outside production.
  assert {
    condition = alltrue([
      for schema, grants in databricks_grants.schema :
      one([for g in grants.grant : g.privileges if g.principal == "etl-data-engineers"]) == toset(["USE_SCHEMA", "SELECT", "MODIFY", "CREATE_TABLE"])
    ])
    error_message = "With read_write, data-engineers must hold read/write privileges on every schema."
  }

  # Analysts: gold only.
  assert {
    condition = [
      for schema, grants in databricks_grants.schema :
      schema if contains([for g in grants.grant : g.principal], "etl-data-analysts")
    ] == ["gold"]
    error_message = "data-analysts must appear on the gold schema only."
  }

  assert {
    condition     = one([for g in databricks_grants.schema["gold"].grant : g.privileges if g.principal == "etl-data-analysts"]) == toset(["USE_SCHEMA", "SELECT"])
    error_message = "data-analysts must hold USE_SCHEMA and SELECT on gold."
  }

  assert {
    condition     = toset([for g in databricks_grants.landing.grant : g.principal]) == toset(["11111111-2222-3333-4444-555555555555", "etl-data-engineers"])
    error_message = "With read_write, the runner and data-engineers may read landing files."
  }

  assert {
    condition = tomap({ for role, acl in databricks_secret_acl.sources : role => acl.permission }) == tomap({
      platform_admins = "MANAGE"
      runner          = "READ"
      data_engineers  = "READ"
    })
    error_message = "Secret scope ACLs outside production: admins MANAGE, runner READ, engineers READ."
  }

  # Entitlements are explicit; analysts get SQL access only.
  assert {
    condition     = databricks_account_iam_workspace_assignment_v2.this["data_analysts"].entitlements == toset(["DATABRICKS_SQL_ACCESS"])
    error_message = "data-analysts must get the Databricks SQL access entitlement only."
  }

  assert {
    condition     = databricks_mws_permission_assignment.platform_admins.permissions == toset(["ADMIN"])
    error_message = "platform-admins must be workspace admins."
  }

  # Federation policy defaults: name-only subject, account ID as audience.
  assert {
    condition     = output.github_oidc_subject == "repo:example-org/example-repo:environment:dev"
    error_message = "The default subject must be repo:<organization>/<repository>:environment:<environment>."
  }

  assert {
    condition     = databricks_service_principal_federation_policy.deployer_github.oidc_policy.audiences == tolist(["00000000-0000-0000-0000-000000000000"])
    error_message = "The default audience must be the Databricks account ID."
  }

  assert {
    condition     = length(databricks_sql_endpoint.analytics) == 0
    error_message = "The SQL warehouse must be off by default."
  }
}

run "production" {
  command = apply

  variables {
    environment          = "prod"
    platform_environment = "prod"
    # data_engineer_access is left at its default on purpose.
  }

  assert {
    condition = alltrue([
      for schema, grants in databricks_grants.schema :
      one([for g in grants.grant : g.privileges if g.principal == "etl-data-engineers"]) == toset(["USE_SCHEMA", "SELECT"])
    ])
    error_message = "By default data-engineers must be read-only on every schema, including etl_audit."
  }

  assert {
    condition     = toset([for g in databricks_grants.landing.grant : g.principal]) == toset(["11111111-2222-3333-4444-555555555555"])
    error_message = "In production only the runner may read landing files."
  }

  assert {
    condition = tomap({ for role, acl in databricks_secret_acl.sources : role => acl.permission }) == tomap({
      platform_admins = "MANAGE"
      runner          = "READ"
    })
    error_message = "In production data-engineers must have no access to the secret scope."
  }

  assert {
    condition     = databricks_service_principal.this["runner"].display_name == "etl-runner-prod" && databricks_service_principal.this["deployer"].display_name == "etl-deployer-prod"
    error_message = "Service principals must be named <prefix>-<role>-<environment>."
  }
}

run "existing_groups_and_explicit_federation_claims" {
  command = apply

  variables {
    create_groups = false
    group_name_overrides = {
      data_analysts = "sg-data-analysts"
    }
    github_oidc_subject   = "repo:example-org@1234567/example-repo@7654321:environment:dev"
    github_oidc_audiences = ["https://github.com/example-org"]
    enable_sql_warehouse  = true
  }

  assert {
    condition     = length(databricks_group.this) == 0 && length(data.databricks_group.existing) == 3
    error_message = "With create_groups = false no group may be created; all three are looked up."
  }

  assert {
    condition     = data.databricks_group.existing["data_analysts"].display_name == "sg-data-analysts" && data.databricks_group.existing["data_engineers"].display_name == "etl-data-engineers"
    error_message = "An override replaces one group name and leaves the others at their defaults."
  }

  assert {
    condition     = output.github_oidc_subject == "repo:example-org@1234567/example-repo@7654321:environment:dev"
    error_message = "An explicit subject must be used verbatim."
  }

  assert {
    condition     = databricks_service_principal_federation_policy.deployer_github.oidc_policy.audiences == tolist(["https://github.com/example-org"])
    error_message = "Explicit audiences must replace the account ID."
  }

  assert {
    condition     = toset([for acl in databricks_permissions.sql_warehouse[0].access_control : acl.permission_level]) == toset(["CAN_USE"])
    error_message = "Warehouse grants must be CAN_USE only."
  }
}

run "rejects_platform_inputs_of_another_environment" {
  command = plan

  variables {
    environment          = "prod"
    platform_environment = "dev"
  }

  expect_failures = [var.platform_environment]
}

run "rejects_unknown_engineer_access_level" {
  command = plan

  variables {
    data_engineer_access = "admin"
  }

  expect_failures = [var.data_engineer_access]
}

run "rejects_subject_not_bound_to_an_environment" {
  command = plan

  variables {
    github_oidc_subject = "repo:example-org/example-repo:ref:refs/heads/main"
  }

  expect_failures = [var.github_oidc_subject]
}
