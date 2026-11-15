# Identities: who exists, who may enter the workspace and with which
# entitlements. Everything here is account level, so every resource names the
# account provider explicitly.

# ---------------------------------------------------------------------------
# Role groups: created here, or looked up when an identity provider owns them
# ---------------------------------------------------------------------------

resource "databricks_group" "this" {
  provider = databricks.account
  for_each = var.create_groups ? local.group_names : {}

  display_name = each.value
}

data "databricks_group" "existing" {
  provider = databricks.account
  for_each = var.create_groups ? {} : local.group_names

  display_name = each.value
  recursive    = false
}

locals {
  group_ids = {
    for role in keys(local.group_names) :
    role => var.create_groups ? databricks_group.this[role].id : data.databricks_group.existing[role].id
  }

  # Read back from the resource or data source rather than from the variable,
  # so grants and ACLs that use a group name wait for the group to exist.
  group_display_names = {
    for role in keys(local.group_names) :
    role => var.create_groups ? databricks_group.this[role].display_name : data.databricks_group.existing[role].display_name
  }
}

# ---------------------------------------------------------------------------
# Service principals: Databricks-managed, so they hold no Entra ID secret
# ---------------------------------------------------------------------------

resource "databricks_service_principal" "this" {
  provider = databricks.account
  for_each = local.service_principal_names

  display_name = each.value

  # Account-level service principals are only deactivated on destroy unless
  # this is false. Deleting keeps a later re-apply from leaving two principals
  # with the same display name.
  disable_as_user_deletion = false
}

# ---------------------------------------------------------------------------
# Workspace assignment
# ---------------------------------------------------------------------------

# Workspace admins. The ADMIN permission adds the group to the workspace
# admins group, which carries every entitlement.
resource "databricks_mws_permission_assignment" "platform_admins" {
  provider = databricks.account

  workspace_id = var.workspace_id
  principal_id = local.group_ids["platform_admins"]
  permissions  = ["ADMIN"]
}

locals {
  # Everyone else gets exactly the entitlements listed here. Assigning without
  # an explicit list would grant the workspace's legacy default set (workspace
  # access and Databricks SQL access) to every principal.
  workspace_assignments = {
    data_engineers = {
      principal_id = local.group_ids["data_engineers"]
      entitlements = ["WORKSPACE_ACCESS", "DATABRICKS_SQL_ACCESS"]
    }
    data_analysts = {
      principal_id = local.group_ids["data_analysts"]
      entitlements = ["DATABRICKS_SQL_ACCESS"]
    }
    runner = {
      principal_id = databricks_service_principal.this["runner"].id
      entitlements = ["WORKSPACE_ACCESS"]
    }
    deployer = {
      principal_id = databricks_service_principal.this["deployer"].id
      entitlements = ["WORKSPACE_ACCESS"]
    }
  }
}

resource "databricks_account_iam_workspace_assignment_v2" "this" {
  provider = databricks.account
  for_each = local.workspace_assignments

  parent       = "accounts/${var.databricks_account_id}/workspaces/${var.workspace_id}"
  principal_id = each.value.principal_id
  entitlements = each.value.entitlements
}

# ---------------------------------------------------------------------------
# CI/CD: the deployer may deploy jobs that run as the runner, and signs in from
# GitHub Actions without a stored secret
# ---------------------------------------------------------------------------

# Authoritative for the runner's rule set: the deployer is the only principal
# with the Service Principal User role on it.
resource "databricks_access_control_rule_set" "runner" {
  provider = databricks.account

  name = "accounts/${var.databricks_account_id}/servicePrincipals/${databricks_service_principal.this["runner"].application_id}/ruleSets/default"

  grant_rules {
    principals = [databricks_service_principal.this["deployer"].acl_principal_id]
    role       = "roles/servicePrincipal.user"
  }
}

# Workload identity federation: Databricks exchanges a GitHub Actions OIDC
# token for a Databricks token when issuer, audience and subject all match.
# The subject pins one repository and one GitHub environment, so the
# environment's protection rules (required reviewers, branch restrictions)
# gate who can deploy.
resource "databricks_service_principal_federation_policy" "deployer_github" {
  provider = databricks.account

  service_principal_id = databricks_service_principal.this["deployer"].id
  policy_id            = "github-actions-${var.environment}"
  description          = "GitHub Actions deployments to ${var.environment}. Trusted subject: ${local.github_oidc_subject}"

  oidc_policy = {
    issuer        = "https://token.actions.githubusercontent.com"
    audiences     = local.github_oidc_audiences
    subject       = local.github_oidc_subject
    subject_claim = "sub"
  }
}
