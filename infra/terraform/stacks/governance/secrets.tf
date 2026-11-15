# Secret scope for source-system credentials. The framework resolves
# ${secret:<scope>/<key>} references against it at run time.
#
# Databricks-backed on purpose: a Key Vault-backed scope requires the vault to
# use access policies, and the platform vault uses Azure RBAC. Secret values are
# copied from Key Vault by an operator or pipeline, never by Terraform, so they
# stay out of state (see the README).

resource "databricks_secret_scope" "sources" {
  name = var.secret_scope_name
}

locals {
  # ACLs apply to the whole scope: a principal that can read one secret can
  # read all of them. The deployer gets none.
  secret_acls = {
    for role, permission in {
      platform_admins = "MANAGE"
      runner          = "READ"
      data_engineers  = local.engineers_read_write ? "READ" : null
    } : role => permission if permission != null
  }
}

resource "databricks_secret_acl" "sources" {
  for_each = local.secret_acls

  scope      = databricks_secret_scope.sources.name
  principal  = local.principals[each.key]
  permission = each.value

  # ACLs are a workspace API: the principal must be in the workspace first.
  depends_on = [
    databricks_mws_permission_assignment.platform_admins,
    databricks_account_iam_workspace_assignment_v2.this,
  ]
}
