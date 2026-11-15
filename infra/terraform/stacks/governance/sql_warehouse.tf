# Optional serverless SQL warehouse, off by default. Analysts can read gold
# without it only through a warehouse someone else provides.

resource "databricks_sql_endpoint" "analytics" {
  count = var.enable_sql_warehouse ? 1 : 0

  name                      = "${var.prefix}-${var.environment}-analytics"
  cluster_size              = var.sql_warehouse_cluster_size
  warehouse_type            = "PRO"
  enable_serverless_compute = true
  auto_stop_mins            = var.sql_warehouse_auto_stop_minutes
  min_num_clusters          = 1
  max_num_clusters          = 1
}

# Authoritative for the warehouse: these two groups may run queries on it,
# workspace admins manage it, nobody else can use it.
resource "databricks_permissions" "sql_warehouse" {
  count = var.enable_sql_warehouse ? 1 : 0

  sql_endpoint_id = databricks_sql_endpoint.analytics[0].id

  access_control {
    group_name       = local.principals["data_analysts"]
    permission_level = "CAN_USE"
  }

  access_control {
    group_name       = local.principals["data_engineers"]
    permission_level = "CAN_USE"
  }

  depends_on = [databricks_account_iam_workspace_assignment_v2.this]
}
