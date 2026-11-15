# ---------------------------------------------------------------------------
# Databricks account
# ---------------------------------------------------------------------------

variable "databricks_account_id" {
  description = "ID of the Azure Databricks account (account console, top right corner). Used by the account-level provider and as the audience of the GitHub OIDC federation policy."
  type        = string

  validation {
    condition     = can(regex("^[0-9a-fA-F]{8}-(?:[0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$", var.databricks_account_id))
    error_message = "databricks_account_id must be a GUID, for example 00000000-0000-0000-0000-000000000000."
  }
}

variable "databricks_account_host" {
  description = "Account console endpoint. Change it only for sovereign clouds."
  type        = string
  default     = "https://accounts.azuredatabricks.net"

  validation {
    condition     = can(regex("^https://[A-Za-z0-9.-]+$", var.databricks_account_host))
    error_message = "databricks_account_host must be an https:// URL without a path or trailing slash."
  }
}

# ---------------------------------------------------------------------------
# Inputs produced by the platform stack. Do not type these by hand: write the
# platform stack's governance_inputs output to
# environments/<env>.platform.tfvars.json and pass it with -var-file.
# ---------------------------------------------------------------------------

variable "platform_environment" {
  description = "Environment the platform inputs were produced for. Guards against applying one environment's governance on another environment's workspace."
  type        = string

  validation {
    condition     = var.platform_environment == var.environment
    error_message = "The platform inputs belong to a different environment than var.environment. Regenerate environments/<env>.platform.tfvars.json from the matching platform state."
  }
}

variable "workspace_url" {
  description = "URL of the Databricks workspace, including https://."
  type        = string

  validation {
    condition     = can(regex("^https://[A-Za-z0-9.-]+$", var.workspace_url))
    error_message = "workspace_url must be an https:// URL without a path or trailing slash, for example https://adb-1234567890123456.7.azuredatabricks.net."
  }
}

variable "workspace_id" {
  description = "Numeric ID of the workspace in the Databricks control plane."
  type        = string

  validation {
    condition     = can(regex("^[0-9]+$", var.workspace_id))
    error_message = "workspace_id must contain digits only."
  }
}

variable "workspace_resource_id" {
  description = "Azure resource ID of the Databricks workspace."
  type        = string

  validation {
    condition     = can(regex("(?i)^/subscriptions/[^/]+/resourceGroups/[^/]+/providers/Microsoft\\.Databricks/workspaces/[^/]+$", var.workspace_resource_id))
    error_message = "workspace_resource_id must be the Azure resource ID of a Microsoft.Databricks/workspaces resource."
  }
}

variable "access_connector_id" {
  description = "Azure resource ID of the Databricks access connector whose managed identity has Storage Blob Data Contributor on the data lake."
  type        = string

  validation {
    condition     = can(regex("(?i)^/subscriptions/[^/]+/resourceGroups/[^/]+/providers/Microsoft\\.Databricks/accessConnectors/[^/]+$", var.access_connector_id))
    error_message = "access_connector_id must be the Azure resource ID of a Microsoft.Databricks/accessConnectors resource."
  }
}

variable "managed_storage_url" {
  description = "abfss URL of the container that backs the catalog's managed storage."
  type        = string

  validation {
    condition     = can(regex("^abfss://[a-z0-9-]+@[a-z0-9.-]+(/.*)?$", var.managed_storage_url))
    error_message = "managed_storage_url must look like abfss://<container>@<account>.dfs.core.windows.net."
  }
}

variable "landing_storage_url" {
  description = "abfss URL of the raw landing zone container."
  type        = string

  validation {
    condition     = can(regex("^abfss://[a-z0-9-]+@[a-z0-9.-]+(/.*)?$", var.landing_storage_url))
    error_message = "landing_storage_url must look like abfss://<container>@<account>.dfs.core.windows.net."
  }
}

# ---------------------------------------------------------------------------
# Naming
# ---------------------------------------------------------------------------

variable "prefix" {
  description = "Short workload prefix. The catalog is <prefix>_<environment>, the groups are <prefix>-<role> and the service principals are <prefix>-<role>-<environment>. Use the same value as in the platform stack."
  type        = string
  default     = "etl"

  validation {
    condition     = can(regex("^[a-z][a-z0-9]{1,7}$", var.prefix))
    error_message = "prefix must be 2 to 8 characters, lowercase letters and digits only, starting with a letter."
  }
}

variable "environment" {
  description = "Deployment environment. One state file and one set of resources per environment."
  type        = string

  validation {
    condition     = contains(["dev", "test", "prod"], var.environment)
    error_message = "environment must be one of: dev, test, prod."
  }
}

# ---------------------------------------------------------------------------
# Groups
# ---------------------------------------------------------------------------

variable "create_groups" {
  description = "true creates the three role groups in the Databricks account. false looks up existing account groups by name, which is the usual setup when groups are synced from Microsoft Entra ID. Membership is never managed here."
  type        = bool
  default     = true
}

variable "group_name_overrides" {
  description = "Display names to use instead of the <prefix>-<role> defaults, keyed by role (platform_admins, data_engineers, data_analysts). Mostly useful with create_groups = false, when the synced groups follow another naming convention."
  type        = map(string)
  default     = {}

  validation {
    condition = alltrue([
      for role, name in var.group_name_overrides :
      contains(["platform_admins", "data_engineers", "data_analysts"], role) && length(trimspace(name)) > 0
    ])
    error_message = "group_name_overrides keys must be platform_admins, data_engineers or data_analysts, and names must not be empty."
  }
}

variable "data_engineer_access" {
  description = "What the data-engineers group may do with data. read_write (non-production): read, write and create tables in every schema, read the landing zone and read the secret scope. read_only (production): read every schema and nothing else. The default is the restrictive one, so forgetting to set it never widens access."
  type        = string
  default     = "read_only"

  validation {
    condition     = contains(["read_only", "read_write"], var.data_engineer_access)
    error_message = "data_engineer_access must be read_only or read_write."
  }
}

# ---------------------------------------------------------------------------
# CI/CD identity: GitHub Actions OIDC federation for the deployer
# ---------------------------------------------------------------------------

variable "github_organization" {
  description = "GitHub organization or user that owns the repository whose workflows deploy the bundle."
  type        = string

  validation {
    condition     = can(regex("^[A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?$", var.github_organization))
    error_message = "github_organization must be a GitHub account name: letters, digits and single hyphens, at most 39 characters."
  }
}

variable "github_repository" {
  description = "Name of the GitHub repository (without the organization) whose workflows deploy the bundle."
  type        = string

  validation {
    condition     = can(regex("^[A-Za-z0-9._-]{1,100}$", var.github_repository))
    error_message = "github_repository must be a repository name: letters, digits, dots, underscores and hyphens, at most 100 characters."
  }
}

variable "github_environment" {
  description = "Name of the GitHub environment the deploy job runs in. Defaults to var.environment. Only jobs that declare this environment can obtain a token for the deployer."
  type        = string
  default     = null

  validation {
    condition     = var.github_environment == null ? true : can(regex("^[^:\\s][^:]{0,254}$", var.github_environment))
    error_message = "github_environment must be a GitHub environment name without colons."
  }
}

variable "github_oidc_subject" {
  description = <<-EOT
    Exact sub claim the deployer's federation policy trusts. When set, it is used as is and
    github_organization, github_repository and github_environment no longer shape the
    subject.

    null (the default) composes repo:<organization>/<repository>:environment:<environment>,
    which is what GitHub issues for repositories created before 15 July 2026 that were not
    renamed or transferred since. Later, renamed, transferred or opted-in repositories get
    an immutable subject that also carries numeric IDs:
    repo:<organization>@<owner id>/<repository>@<repository id>:environment:<environment>.
    Databricks compares the subject exactly, so supply the real value here in that case.
  EOT
  type        = string
  default     = null

  validation {
    condition     = var.github_oidc_subject == null ? true : can(regex("^repo:[^:]+:environment:[^:]+$", var.github_oidc_subject))
    error_message = "github_oidc_subject must be an environment-scoped subject: repo:<owner>/<repository>:environment:<environment>, with @<id> after the owner and the repository for the immutable format."
  }
}

variable "github_oidc_audiences" {
  description = "Audiences (aud claim) the deployer's federation policy accepts. null (the default) accepts the Databricks account ID, which is what Databricks recommends and what the Databricks CLI requests from GitHub unless told otherwise. If you set another value, make the workflow request it by setting DATABRICKS_TOKEN_AUDIENCE."
  type        = list(string)
  default     = null

  validation {
    condition     = var.github_oidc_audiences == null ? true : length(var.github_oidc_audiences) > 0 && alltrue([for audience in var.github_oidc_audiences : length(trimspace(audience)) > 0])
    error_message = "github_oidc_audiences must be null or a non-empty list of non-empty strings."
  }
}

# ---------------------------------------------------------------------------
# Unity Catalog and secrets
# ---------------------------------------------------------------------------

variable "isolate_to_workspace" {
  description = "Bind the catalog, the storage credential and the external locations to this workspace only. Environments usually share one regional metastore, so without the binding a principal with a grant could reach this environment's data from another environment's workspace."
  type        = bool
  default     = true
}

variable "force_destroy" {
  description = "Allow terraform destroy to delete the catalog, schemas, external locations and storage credential even when they still contain tables or dependants. Leave false; set it to true and apply only immediately before a planned teardown."
  type        = bool
  default     = false
}

variable "secret_scope_name" {
  description = "Name of the Databricks secret scope for source-system credentials. Pipelines reference it as $${secret:<scope>/<key>}. Every environment has its own workspace, so the same name in all of them keeps pipeline configuration identical across environments."
  type        = string
  default     = "source-systems"

  validation {
    condition     = can(regex("^[A-Za-z0-9_.@-]{1,128}$", var.secret_scope_name))
    error_message = "secret_scope_name may contain letters, digits, dashes, underscores, periods and @, and must not exceed 128 characters."
  }
}

# ---------------------------------------------------------------------------
# Optional SQL warehouse for analysts
# ---------------------------------------------------------------------------

variable "enable_sql_warehouse" {
  description = "Create a serverless SQL warehouse that the data-analysts and data-engineers groups can use."
  type        = bool
  default     = false
}

variable "sql_warehouse_cluster_size" {
  description = "Size of the SQL warehouse."
  type        = string
  default     = "2X-Small"

  validation {
    condition     = contains(["2X-Small", "X-Small", "Small", "Medium", "Large", "X-Large", "2X-Large", "3X-Large", "4X-Large", "5X-Large"], var.sql_warehouse_cluster_size)
    error_message = "sql_warehouse_cluster_size must be one of: 2X-Small, X-Small, Small, Medium, Large, X-Large, 2X-Large, 3X-Large, 4X-Large, 5X-Large."
  }
}

variable "sql_warehouse_auto_stop_minutes" {
  description = "Idle minutes after which the SQL warehouse stops."
  type        = number
  default     = 10

  validation {
    condition     = var.sql_warehouse_auto_stop_minutes >= 1 && floor(var.sql_warehouse_auto_stop_minutes) == var.sql_warehouse_auto_stop_minutes
    error_message = "sql_warehouse_auto_stop_minutes must be a whole number of at least 1."
  }
}
