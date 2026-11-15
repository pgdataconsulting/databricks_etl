# ---------------------------------------------------------------------------
# Identity of the deployment
# ---------------------------------------------------------------------------

variable "subscription_id" {
  description = "ID of the Azure subscription this environment is deployed into."
  type        = string

  validation {
    condition     = can(regex("^[0-9a-fA-F]{8}-(?:[0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$", var.subscription_id))
    error_message = "subscription_id must be a GUID, for example 00000000-0000-0000-0000-000000000000."
  }
}

variable "prefix" {
  description = "Short workload prefix used in every resource name. It is also the prefix of the Unity Catalog catalog and of the groups in the governance stack, so use the same value in both stacks."
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

variable "location" {
  description = "Azure region for every resource, as a region name without spaces (for example westeurope)."
  type        = string

  validation {
    condition     = can(regex("^[a-z]+[a-z0-9]*$", var.location))
    error_message = "location must be an Azure region name in lowercase without spaces, for example westeurope."
  }
}

variable "name_suffix" {
  description = "Short suffix that makes the globally unique names (storage account, Key Vault) unique. Pick it once per environment and do not change it: changing it replaces both resources."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9]{2,6}$", var.name_suffix))
    error_message = "name_suffix must be 2 to 6 lowercase letters or digits."
  }
}

variable "project" {
  description = "Value of the project tag on every resource."
  type        = string
  default     = "databricks-etl"

  validation {
    condition     = length(var.project) > 0 && length(var.project) <= 256
    error_message = "project must be between 1 and 256 characters."
  }
}

variable "tags" {
  description = "Additional tags for every resource. The project, environment and managed_by tags are set by the stack and cannot be overridden here."
  type        = map(string)
  default     = {}
}

variable "register_resource_providers" {
  description = "Whether Terraform registers the Azure resource providers this stack needs. Set to false when resource provider registration is managed outside Terraform or the deploying identity may not register providers."
  type        = bool
  default     = true
}

# ---------------------------------------------------------------------------
# Data lake storage
# ---------------------------------------------------------------------------

variable "storage_replication_type" {
  description = "Replication of the data lake storage account."
  type        = string
  default     = "GRS"

  validation {
    condition     = contains(["LRS", "ZRS", "GRS", "RAGRS", "GZRS", "RAGZRS"], var.storage_replication_type)
    error_message = "storage_replication_type must be one of: LRS, ZRS, GRS, RAGRS, GZRS, RAGZRS."
  }
}

variable "storage_soft_delete_retention_days" {
  description = "Days a deleted blob or container stays recoverable in the data lake storage account."
  type        = number
  default     = 7

  validation {
    condition     = var.storage_soft_delete_retention_days >= 1 && var.storage_soft_delete_retention_days <= 365 && floor(var.storage_soft_delete_retention_days) == var.storage_soft_delete_retention_days
    error_message = "storage_soft_delete_retention_days must be a whole number between 1 and 365."
  }
}

variable "storage_firewall" {
  description = <<-EOT
    Network restriction for the data lake storage account.

    null (the default) leaves the public endpoint reachable from any network. Requests
    still need Microsoft Entra ID authorization, because Shared Key access and anonymous
    access are disabled.

    Set it to deny by default and allow only the listed public IP ranges and subnets. The
    Databricks access connector is always allowed as a resource instance. Only do this when
    Databricks compute reaches the account through one of the listed subnets or a private
    endpoint; this stack creates neither.
  EOT
  type = object({
    allowed_ip_ranges  = optional(list(string), [])
    allowed_subnet_ids = optional(list(string), [])
  })
  default = null
}

# ---------------------------------------------------------------------------
# Databricks workspace
# ---------------------------------------------------------------------------

variable "workspace_vnet_injection" {
  description = <<-EOT
    Deploy the workspace into an existing virtual network (VNet injection). null (the
    default) lets Azure Databricks create and manage the workspace network.

    The virtual network, the two delegated subnets and their network security group
    associations must already exist; this stack does not create them. required_nsg_rules
    is AllRules unless classic compute reaches the control plane over Private Link only, in
    which case it is NoAzureDatabricksRules.
  EOT
  type = object({
    virtual_network_id                = string
    public_subnet_name                = string
    private_subnet_name               = string
    public_subnet_nsg_association_id  = string
    private_subnet_nsg_association_id = string
    required_nsg_rules                = optional(string, "AllRules")
  })
  default = null

  validation {
    condition     = var.workspace_vnet_injection == null ? true : contains(["AllRules", "NoAzureDatabricksRules"], var.workspace_vnet_injection.required_nsg_rules)
    error_message = "workspace_vnet_injection.required_nsg_rules must be AllRules or NoAzureDatabricksRules."
  }
}

variable "workspace_public_network_access_enabled" {
  description = "Whether the workspace is reachable over the public internet. Setting it to false makes the workspace reachable through Private Link only, which requires a VNet-injected workspace (workspace_vnet_injection) and private endpoints that this stack does not create."
  type        = bool
  default     = true

  validation {
    condition     = var.workspace_public_network_access_enabled || var.workspace_vnet_injection != null
    error_message = "Disabling public network access requires workspace_vnet_injection: Private Link is only available for VNet-injected workspaces."
  }
}

# ---------------------------------------------------------------------------
# Key Vault
# ---------------------------------------------------------------------------

variable "key_vault_firewall" {
  description = <<-EOT
    Network restriction for the Key Vault that holds source-system credentials.

    null (the default) leaves the public endpoint reachable from any network; requests
    still need Microsoft Entra ID authorization and an Azure RBAC role on the vault. Set it
    to deny by default and allow only the listed public IP ranges and subnets (the people
    and pipelines that maintain the secrets).
  EOT
  type = object({
    allowed_ip_ranges  = optional(list(string), [])
    allowed_subnet_ids = optional(list(string), [])
  })
  default = null
}

variable "key_vault_secrets_officer_principal_ids" {
  description = "Microsoft Entra object IDs (users, groups or service principals) that may create, read and delete secrets in the Key Vault. The vault uses Azure RBAC, so nobody can touch secrets until a role is assigned, including the identity that created the vault."
  type        = list(string)
  default     = []

  validation {
    condition = alltrue([
      for id in var.key_vault_secrets_officer_principal_ids :
      can(regex("^[0-9a-fA-F]{8}-(?:[0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$", id))
    ])
    error_message = "Every entry of key_vault_secrets_officer_principal_ids must be a Microsoft Entra object ID (a GUID)."
  }
}
