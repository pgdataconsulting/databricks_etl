# Tests of naming, tagging and input validation. The provider is mocked, so this
# runs without credentials and creates nothing:
#
#   terraform init -backend=false
#   terraform test

mock_provider "azurerm" {
  mock_data "azurerm_client_config" {
    defaults = {
      tenant_id = "00000000-0000-0000-0000-000000000000"
    }
  }
}

variables {
  subscription_id = "00000000-0000-0000-0000-000000000000"
  environment     = "dev"
  location        = "westeurope"
  name_suffix     = "d1x7"
}

run "names_and_tags" {
  command = plan

  variables {
    tags = {
      cost_center = "0000"
      managed_by  = "someone-else"
    }
  }

  assert {
    condition     = azurerm_resource_group.this.name == "rg-etl-dev"
    error_message = "Unexpected resource group name."
  }

  assert {
    condition     = azurerm_storage_account.lake.name == "stetldevd1x7"
    error_message = "Unexpected storage account name."
  }

  assert {
    condition     = azurerm_key_vault.this.name == "kv-etl-dev-d1x7"
    error_message = "Unexpected Key Vault name."
  }

  assert {
    condition     = azurerm_databricks_workspace.this.name == "dbw-etl-dev" && azurerm_databricks_workspace.this.sku == "premium"
    error_message = "The workspace must be named dbw-<prefix>-<environment> and be premium."
  }

  assert {
    condition     = toset([for container in azurerm_storage_container.this : container.name]) == toset(["unity-catalog", "landing"])
    error_message = "The data lake must have the unity-catalog and landing containers."
  }

  assert {
    condition = azurerm_resource_group.this.tags == tomap({
      cost_center = "0000"
      project     = "databricks-etl"
      environment = "dev"
      managed_by  = "terraform"
    })
    error_message = "Extra tags are merged in, but project, environment and managed_by cannot be overridden."
  }

  # No firewall configured: both data planes allow by default.
  assert {
    condition     = azurerm_storage_account.lake.network_rules[0].default_action == "Allow" && azurerm_key_vault.this.network_acls[0].default_action == "Allow"
    error_message = "Without a firewall variable the default action must be Allow."
  }
}

run "longest_names_fit_azure_limits" {
  command = plan

  variables {
    prefix      = "abcdefgh"
    environment = "test"
    name_suffix = "123456"
  }

  assert {
    condition     = length(azurerm_storage_account.lake.name) <= 24 && can(regex("^[a-z0-9]+$", azurerm_storage_account.lake.name))
    error_message = "Storage account names are limited to 24 lowercase letters and digits."
  }

  assert {
    condition     = length(azurerm_key_vault.this.name) <= 24
    error_message = "Key Vault names are limited to 24 characters."
  }
}

run "firewalls_deny_by_default_when_configured" {
  command = plan

  variables {
    storage_firewall = {
      allowed_ip_ranges = ["203.0.113.0/24"]
    }
    key_vault_firewall = {
      allowed_ip_ranges = ["203.0.113.0/24"]
    }
  }

  assert {
    condition     = azurerm_storage_account.lake.network_rules[0].default_action == "Deny" && azurerm_storage_account.lake.network_rules[0].ip_rules == toset(["203.0.113.0/24"])
    error_message = "A storage firewall must deny by default and allow the listed ranges."
  }

  assert {
    condition     = azurerm_key_vault.this.network_acls[0].default_action == "Deny" && azurerm_key_vault.this.network_acls[0].ip_rules == toset(["203.0.113.0/24"])
    error_message = "A Key Vault firewall must deny by default and allow the listed ranges."
  }
}

run "private_workspace_requires_vnet_injection" {
  command = plan

  variables {
    workspace_public_network_access_enabled = false
  }

  expect_failures = [var.workspace_public_network_access_enabled]
}

run "rejects_unknown_environment" {
  command = plan

  variables {
    environment = "staging"
  }

  expect_failures = [var.environment]
}

run "rejects_suffix_that_breaks_storage_naming" {
  command = plan

  variables {
    name_suffix = "Dev-01"
  }

  expect_failures = [var.name_suffix]
}
