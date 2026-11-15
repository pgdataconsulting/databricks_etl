terraform {
  required_version = ">= 1.9.0, < 2.0.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 5.14"
    }
  }

  # Partial configuration: the storage account, container and state key are
  # supplied at init time with -backend-config=<env>.backend.hcl.
  backend "azurerm" {}
}
