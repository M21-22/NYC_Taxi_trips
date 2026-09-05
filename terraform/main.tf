# Setup azurerm as a state backend
terraform {
  backend "azurerm" {
    resource_group_name  = "spark-nyctrips-rg"
    storage_account_name = "sparknyctripssa" # Provide Storage Account name, where Terraform Remote state is stored
    container_name       = "spark-nyctrips-sc"
    key                  = "terraform.tfstate"
  }
}

# Configure the Microsoft Azure Provider
provider "azurerm" {
  features {}
}

resource "random_string" "suffix" {
  length  = 4
  special = false
  upper   = false
}

data "azurerm_client_config" "current" {}

resource "azurerm_resource_group" "sdbdp" {
  name     = "rg-${var.ENV}-${var.LOCATION}-${random_string.suffix.result}"
  location = var.LOCATION

  lifecycle {
    prevent_destroy = false
  }

  tags = {
    region = var.SDBDP_REGION
    env    = var.ENV
  }
}

resource "azurerm_storage_account" "sdbdp" {
  depends_on = [
  azurerm_resource_group.sdbdp]

  name                     = "st${var.ENV}${var.LOCATION}${random_string.suffix.result}"
  resource_group_name      = azurerm_resource_group.sdbdp.name
  location                 = azurerm_resource_group.sdbdp.location
  account_tier             = "Standard"
  account_replication_type = var.STORAGE_ACCOUNT_REPLICATION_TYPE
  is_hns_enabled           = "true"

  network_rules {
    default_action = "Allow"
  }

  lifecycle {
    prevent_destroy = false
  }

  tags = {
    region = var.SDBDP_REGION
    env    = var.ENV
  }
}

resource "azurerm_storage_data_lake_gen2_filesystem" "gen2_data" {
  depends_on = [
  azurerm_storage_account.sdbdp]

  name               = "data"
  storage_account_id = azurerm_storage_account.sdbdp.id

  lifecycle {
    prevent_destroy = false
  }
}

resource "azurerm_databricks_workspace" "sdbdp" {
  depends_on = [
    azurerm_resource_group.sdbdp
  ]

  name                = "dbw-${var.ENV}-${var.LOCATION}-${random_string.suffix.result}"
  resource_group_name = azurerm_resource_group.sdbdp.name
  location            = azurerm_resource_group.sdbdp.location
  sku                 = "premium" # premium <- standard

  tags = {
    region = var.SDBDP_REGION
    env    = var.ENV
  }
}

resource "azurerm_databricks_access_connector" "sdbdp" {
  name                = "ac-${var.ENV}-${var.LOCATION}-${random_string.suffix.result}"
  resource_group_name = azurerm_resource_group.sdbdp.name
  location            = azurerm_resource_group.sdbdp.location

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_role_assignment" "storage_blob_contributor" {
  scope                = azurerm_storage_account.sdbdp.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.sdbdp.identity[0].principal_id
}

output "resource_group_name" {
  description = "The name of the created Azure Resource Group."
  value       = azurerm_resource_group.sdbdp.name
}

output "storage_account_name" {
  value = azurerm_storage_account.sdbdp.name
}

output "access_connector_id" {
  value = azurerm_databricks_access_connector.sdbdp.id
}

output "databricks_workspace_url" {
  value = azurerm_databricks_workspace.sdbdp.workspace_url
}