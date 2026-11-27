provider "azurerm" {
  features {}
  storage_use_azuread = true
}

resource "random_string" "suffix" {
  length  = 4
  special = false
  upper   = false
}

locals {
  tags = {
    project = "nyc-taxi-trips"
    region  = var.SDBDP_REGION
    env     = var.ENV
  }
  container_names = {
    landing  = "trip-input"
    customer = "customer-input"
    managed  = "lakehouse-managed"
  }
}

resource "azurerm_resource_group" "sdbdp" {
  name     = "rg-${var.ENV}-${var.LOCATION}-${random_string.suffix.result}"
  location = var.LOCATION
  tags     = local.tags
}

resource "azurerm_storage_account" "sdbdp" {
  name                            = "st${var.ENV}${var.LOCATION}${random_string.suffix.result}"
  resource_group_name             = azurerm_resource_group.sdbdp.name
  location                        = azurerm_resource_group.sdbdp.location
  account_tier                    = "Standard"
  account_replication_type        = var.STORAGE_ACCOUNT_REPLICATION_TYPE
  is_hns_enabled                  = true
  shared_access_key_enabled       = false
  allow_nested_items_to_be_public = false
  min_tls_version                 = "TLS1_2"
  tags                            = local.tags

  blob_properties {
    delete_retention_policy {
      days = 7
    }
    container_delete_retention_policy {
      days = 7
    }
  }

  lifecycle {
    precondition {
      condition     = can(regex("^[a-z0-9]{3,24}$", "st${var.ENV}${var.LOCATION}0000"))
      error_message = "ENV and LOCATION must yield an Azure storage account name of 3–24 lowercase alphanumeric characters."
    }
  }
}

resource "azurerm_storage_container" "data" {
  for_each              = local.container_names
  name                  = each.value
  storage_account_id    = azurerm_storage_account.sdbdp.id
  container_access_type = "private"
}

resource "azurerm_databricks_workspace" "sdbdp" {
  name                = "dbw-${var.ENV}-${var.LOCATION}-${random_string.suffix.result}"
  resource_group_name = azurerm_resource_group.sdbdp.name
  location            = azurerm_resource_group.sdbdp.location
  sku                 = "premium"
  tags                = local.tags
}

resource "azurerm_databricks_access_connector" "sdbdp" {
  name                = "ac-${var.ENV}-${var.LOCATION}-${random_string.suffix.result}"
  resource_group_name = azurerm_resource_group.sdbdp.name
  location            = azurerm_resource_group.sdbdp.location
  tags                = local.tags

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_role_assignment" "storage_blob_delegator" {
  scope                = azurerm_storage_account.sdbdp.id
  role_definition_name = "Storage Blob Delegator"
  principal_id         = azurerm_databricks_access_connector.sdbdp.identity[0].principal_id
}

resource "azurerm_role_assignment" "container_blob_contributor" {
  for_each             = azurerm_storage_container.data
  scope                = each.value.resource_manager_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.sdbdp.identity[0].principal_id
}

data "azurerm_client_config" "current" {}

resource "azurerm_role_assignment" "input_uploader" {
  for_each = toset(["customer", "landing"])

  scope                = azurerm_storage_container.data[each.key].resource_manager_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azurerm_client_config.current.object_id
}

output "resource_group_name" {
  value = azurerm_resource_group.sdbdp.name
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

output "unity_catalog_inputs" {
  description = "Nonsecret Azure outputs consumed by the separate Unity Catalog root."
  value = {
    workspace_host      = "https://${azurerm_databricks_workspace.sdbdp.workspace_url}"
    access_connector_id = azurerm_databricks_access_connector.sdbdp.id
    storage_urls = {
      for key, container in azurerm_storage_container.data :
      key => "abfss://${container.name}@${azurerm_storage_account.sdbdp.name}.dfs.core.windows.net"
    }
  }
  depends_on = [
    azurerm_role_assignment.storage_blob_delegator,
    azurerm_role_assignment.container_blob_contributor,
  ]
}
