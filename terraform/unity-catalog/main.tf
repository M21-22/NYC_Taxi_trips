provider "databricks" {
  host = var.workspace_host
}

locals {
  schema_access = {
    landing = {
      for principal in toset([var.principals.staging, var.principals.pipeline]) :
      principal => ["USE_SCHEMA"]
    }
    restricted = {
      (var.principals.customer_admin) = ["USE_SCHEMA", "SELECT"]
      (var.principals.customer_job)   = ["USE_SCHEMA", "CREATE_TABLE"]
    }
  }
  schema_grants = merge([
    for schema, grants in local.schema_access : {
      for principal, privileges in grants :
      "${schema}/${principal}" => { schema = schema, principal = principal, privileges = privileges }
    }
  ]...)
  volumes = {
    trips     = { schema = "landing", location = "landing" }
    customers = { schema = "restricted", location = "customer" }
  }
}

resource "databricks_storage_credential" "access_connector" {
  name = "${var.catalog_name}_storage"
  azure_managed_identity {
    access_connector_id = var.access_connector_id
  }
}

resource "databricks_external_location" "data" {
  for_each           = var.storage_urls
  name               = "${var.catalog_name}_${each.key}"
  url                = each.value
  credential_name    = databricks_storage_credential.access_connector.name
  enable_file_events = false
}

resource "databricks_catalog" "taxi" {
  name         = var.catalog_name
  storage_root = "${trimsuffix(databricks_external_location.data["managed"].url, "/")}/catalog"
}

resource "databricks_schema" "layers" {
  for_each     = local.schema_access
  catalog_name = databricks_catalog.taxi.name
  name         = each.key
}

resource "databricks_volume" "inputs" {
  for_each         = local.volumes
  catalog_name     = databricks_catalog.taxi.name
  schema_name      = databricks_schema.layers[each.value.schema].name
  name             = each.key
  volume_type      = "EXTERNAL"
  storage_location = "${trimsuffix(databricks_external_location.data[each.value.location].url, "/")}/files"
}

resource "databricks_grant" "catalog_use" {
  for_each   = toset(values(var.principals))
  catalog    = databricks_catalog.taxi.name
  principal  = each.value
  privileges = ["USE_CATALOG"]
}

resource "databricks_grant" "schema_use" {
  for_each   = local.schema_grants
  schema     = databricks_schema.layers[each.value.schema].id
  principal  = each.value.principal
  privileges = each.value.privileges
}

resource "databricks_grant" "trip_volume" {
  for_each   = toset([var.principals.staging, var.principals.pipeline])
  volume     = databricks_volume.inputs["trips"].id
  principal  = each.value
  privileges = each.value == var.principals.staging ? ["READ_VOLUME", "WRITE_VOLUME"] : ["READ_VOLUME"]
}

resource "databricks_grant" "customer_volume" {
  volume     = databricks_volume.inputs["customers"].id
  principal  = var.principals.customer_admin
  privileges = ["READ_VOLUME", "WRITE_VOLUME"]
}

resource "databricks_grant" "customer_job_volume" {
  volume     = databricks_volume.inputs["customers"].id
  principal  = var.principals.customer_job
  privileges = ["READ_VOLUME"]
}

output "catalog_name" {
  value = databricks_catalog.taxi.name
}

output "trip_volume_path" {
  value = "/Volumes/${databricks_catalog.taxi.name}/${databricks_volume.inputs["trips"].schema_name}/${databricks_volume.inputs["trips"].name}"
}

output "customer_volume_path" {
  value = "/Volumes/${databricks_catalog.taxi.name}/${databricks_volume.inputs["customers"].schema_name}/${databricks_volume.inputs["customers"].name}"
}
