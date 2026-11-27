resource "databricks_secret_scope" "customer_encryption" {
  name = "${var.catalog_name}-pii"
}

resource "databricks_secret_acl" "customer_key_read" {
  for_each = toset([var.principals.customer_job, var.principals.customer_admin])

  scope      = databricks_secret_scope.customer_encryption.name
  principal  = each.value
  permission = "READ"
}
