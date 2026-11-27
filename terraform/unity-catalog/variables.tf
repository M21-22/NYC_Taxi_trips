variable "workspace_host" {
  description = "HTTPS workspace URL from the Azure Terraform output."
  type        = string
}

variable "access_connector_id" {
  description = "Azure Access Connector resource ID from the Azure Terraform output."
  type        = string
}

variable "storage_urls" {
  description = "Separate ADLS container URLs from the Azure Terraform output."
  type = object({
    landing  = string
    customer = string
    managed  = string
  })
}

variable "catalog_name" {
  description = "Name of the new Unity Catalog catalog."
  type        = string
  default     = "nyc_taxi_dev"

  validation {
    condition     = can(regex("^[a-z][a-z0-9_]*$", var.catalog_name))
    error_message = "Use a lowercase SQL identifier for the catalog name."
  }
}

variable "principals" {
  description = "Existing Unity Catalog principals. Use service principal application IDs for the pipeline and customer job."
  type = object({
    pipeline       = string
    staging        = string
    analytics      = string
    customer_admin = string
    customer_job   = string
  })

  validation {
    condition = alltrue([
      for principal in values(var.principals) : trimspace(principal) != ""
    ])
    error_message = "Each role must name an existing Unity Catalog principal."
  }

  validation {
    condition     = length(distinct([for principal in values(var.principals) : lower(trimspace(principal))])) == 5
    error_message = "Use a distinct principal for each access role."
  }
}
