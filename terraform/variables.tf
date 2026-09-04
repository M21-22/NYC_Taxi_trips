variable "ENV" {
  type        = string
  description = "The prefix which should be used for all resources in this environment. Make it unique."
  default     = "dev"
}

variable "LOCATION" {
  type        = string
  description = "The Azure Region in which all resources in this example should be created."
  default     = "northeurope"
}

variable "SDBDP_REGION" {
  type        = string
  description = "The SDBDP Region for billing."
  default     = "global"
}

variable "STORAGE_ACCOUNT_REPLICATION_TYPE" {
  type        = string
  description = "Storage Account replication type."
  default     = "LRS"
}
