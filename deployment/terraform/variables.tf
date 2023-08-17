variable "project" {
  type        = string
  description = "A project namespace for the infrastructure."
}
variable "environment" {
  type        = string
  description = "An environment namespace for the infrastructure."
}

variable "account" {
  type = string
}

variable "region" {
  type        = string
  default     = "us-east-1"
  description = "A valid AWS region to configure the underlying AWS SDK."
}

variable "AWSLambdaBasicExecutionRoleManagedPolicy" {
  type = string
  default = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

variable "portal_emr_ec2_subnet_id" {
  type = string
  description = "Subnet ID used for Ec2 (EMR)"
}

variable "portal_etl_bucket" {
  type = string
  description = "Portal ETL Bucket"
}

variable "emr_instance_profile" {
  type = string
  description = "EMR Instance Profile"
}

variable "emr_service_role" {
  type = string
}

variable "elastic_search_endpoint" {
  type = string
}