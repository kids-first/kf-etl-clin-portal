variable "project" {
  type        = string
  description = "A project namespace for the infrastructure."
}
variable "environment" {
  type        = string
  description = "An environment namespace for the infrastructure."
}

variable "region" {
  type        = string
  default     = "us-east-1"
  description = "A valid AWS region to configure the underlying AWS SDK."
}

variable "vpc_id" {
  type = string
}

variable "vpc_availability_zones" {
  type = list(string)
}

variable "vpc_private_subnet_ids" {
  type = list(string)
}

variable "AWSLambdaBasicExecutionRoleManagedPolicy" {
  type = string
  default = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

variable "portal_emr_ec2_subnet_id" {
  type = string
  description = "Subnet ID used for Ec2 (EMR)"
}