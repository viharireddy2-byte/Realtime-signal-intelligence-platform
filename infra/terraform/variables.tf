variable "project_name" {
  description = "Short name used as a prefix for every resource this creates."
  type        = string
  default     = "signal-intel"
}

variable "aws_region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "us-east-1"
}

variable "environment_size" {
  description = "Sizing tier matching docs/COST.md: \"small\" (<=10k events/sec), \"medium\" (<=50k events/sec), or \"large\"."
  type        = string
  default     = "small"

  validation {
    condition     = contains(["small", "medium", "large"], var.environment_size)
    error_message = "environment_size must be one of: small, medium, large."
  }
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC."
  type        = string
  default     = "10.42.0.0/16"
}

variable "availability_zone_count" {
  description = "Number of AZs to spread subnets across."
  type        = number
  default     = 3
}

variable "kubernetes_version" {
  description = "EKS control plane version."
  type        = string
  default     = "1.29"
}

variable "postgres_master_username" {
  description = "Master username for the RDS/TimescaleDB instance."
  type        = string
  default     = "signalintel_admin"
}

variable "postgres_master_password" {
  description = "Master password for the RDS/TimescaleDB instance. No default on purpose -- pass via TF_VAR_postgres_master_password or, better, wire the module to pull from AWS Secrets Manager instead of taking this as a plain variable at all."
  type        = string
  sensitive   = true
}
