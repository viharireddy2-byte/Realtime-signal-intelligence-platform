variable "name" {
  type = string
}

variable "kafka_version" {
  type    = string
  default = "3.6.0"
}

variable "broker_instance_type" {
  type = string
}

variable "broker_count" {
  description = "Total broker count across all AZs -- must be a multiple of the number of subnets."
  type        = number
}

variable "broker_ebs_volume_gb" {
  type = number
}

variable "vpc_id" {
  type = string
}

variable "subnet_ids" {
  type = list(string)
}

variable "allowed_security_group_ids" {
  description = "Security groups (e.g. the EKS node SG) allowed to reach the brokers on the Kafka ports."
  type        = list(string)
}

variable "tags" {
  type    = map(string)
  default = {}
}
