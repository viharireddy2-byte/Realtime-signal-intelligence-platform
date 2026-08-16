variable "name" {
  type = string
}

variable "node_type" {
  type = string
}

variable "num_cache_clusters" {
  description = "1 = single node (matches docker-compose's standalone Redis), 2+ = primary + replicas."
  type        = number
  default     = 1
}

variable "vpc_id" {
  type = string
}

variable "subnet_ids" {
  type = list(string)
}

variable "allowed_security_group_ids" {
  type = list(string)
}

variable "tags" {
  type    = map(string)
  default = {}
}
