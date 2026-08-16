variable "name" {
  type = string
}

variable "cidr" {
  type = string
}

variable "azs" {
  description = "List of availability zone names to spread subnets across."
  type        = list(string)
}

variable "tags" {
  type    = map(string)
  default = {}
}
