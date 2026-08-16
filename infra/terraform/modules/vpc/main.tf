# VPC with one private + one public subnet per AZ, a NAT gateway per AZ for
# the private subnets (EKS nodes, MSK brokers, RDS, ElastiCache all live in
# private subnets), and DNS support for EKS/service discovery.
#
# Uses the community module rather than hand-rolled aws_vpc/aws_subnet
# resources -- this is the standard way real teams provision a VPC in
# Terraform, and reimplementing subnet math and route tables by hand here
# would be a worse reference, not a better one.
module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.8"

  name = var.name
  cidr = var.cidr

  azs             = var.azs
  private_subnets = [for i, az in var.azs : cidrsubnet(var.cidr, 4, i)]
  public_subnets  = [for i, az in var.azs : cidrsubnet(var.cidr, 4, i + 8)]

  enable_nat_gateway   = true
  single_nat_gateway   = false # one per AZ -- no cross-AZ single point of failure
  enable_dns_hostnames = true
  enable_dns_support   = true

  # Required tags for the EKS/ALB controllers to auto-discover subnets.
  public_subnet_tags = {
    "kubernetes.io/role/elb" = "1"
  }
  private_subnet_tags = {
    "kubernetes.io/role/internal-elb" = "1"
  }

  tags = var.tags
}
