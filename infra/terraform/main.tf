# Wires the modules together into the three docs/COST.md tiers.
#
# NOTE: written and structurally validated (hcl2 parse) in an environment
# with no Terraform binary and no AWS credentials -- see README.md's
# "Status: written, not applied" section before running this anywhere.

locals {
  name = "${var.project_name}-${var.environment_size}"

  azs = slice(
    ["${var.aws_region}a", "${var.aws_region}b", "${var.aws_region}c", "${var.aws_region}d"],
    0,
    var.availability_zone_count,
  )

  # Mirrors docs/COST.md's Small / Medium / Large tables.
  size_map = {
    small = {
      eks_node_instance_types = ["t3.medium"]
      eks_node_min            = 2
      eks_node_max            = 6
      eks_node_desired        = 3

      msk_broker_instance_type = "kafka.t3.small"
      msk_broker_count         = 3
      msk_broker_ebs_gb        = 100

      redis_node_type   = "cache.t3.micro"
      redis_num_clusters = 1

      rds_instance_class     = "db.t3.medium"
      rds_allocated_storage  = 100
      rds_multi_az           = false
    }
    medium = {
      eks_node_instance_types = ["m5.xlarge"]
      eks_node_min            = 3
      eks_node_max            = 12
      eks_node_desired        = 5

      msk_broker_instance_type = "kafka.m5.large"
      msk_broker_count         = 6
      msk_broker_ebs_gb        = 500

      redis_node_type    = "cache.m5.large"
      redis_num_clusters = 2

      rds_instance_class    = "db.r5.xlarge"
      rds_allocated_storage = 500
      rds_multi_az          = true
    }
    large = {
      eks_node_instance_types = ["m5.2xlarge"]
      eks_node_min            = 5
      eks_node_max            = 30
      eks_node_desired        = 10

      msk_broker_instance_type = "kafka.m5.2xlarge"
      msk_broker_count         = 9
      msk_broker_ebs_gb        = 1000

      redis_node_type    = "cache.r5.xlarge"
      redis_num_clusters = 3

      rds_instance_class    = "db.r5.2xlarge"
      rds_allocated_storage = 2000
      rds_multi_az          = true
    }
  }

  tier = local.size_map[var.environment_size]

  tags = {
    Tier = var.environment_size
  }
}

module "vpc" {
  source = "./modules/vpc"

  name = local.name
  cidr = var.vpc_cidr
  azs  = local.azs
  tags = local.tags
}

module "eks" {
  source = "./modules/eks"

  name               = local.name
  kubernetes_version = var.kubernetes_version
  vpc_id             = module.vpc.vpc_id
  private_subnet_ids = module.vpc.private_subnet_ids

  node_instance_types = local.tier.eks_node_instance_types
  node_min_size       = local.tier.eks_node_min
  node_max_size       = local.tier.eks_node_max
  node_desired_size   = local.tier.eks_node_desired

  tags = local.tags
}

module "msk" {
  source = "./modules/msk"

  name                  = local.name
  broker_instance_type  = local.tier.msk_broker_instance_type
  broker_count          = local.tier.msk_broker_count
  broker_ebs_volume_gb  = local.tier.msk_broker_ebs_gb
  vpc_id                = module.vpc.vpc_id
  subnet_ids            = module.vpc.private_subnet_ids
  allowed_security_group_ids = [module.eks.node_security_group_id]

  tags = local.tags
}

module "redis" {
  source = "./modules/elasticache"

  name               = local.name
  node_type          = local.tier.redis_node_type
  num_cache_clusters = local.tier.redis_num_clusters
  vpc_id             = module.vpc.vpc_id
  subnet_ids         = module.vpc.private_subnet_ids
  allowed_security_group_ids = [module.eks.node_security_group_id]

  tags = local.tags
}

module "timescaledb" {
  source = "./modules/rds_timescaledb"

  name                  = local.name
  instance_class        = local.tier.rds_instance_class
  allocated_storage_gb  = local.tier.rds_allocated_storage
  multi_az              = local.tier.rds_multi_az
  master_username       = var.postgres_master_username
  master_password       = var.postgres_master_password
  vpc_id                = module.vpc.vpc_id
  subnet_ids            = module.vpc.private_subnet_ids
  allowed_security_group_ids = [module.eks.node_security_group_id]

  tags = local.tags
}
