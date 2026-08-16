# EKS cluster + one managed node group, via the community module for the
# same reason vpc/main.tf does -- IAM roles for cluster/nodes, OIDC
# provider wiring, and add-on management are exactly the kind of thing
# that's easy to get subtly wrong by hand and well-tested in the module.
#
# infra/helm/signal-intel-platform deploys onto whatever cluster this
# produces -- `aws eks update-kubeconfig --name <cluster_name>` then
# `helm upgrade --install` per docs/DEPLOYMENT.md.
module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.8"

  cluster_name    = var.name
  cluster_version = var.kubernetes_version

  vpc_id     = var.vpc_id
  subnet_ids = var.private_subnet_ids

  cluster_endpoint_public_access = true # narrow this to a CIDR allowlist for anything beyond a demo

  eks_managed_node_groups = {
    default = {
      instance_types = var.node_instance_types
      min_size       = var.node_min_size
      max_size       = var.node_max_size
      desired_size   = var.node_desired_size
      capacity_type  = "ON_DEMAND"
    }
  }

  # Lets in-cluster workloads (e.g. an external-secrets or cluster-autoscaler
  # deployment, not shipped in this repo) assume IAM roles via IRSA.
  enable_irsa = true

  tags = var.tags
}
