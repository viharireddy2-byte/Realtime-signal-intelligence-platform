# Terraform (reference architecture)

`docs/COST.md` describes three AWS sizing tiers (small/medium/large) for
running this platform in the cloud, but until now there was nothing in the
repo that actually provisioned them — the Helm chart deploys application
workloads *onto* a Kubernetes cluster, but nothing created the cluster,
the managed Kafka, the managed Redis, or the managed Postgres it assumes
exist. This directory is that missing layer: Terraform that provisions a
VPC, an EKS cluster, an MSK (managed Kafka) cluster, an ElastiCache (Redis)
replication group, and an RDS PostgreSQL instance with the TimescaleDB
extension enabled — sized by an `environment_size` variable that maps onto
`docs/COST.md`'s tiers.

## Status: written, not applied

**Read this before you run anything here.** This was written and
syntax-checked (`python -c "import hcl2; hcl2.load(...)"` against every
`.tf` file — a structural HCL2 parse, not a semantic one) in an
environment with no Terraform binary and no AWS credentials, so:

- `terraform validate` (provider-schema-aware validation) has **not** been run.
- `terraform plan` against a real AWS account has **not** been run.
- No `apply` has ever happened. There is no real infrastructure behind this.

Before pointing this at a real account:

```bash
cd infra/terraform
terraform init
terraform validate
terraform plan -var-file=terraform.tfvars   # review every resource it proposes
```

Treat this the way you'd treat any Terraform you inherited from someone
else on the team: read it, validate it, plan it, and only then apply it —
ideally against a throwaway account first.

## Layout

```text
infra/terraform/
├── main.tf                    # wires the modules together
├── variables.tf                # environment_size, region, project name, etc.
├── outputs.tf                  # cluster endpoint, MSK brokers, Redis/RDS endpoints
├── terraform.tfvars.example    # one example per docs/COST.md tier
└── modules/
    ├── vpc/                    # VPC, subnets, NAT — via terraform-aws-modules/vpc/aws
    ├── eks/                    # EKS cluster + managed node group — via terraform-aws-modules/eks/aws
    ├── msk/                    # Managed Kafka (aws_msk_cluster)
    ├── elasticache/            # Redis replication group (aws_elasticache_replication_group)
    └── rds_timescaledb/        # PostgreSQL 15 + timescaledb extension (aws_db_instance + aws_db_parameter_group)
```

`vpc` and `eks` use the well-known community modules
(`terraform-aws-modules/vpc/aws`, `terraform-aws-modules/eks/aws`) rather
than hand-rolled resources — that's the standard, idiomatic way to
provision either in real Terraform, and reimplementing them by hand would
be a worse example to learn from, not a better one. `msk`, `elasticache`,
and `rds_timescaledb` are hand-rolled native resources, since AWS doesn't
publish first-party modules for those with the same ubiquity.

## Sizing tiers

`environment_size = "small" | "medium" | "large"` in `terraform.tfvars`
selects instance types and counts matching the corresponding table in
[`../../docs/COST.md`](../../docs/COST.md). See `variables.tf` for the
exact mapping (`local.size_map` in `main.tf`).

## What this deliberately does not do

- No remote state backend is configured (`versions.tf` has an example,
  commented out) — wire up an S3 backend + DynamoDB lock table before any
  real use; local state is fine for `plan`-only review.
- No CI job runs `terraform plan` against a real account (that would need
  cloud credentials in GitHub Actions secrets, which this repo
  intentionally doesn't ship). CI does run `terraform fmt -check` and a
  provider-less `terraform validate` (see `.github/workflows/ci-cd.yml`,
  `terraform-validate` job) so syntax errors are caught even without
  credentials.
- Secrets (the MSK/RDS master password, etc.) are passed as Terraform
  variables with no default, deliberately — wire these to AWS Secrets
  Manager or SSM Parameter Store in a real deployment rather than a
  `.tfvars` file that could end up in version control.
