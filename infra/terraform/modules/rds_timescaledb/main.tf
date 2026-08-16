# Replaces the docker-compose `timescale/timescaledb:latest-pg15` container.
# RDS PostgreSQL supports TimescaleDB as an allow-listed extension as of
# PG 13+; this module provisions the instance and the parameter group that
# preloads it, but does NOT run `CREATE EXTENSION timescaledb;` or the
# schema in infra/docker-compose/init-scripts/01-init-timescaledb.sql --
# Terraform provisions infrastructure, not application schema, and this
# repo doesn't have a migration tool yet (see the "Known limitations"
# section of the root README). Run that SQL against the resulting endpoint
# once, the same way infra/docker-compose does via docker-entrypoint-initdb.d,
# or wire up a real migration tool (Alembic/Flyway) as a follow-up.
resource "aws_db_subnet_group" "this" {
  name       = "${var.name}-timescaledb"
  subnet_ids = var.subnet_ids
}

resource "aws_security_group" "postgres" {
  name_prefix = "${var.name}-timescaledb-"
  vpc_id      = var.vpc_id
  tags        = var.tags
}

resource "aws_security_group_rule" "postgres_from_allowed" {
  for_each = toset(var.allowed_security_group_ids)

  type                     = "ingress"
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  security_group_id        = aws_security_group.postgres.id
  source_security_group_id = each.value
}

resource "aws_security_group_rule" "postgres_egress_all" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  security_group_id = aws_security_group.postgres.id
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_db_parameter_group" "timescaledb" {
  name_prefix = "${var.name}-timescaledb-"
  family      = "postgres15"

  parameter {
    name         = "shared_preload_libraries"
    value        = "timescaledb"
    apply_method = "pending-reboot"
  }
}

resource "aws_db_instance" "this" {
  identifier     = "${var.name}-timescaledb"
  engine         = "postgres"
  engine_version = "15.7"
  instance_class = var.instance_class

  allocated_storage     = var.allocated_storage_gb
  max_allocated_storage = var.allocated_storage_gb * 2 # allows autoscaling storage up to 2x before a manual resize is needed
  storage_type          = "gp3"
  storage_encrypted     = true

  db_name  = "signalintel"
  username = var.master_username
  password = var.master_password
  port     = 5432

  db_subnet_group_name   = aws_db_subnet_group.this.name
  vpc_security_group_ids = [aws_security_group.postgres.id]
  parameter_group_name   = aws_db_parameter_group.timescaledb.name

  multi_az                = var.multi_az
  backup_retention_period = 7
  deletion_protection     = false # flip to true once this is backing anything real
  skip_final_snapshot     = true  # flip to false (and set final_snapshot_identifier) once this is backing anything real

  tags = var.tags
}
