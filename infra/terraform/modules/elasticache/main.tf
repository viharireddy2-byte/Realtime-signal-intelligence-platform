# Replaces the docker-compose `redis:7-alpine` container with the hot-path
# store query-api's /kpi endpoint reads from (see docs/ARCHITECTURE.md,
# "Why Redis *and* TimescaleDB").
resource "aws_elasticache_subnet_group" "this" {
  name       = "${var.name}-redis"
  subnet_ids = var.subnet_ids
}

resource "aws_security_group" "redis" {
  name_prefix = "${var.name}-redis-"
  vpc_id      = var.vpc_id
  tags        = var.tags
}

resource "aws_security_group_rule" "redis_from_allowed" {
  for_each = toset(var.allowed_security_group_ids)

  type                     = "ingress"
  from_port                = 6379
  to_port                  = 6379
  protocol                 = "tcp"
  security_group_id        = aws_security_group.redis.id
  source_security_group_id = each.value
}

resource "aws_security_group_rule" "redis_egress_all" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  security_group_id = aws_security_group.redis.id
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_elasticache_replication_group" "this" {
  replication_group_id = var.name
  description           = "Signal Intelligence Platform hot-path cache (query-api /kpi)"

  node_type          = var.node_type
  num_cache_clusters = var.num_cache_clusters
  engine             = "redis"
  engine_version     = "7.1"
  port               = 6379

  subnet_group_name = aws_elasticache_subnet_group.this.name
  security_group_ids = [aws_security_group.redis.id]

  automatic_failover_enabled = var.num_cache_clusters > 1
  at_rest_encryption_enabled = true
  transit_encryption_enabled = true

  tags = var.tags
}
