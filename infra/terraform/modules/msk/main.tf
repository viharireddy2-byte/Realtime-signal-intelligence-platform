# Managed Kafka (MSK), replacing the single-broker Confluent container
# docker-compose runs locally. TLS-in-transit is on; SASL/SCRAM auth is
# left as a follow-up (see docs/SECURITY.md's "enable SASL/SSL" note) --
# wiring it here means also wiring credential rotation via Secrets
# Manager, which is out of scope for a reference module like this one.
resource "aws_security_group" "msk" {
  name_prefix = "${var.name}-msk-"
  vpc_id      = var.vpc_id
  tags        = var.tags
}

resource "aws_security_group_rule" "msk_kafka_from_allowed" {
  for_each = toset(var.allowed_security_group_ids)

  type                     = "ingress"
  from_port                = 9092
  to_port                  = 9098 # covers plaintext, TLS, and SASL listener ports
  protocol                 = "tcp"
  security_group_id        = aws_security_group.msk.id
  source_security_group_id = each.value
}

resource "aws_security_group_rule" "msk_egress_all" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  security_group_id = aws_security_group.msk.id
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_msk_configuration" "this" {
  name              = "${var.name}-config"
  kafka_versions    = [var.kafka_version]
  server_properties = <<-PROPERTIES
    auto.create.topics.enable=true
    default.replication.factor=3
    min.insync.replicas=2
    num.partitions=6
    log.retention.hours=168
    compression.type=producer
  PROPERTIES
}

resource "aws_msk_cluster" "this" {
  cluster_name           = var.name
  kafka_version          = var.kafka_version
  number_of_broker_nodes = var.broker_count

  broker_node_group_info {
    instance_type   = var.broker_instance_type
    client_subnets  = var.subnet_ids
    security_groups = [aws_security_group.msk.id]

    storage_info {
      ebs_storage_info {
        volume_size = var.broker_ebs_volume_gb
      }
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.this.arn
    revision = aws_msk_configuration.this.latest_revision
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }

  enhanced_monitoring = "PER_TOPIC_PER_PARTITION"

  tags = var.tags
}
