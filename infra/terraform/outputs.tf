output "eks_cluster_name" {
  description = "Pass to `aws eks update-kubeconfig --name <this>` before `helm upgrade --install`."
  value       = module.eks.cluster_name
}

output "eks_cluster_endpoint" {
  value = module.eks.cluster_endpoint
}

output "kafka_bootstrap_brokers_tls" {
  description = "Set as KAFKA_BOOTSTRAP_SERVERS (via infra/helm's values.yaml config.kafka.bootstrapServers) once TLS auth is wired into the client config -- see docs/SECURITY.md."
  value       = module.msk.bootstrap_brokers_tls
}

output "redis_primary_endpoint" {
  value = module.redis.primary_endpoint_address
}

output "timescaledb_endpoint" {
  value = module.timescaledb.endpoint
}
