output "endpoint" {
  value = aws_db_instance.this.endpoint
}

output "address" {
  value = aws_db_instance.this.address
}

output "security_group_id" {
  value = aws_security_group.postgres.id
}
