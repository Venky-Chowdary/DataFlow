output "cluster_name" {
  description = "EKS cluster name"
  value       = module.eks.cluster_name
}

output "cluster_endpoint" {
  description = "EKS cluster endpoint"
  value       = module.eks.cluster_endpoint
}

output "cluster_certificate_authority_data" {
  description = "EKS cluster CA certificate (base64)"
  value       = module.eks.cluster_certificate_authority_data
}

output "oidc_provider_arn" {
  description = "OIDC provider ARN for IRSA"
  value       = module.eks.oidc_provider_arn
}

output "vpc_id" {
  description = "Customer VPC ID"
  value       = module.vpc.vpc_id
}

output "private_subnets" {
  description = "Private subnet IDs"
  value       = module.vpc.private_subnets
}

output "database_subnets" {
  description = "Database subnet IDs"
  value       = module.vpc.database_subnets
}

output "rds_cluster_endpoint" {
  description = "RDS Aurora PostgreSQL writer endpoint"
  value       = module.rds_aurora.cluster_endpoint
}

output "rds_cluster_reader_endpoint" {
  description = "RDS Aurora PostgreSQL reader endpoint"
  value       = module.rds_aurora.cluster_reader_endpoint
}

output "redis_primary_endpoint" {
  description = "ElastiCache Redis primary endpoint"
  value       = aws_elasticache_replication_group.dataflow.primary_endpoint_address
}

output "documentdb_endpoint" {
  description = "DocumentDB cluster endpoint"
  value       = try(aws_docdb_cluster.dataflow.endpoint, "")
}

output "s3_data_bucket" {
  description = "S3 bucket for uploads/object store"
  value       = aws_s3_bucket.data.id
}

output "s3_backups_bucket" {
  description = "S3 bucket for cross-region backups"
  value       = aws_s3_bucket.backups.id
}

output "s3_audit_bucket" {
  description = "S3 bucket for audit logs"
  value       = aws_s3_bucket.audit.id
}

output "kms_key_arn" {
  description = "KMS key ARN for data encryption"
  value       = aws_kms_key.data.arn
}

output "secrets_manager_secret_name" {
  description = "AWS Secrets Manager secret name for master secrets"
  value       = aws_secretsmanager_secret.dataflow.name
}

output "irsa_role_arn" {
  description = "IAM role ARN for DataFlow service account (IRSA)"
  value       = module.dataflow_irsa.iam_role_arn
}
