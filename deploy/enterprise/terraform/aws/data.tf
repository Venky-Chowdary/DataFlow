# Data platform: S3 private buckets, RDS Aurora PostgreSQL, ElastiCache Redis,
# and DocumentDB (MongoDB-compatible) for job history / audit.

# ------------------------------
# S3
# ------------------------------
resource "aws_s3_bucket" "data" {
  bucket_prefix = "${local.name}-data-"
  force_destroy = var.environment != "prod"

  tags = local.tags
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.data.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_versioning" "data" {
  bucket = aws_s3_bucket.data.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket = aws_s3_bucket.data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket" "backups" {
  bucket_prefix = "${local.name}-backups-"
  provider      = aws.secondary

  tags = merge(local.tags, { Region = var.secondary_region })
}

resource "aws_s3_bucket_server_side_encryption_configuration" "backups" {
  provider = aws.secondary
  bucket   = aws_s3_bucket.backups.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.data_secondary.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_object_lock_configuration" "backups" {
  provider = aws.secondary
  bucket   = aws_s3_bucket.backups.id

  rule {
    default_retention {
      mode = "COMPLIANCE"
      days = 30
    }
  }
}

resource "aws_s3_bucket" "audit" {
  bucket_prefix = "${local.name}-audit-"

  tags = local.tags
}

resource "aws_s3_bucket_server_side_encryption_configuration" "audit" {
  bucket = aws_s3_bucket.audit.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.data.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_object_lock_configuration" "audit" {
  bucket = aws_s3_bucket.audit.id

  rule {
    default_retention {
      mode = "COMPLIANCE"
      days = 365
    }
  }
}

# ------------------------------
# KMS
# ------------------------------
resource "aws_kms_key" "data" {
  description             = "DataFlow data encryption key"
  deletion_window_in_days = 30
  enable_key_rotation     = true

  tags = local.tags
}

resource "aws_kms_alias" "data" {
  name          = "alias/${local.name}-data"
  target_key_id = aws_kms_key.data.key_id
}

resource "aws_kms_key" "data_secondary" {
  provider                  = aws.secondary
  description             = "DataFlow secondary region data encryption key"
  deletion_window_in_days = 30
  enable_key_rotation     = true

  tags = merge(local.tags, { Region = var.secondary_region })
}

# ------------------------------
# RDS Aurora PostgreSQL
# ------------------------------
resource "aws_db_subnet_group" "dataflow" {
  name       = "${local.name}-db"
  subnet_ids = module.vpc.database_subnets

  tags = local.tags
}

resource "aws_security_group" "rds" {
  name_prefix = "${local.name}-rds-"
  description = "Allow PostgreSQL access from EKS nodes"
  vpc_id      = module.vpc.vpc_id

  ingress {
    description     = "PostgreSQL from EKS cluster SG"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [module.eks.cluster_security_group_id]
  }

  tags = local.tags
}

module "rds_aurora" {
  source  = "terraform-aws-modules/rds-aurora/aws"
  version = "~> 9.10"

  name        = "${local.name}-postgres"
  engine      = "aurora-postgresql"
  engine_mode = "provisioned"
  engine_version = "16.1"

  database_name = "dataflow"

  instance_class = "db.t4g.medium"
  instances = {
    one = {}
    two = {}
  }

  vpc_id               = module.vpc.vpc_id
  db_subnet_group_name = aws_db_subnet_group.dataflow.name
  security_group_rules = {
    ingress = {
      source_security_group_id = module.eks.cluster_security_group_id
    }
  }

  storage_encrypted = true
  kms_key_id        = aws_kms_key.data.arn

  backup_retention_period = 35
  skip_final_snapshot     = var.environment != "prod"
  deletion_protection     = var.environment == "prod"

  tags = local.tags
}

# ------------------------------
# ElastiCache Redis
# ------------------------------
resource "aws_elasticache_subnet_group" "dataflow" {
  name       = "${local.name}-redis"
  subnet_ids = module.vpc.database_subnets
}

resource "aws_security_group" "redis" {
  name_prefix = "${local.name}-redis-"
  description = "Allow Redis access from EKS nodes"
  vpc_id      = module.vpc.vpc_id

  ingress {
    description     = "Redis from EKS cluster SG"
    from_port       = 6379
    to_port         = 6379
    protocol        = "tcp"
    security_groups = [module.eks.cluster_security_group_id]
  }

  tags = local.tags
}

resource "aws_elasticache_replication_group" "dataflow" {
  replication_group_id = "${local.name}-redis"
  description          = "DataFlow Redis cluster"

  engine               = "redis"
  engine_version       = "7.1"
  node_type            = "cache.t4g.medium"
  num_cache_clusters   = 2
  parameter_group_name = "default.redis7"
  port                 = 6379

  automatic_failover_enabled = true
  multi_az_enabled           = true

  subnet_group_name  = aws_elasticache_subnet_group.dataflow.name
  security_group_ids = [aws_security_group.redis.id]

  at_rest_encryption_enabled = true
  transit_encryption_enabled = true
  auth_token           = random_password.redis_password.result

  snapshot_retention_limit = 7

  tags = local.tags
}

# Primary application secrets; per-tenant secrets are created by the app at runtime.
resource "aws_secretsmanager_secret" "dataflow" {
  name_prefix = "${var.secrets_manager_prefix}/master-"
  description = "DataFlow master secrets for ${var.environment}"

  kms_key_id = aws_kms_key.data.arn

  tags = local.tags
}

resource "aws_secretsmanager_secret_version" "dataflow" {
  secret_id     = aws_secretsmanager_secret.dataflow.id
  secret_string = jsonencode({
    DATAFLOW_AUTH_SECRET  = random_password.auth_secret.result
    DATAFLOW_SECRETS_KEY  = random_password.secrets_key.result
    POSTGRES_PASSWORD     = module.rds_aurora.cluster_master_password
    MONGODB_PASSWORD      = random_password.mongodb_password.result
    REDIS_PASSWORD        = aws_elasticache_replication_group.dataflow.auth_token
  })
}

resource "random_password" "auth_secret" {
  length  = 64
  special = true
}

resource "random_password" "secrets_key" {
  length  = 32
  special = true
}

resource "random_password" "mongodb_password" {
  length  = 32
  special = true
}

resource "random_password" "redis_password" {
  length  = 32
  special = false
}

# ------------------------------
# DocumentDB (MongoDB-compatible)
# ------------------------------
resource "aws_docdb_subnet_group" "dataflow" {
  name       = "${local.name}-docdb"
  subnet_ids = module.vpc.database_subnets

  tags = local.tags
}

resource "aws_security_group" "docdb" {
  name_prefix = "${local.name}-docdb-"
  description = "Allow DocumentDB access from EKS nodes"
  vpc_id      = module.vpc.vpc_id

  ingress {
    description     = "DocumentDB from EKS cluster SG"
    from_port       = 27017
    to_port         = 27017
    protocol        = "tcp"
    security_groups = [module.eks.cluster_security_group_id]
  }

  tags = local.tags
}

resource "aws_docdb_cluster" "dataflow" {
  cluster_identifier     = "${local.name}-docdb"
  engine                 = "docdb"
  engine_version         = "5.0"
  master_username        = "dataflow"
  master_password        = random_password.mongodb_password.result
  db_subnet_group_name   = aws_docdb_subnet_group.dataflow.name
  vpc_security_group_ids = [aws_security_group.docdb.id]

  storage_encrypted = true
  kms_key_id        = aws_kms_key.data.arn

  backup_retention_period = 35
  preferred_backup_window = "03:00-04:00"
  skip_final_snapshot     = var.environment != "prod"
  deletion_protection   = var.environment == "prod"

  tags = local.tags
}

resource "aws_docdb_cluster_instance" "dataflow" {
  count              = var.environment == "prod" ? 2 : 1
  identifier         = "${local.name}-docdb-${count.index + 1}"
  cluster_identifier = aws_docdb_cluster.dataflow.id
  instance_class     = "db.t4g.medium"

  tags = local.tags
}
