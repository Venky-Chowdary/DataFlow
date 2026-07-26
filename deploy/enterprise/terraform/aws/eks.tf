# EKS cluster with separate node groups for API/Web, workers, and observability.
# API/Web run on SPOT where acceptable; workers run on ON_DEMAND for CDC/job reliability.

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.24"

  cluster_name    = local.name
  cluster_version = "1.30"

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  cluster_endpoint_public_access = true
  cluster_endpoint_private_access = true

  enable_cluster_creator_admin_permissions = true

  cluster_security_group_additional_rules = {
    ingress_nodes_ephemeral_ports_tcp = {
      description                = "Nodes on ephemeral ports"
      protocol                   = "tcp"
      from_port                  = 1025
      to_port                    = 65535
      type                       = "ingress"
      source_node_security_group = true
    }
  }

  eks_managed_node_groups = {
    api = {
      name           = "api"
      instance_types = ["m6i.large", "m6i.xlarge"]

      min_size     = 2
      max_size     = 10
      desired_size = 3

      capacity_type = "SPOT"

      labels = {
        workload = "api"
      }

      update_config = {
        max_unavailable_percentage = 25
      }
    }

    worker = {
      name           = "worker"
      instance_types = ["m6i.xlarge", "m6i.2xlarge"]

      min_size     = 2
      max_size     = 20
      desired_size = 2

      capacity_type = "ON_DEMAND"

      labels = {
        workload = "worker"
      }

      taints = [{
        key    = "dedicated"
        value  = "worker"
        effect = "NO_SCHEDULE"
      }]
    }
  }

  # Encrypt etcd and enable audit logs.
  cluster_enabled_log_types = ["api", "audit", "authenticator", "controllerManager", "scheduler"]

  tags = local.tags
}

# Allow EKS nodes to reach AWS services via IRSA instead of long-lived credentials.
module "dataflow_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "~> 5.44"

  role_name = "${local.name}-node"

  role_policy_arns = {
    s3            = aws_iam_policy.dataflow_s3.arn
    secrets       = aws_iam_policy.dataflow_secrets.arn
    cloudwatch    = "arn:${data.aws_partition.current.partition}:iam::aws:policy/CloudWatchAgentServerPolicy"
    amazon_managed = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonSSMManagedInstanceCore"
  }

  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["dataflow:dataflow"]
    }
  }

  tags = local.tags
}

resource "aws_iam_policy" "dataflow_s3" {
  name_prefix = "${local.name}-s3-"
  description = "DataFlow access to private S3 buckets"
  policy      = data.aws_iam_policy_document.dataflow_s3.json

  tags = local.tags
}

data "aws_iam_policy_document" "dataflow_s3" {
  statement {
    sid    = "ListDataFlowBuckets"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:ListBucketVersions",
    ]
    resources = [
      aws_s3_bucket.data.arn,
      aws_s3_bucket.backups.arn,
      aws_s3_bucket.audit.arn,
    ]
  }

  statement {
    sid    = "ReadWriteDataFlowBuckets"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
    ]
    resources = [
      "${aws_s3_bucket.data.arn}/*",
      "${aws_s3_bucket.backups.arn}/*",
      "${aws_s3_bucket.audit.arn}/*",
    ]
  }
}

resource "aws_iam_policy" "dataflow_secrets" {
  name_prefix = "${local.name}-secrets-"
  description = "DataFlow read access to per-tenant secrets"
  policy      = data.aws_iam_policy_document.dataflow_secrets.json

  tags = local.tags
}

data "aws_iam_policy_document" "dataflow_secrets" {
  statement {
    sid    = "ReadTenantSecrets"
    effect = "Allow"
    actions = [
      "secretsmanager:GetSecretValue",
      "secretsmanager:DescribeSecret",
    ]
    resources = [
      "arn:${data.aws_partition.current.partition}:secretsmanager:${var.region}:${local.account_id}:secret:${var.secrets_manager_prefix}/*",
    ]
  }
}
