locals {
  name = "dataflow-${var.environment}"
  azs  = [for az in var.availability_zones : "${var.region}${az}"]

  tags = merge(var.tags, {
    Environment = var.environment
    ManagedBy   = "terraform"
    Project     = "dataflow"
  })

  account_id = data.aws_caller_identity.current.account_id
}

data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}
data "aws_availability_zones" "available" {
  state = "available"
}
