variable "environment" {
  description = "Deployment environment (prod, staging, dev)"
  type        = string
  default     = "prod"

  validation {
    condition     = contains(["prod", "staging", "dev"], var.environment)
    error_message = "Environment must be prod, staging, or dev."
  }
}

variable "domain" {
  description = "Base domain for the deployment (e.g. dataflow.example.com)"
  type        = string
}

variable "region" {
  description = "AWS region for the primary deployment"
  type        = string
  default     = "us-east-1"
}

variable "secondary_region" {
  description = "AWS region for cross-region backup / DR"
  type        = string
  default     = "us-west-2"
}

variable "vpc_cidr" {
  description = "CIDR block for the customer VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "availability_zones" {
  description = "List of AZs to use"
  type        = list(string)
  default     = ["a", "b", "c"]
}

variable "enable_private_link" {
  description = "Create VPC endpoints for AWS services and PrivateLink for supported SaaS"
  type        = bool
  default     = true
}

variable "enable_istio" {
  description = "Install Istio service mesh for mTLS and traffic management"
  type        = bool
  default     = false
}

variable "api_replicas" {
  description = "Initial API pod replica count"
  type        = number
  default     = 3
}

variable "worker_replicas" {
  description = "Initial worker pod replica count"
  type        = number
  default     = 2
}

variable "api_image" {
  description = "DataFlow API container image"
  type        = string
  default     = "ghcr.io/venky-chowdary/dataflow-api:latest"
}

variable "web_image" {
  description = "DataFlow Web container image"
  type        = string
  default     = "ghcr.io/venky-chowdary/dataflow-web:latest"
}

variable "secrets_manager_prefix" {
  description = "Prefix for AWS Secrets Manager secret names"
  type        = string
  default     = "dataflow"
}

variable "tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default     = {}
}
