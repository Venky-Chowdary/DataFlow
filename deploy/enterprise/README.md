# DataFlow Enterprise Deployment

This directory contains customer-dedicated deployment packaging for enterprise environments. It is a reference implementation for running DataFlow in a **single-tenant VPC** on AWS with EKS, AWS PrivateLink endpoints for AWS services, per-tenant secrets, SSO/SAML, and auditable backup primitives. SaaS PrivateLink, customer-managed mTLS meshes, and validated DR SLAs require additional customer-specific configuration and drills.

Two deployment models are supported:

1. **Customer Dedicated (recommended for enterprises with private data sources)**  
   The entire control plane + data plane runs in the customer’s AWS account / VPC. DataFlow engineers operate it via GitOps, or the customer self-manages it with this packaging.

2. **Enterprise Cloud (managed SaaS with private networking)**  
   Control plane runs in DataFlow’s AWS account; a small **Connector Bridge** agent runs in the customer VPC and reaches private sources. See `docs/hybrid-connector-bridge.md`.

## Why not Railway for private enterprise data?

Railway works well for public SaaS/API and cloud data warehouses with public endpoints. It cannot guarantee static outbound IPs or private VPC reachability, so it is **not** suitable for customers with private RDS, on-prem, or VPC-only SaaS. This packaging provides the infrastructure primitives for those cases; customer-side network peering, PrivateLink acceptance, and egress allowlisting must be completed before transfers to private endpoints are live.

## Quick start

```bash
cd deploy/enterprise/terraform/aws
terraform init
terraform plan -var="domain=dataflow.example.com" -var="environment=prod"
terraform apply

cd ../../helm/dataflow
helm dependency update
helm upgrade --install dataflow . \
  --namespace dataflow --create-namespace \
  --values values-production.yaml
```

## Security defaults

- All pods run as non-root, read-only root FS, drop all capabilities.
- NetworkPolicies enforce least-privilege pod-to-pod traffic.
- Secrets are never mounted as plain env vars; they are injected via AWS Secrets Manager CSI driver or External Secrets Operator.
- Ingress terminates TLS with ACM and enforces HSTS/CSP/Frame-Options via AWS WAF or nginx.
- Inter-service traffic is isolated with AWS VPC CNI network policies. Istio/Linkerd mTLS is optional (default `enable_istio=false`) and must be installed and verified separately.
- SSO/SAML 2.0 or OIDC is required for console access; local password auth can be disabled.
- Audit logs are shipped to CloudWatch/S3 with object lock and 1-year retention.

## Topology

```
Customer VPC
├── Public subnets (Bastion, NLB, NAT)
├── Private subnets (EKS worker nodes)
├── Database subnets (RDS PostgreSQL, ElastiCache Redis, DocumentDB)
├── S3 private buckets (uploads, backups, vector store, audit logs) with versioning, KMS, and object lock
├── AWS VPC endpoints for AWS services (S3, ECR, CloudWatch, Secrets Manager, KMS)
└── Optional PrivateLink endpoints for SaaS providers (requires partner-hosted endpoint services)

EKS cluster
├── dataflow-api (HPA, PDB, ServiceAccount IRSA)
├── dataflow-web (static + nginx)
├── dataflow-worker (CDC + batch execution, separate node group)
├── dataflow-scheduler (cron / background)
└── observability (Prometheus/Grafana/Fluent Bit)

Managed data plane
├── RDS Aurora PostgreSQL (state, jobs, catalog)
├── ElastiCache Redis (leases, queues, cache)
├── DocumentDB (job history, audit)
└── S3 (object store, backups with cross-region replication)
```

## Disaster recovery

- RDS Aurora: cross-region read replica + automated daily snapshots to S3 with object lock.
- MongoDB/DocumentDB: daily `mongodump` to S3, point-in-time restore.
- S3: versioning + cross-region replication + object lock enabled; replication rules must be completed and validated in each account.
- RTO/RPO are customer-configurable; the packaging provides automated snapshots and backups, but target SLAs must be proven with restore drills.

## Operations

See `docs/OPERATIONS.md` for day-2 runbooks: scaling, certificate rotation, secret rotation, patching, incident response.
