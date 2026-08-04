# DataFlow Enterprise Operations Runbook

## Day-0: Deploy a new customer-dedicated environment

1. Bootstrap AWS credentials with permissions to create EKS, RDS, ElastiCache, DocumentDB, S3, KMS, IAM, VPC endpoints, and Secrets Manager.
2. Run Terraform:
   ```bash
   cd deploy/enterprise/terraform/aws
   terraform init
   terraform plan -var="domain=dataflow.example.com"
   terraform apply
   ```
3. Configure `kubectl`:
   ```bash
   aws eks update-kubeconfig --region us-east-1 --name $(terraform output -raw cluster_name)
   ```
4. Install the Helm chart with Terraform outputs:
   ```bash
   cd deploy/enterprise/helm/dataflow
   helm dependency update
   helm upgrade --install dataflow . \
     --namespace dataflow --create-namespace \
     --values values-production.yaml \
     --set serviceAccount.annotations."eks\.amazonaws\.com/role-arn"=$(terraform output -raw irsa_role_arn) \
     --set data.rds.host=$(terraform output -raw rds_cluster_endpoint) \
     --set data.redis.host=$(terraform output -raw redis_primary_endpoint) \
     --set data.documentdb.host=$(terraform output -raw documentdb_endpoint) \
     --set objectStore.s3.bucket=$(terraform output -raw s3_data_bucket) \
     --set global.domain=dataflow.example.com \
     --set externalSecrets.remoteSecretName=$(terraform output -raw secrets_manager_secret_name)
   ```

## Day-1: Scaling

- API/Web scale horizontally via HPA on CPU/memory.
- Workers scale horizontally via HPA; for CDC back-pressure, prefer scale-up over batch size increases.
- Database scaling: Aurora Serverless v2 can be enabled for variable load; DocumentDB instances can be added.

## Backup / DR

- RDS Aurora: automated backups retained 35 days, cross-region snapshot copy to `s3-backups` bucket.
- DocumentDB: daily `mongodump` CronJob to `s3-backups` bucket.
- S3: versioning + cross-region replication + object lock.
- Failover: restore Terraform state in secondary region, promote cross-region Aurora read replica and DocumentDB snapshot.

## Certificate / secret rotation

1. Rotate `DATAFLOW_AUTH_SECRET` and `DATAFLOW_SECRETS_KEY` in Secrets Manager.
2. Trigger a rolling restart of API + worker pods.
3. Re-encrypt existing connector credentials by re-saving integrations (documented in user guide).

## Incident response

- API readiness: `/health/ready` returns 503 until all warm-up checks pass.
- Worker queue depth: scrape `dataflow_worker_queue_depth` metric from `/metrics`.
- CDC lag: scrape `dataflow_cdc_lag_seconds`.
- Audit logs: shipped to CloudWatch and S3 `audit` bucket.

## Security hardening

- Set `networkPolicy.enabled=true` and restrict `ingress`/`egress` to required CIDRs.
- Enable `enable_istio=true` in Terraform for mTLS between pods (requires Istio installed separately).
- DocumentDB `MONGODB_URL` uses `ssl=false` in the default template; for production, either use a custom parameter group with TLS disabled or mount the AWS CA bundle and set `tls=true`.
- Enable AWS WAF on the ALB and restrict `ingress` source IPs.

## Custom domain + CORS (tenant vanity host)

- API env: `DATAFLOW_CORS_ORIGINS` (comma-separated) must include the Studio origin
  (`https://app.example.com`) and any tenant custom domain (`https://data.acme.com`).
- Custom domain DNS CNAME → platform ingress; TLS via cert-manager / ACM.
- After DNS cutover, verify:
  1. `OPTIONS` preflight on `/api/health` returns `Access-Control-Allow-Origin` for the vanity host.
  2. Browser Studio login cookie / bearer flow against the vanity host (SameSite=None + Secure when cross-site).
- Honesty: vanity host is routing + CORS allowlist — not a separate multi-tenant isolation boundary.
  Workspace RBAC / resource ACLs still gate data.

## Audit tip anchors (WORM honesty)

- `GET /audit/tip` returns the current HMAC chain tip + optional external anchor metadata.
- `POST /audit/tip` (requires `WORKSPACE_MANAGE`) records an **external anchor stub**
  (hash reference + timestamp). This is **not** Amazon S3 Object Lock, Glacier Vault Lock,
  or a TSA timestamp authority.
- Production WORM: ship tip hashes to an immutable store (S3 Object Lock / Azure WORM /
  vendor TSA) out-of-band, then POST the receipt id back into the stub fields for auditors.
- Do not claim SOC2 control evidence from the stub alone — auditor letters remain org-owned.
