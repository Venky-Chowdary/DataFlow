# Hybrid Connector Bridge

Use this pattern when the DataFlow control plane runs on Railway or in a managed SaaS account, but the customer has private VPC data sources (RDS, on-prem, VPC-only SaaS) that cannot be reached from the public internet.

## How it works

A small, read-only **Connector Bridge** agent runs inside the customer network. It does not expose any inbound port. Instead it opens an outbound-only WebSocket/mTLS tunnel to the control plane and exposes a local gRPC/HTTP proxy that the control plane can use to reach private databases and APIs.

```
┌──────────────────┐         outbound mTLS          ┌─────────────────────┐
│  Customer VPC    │      tunnel (no inbound ports)    │  DataFlow SaaS    │
│  ┌────────────┐  │  ──────────────────────────────▶ │  Control Plane    │
│  │   Bridge   │  │                                 │                   │
│  │   Agent    │  │  ◀────────────────────────────── │  schedules jobs   │
│  └─────┬──────┘  │         job requests              │                   │
│        │         │                                 │                   │
│   ┌────┴────┐     │                                 │                   │
│   │ Private │     │                                 │                   │
│   │  RDS    │     │                                 │                   │
│   └─────────┘     │                                 │                   │
└──────────────────┘                                 └─────────────────────┘
```

## Security properties

- Outbound-only: no public IP or NAT hole required in the customer VPC.
- mTLS: every connection is authenticated with short-lived, rotated client certificates.
- Scoped: the bridge is issued a token valid only for a single workspace and a configured allow-list of private endpoints.
- Audit: all proxy requests are logged to the control plane audit trail.

## Deployment

The bridge is packaged as a container (`ghcr.io/venky-chowdary/dataflow-bridge`). Deploy it in the customer VPC with:

- Environment variables `DATAFLOW_CONTROL_PLANE_URL` and `DATAFLOW_BRIDGE_TOKEN`.
- IAM role / kube service account granting no permissions beyond writing CloudWatch logs.
- Network access to the private endpoints in the allow-list.

For Terraform and Helm packaging of the bridge, see `deploy/enterprise/terraform/aws` and `deploy/enterprise/helm/dataflow`.
