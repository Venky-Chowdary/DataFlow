# DataWrap — Product Architecture (Current System)

**Status:** Phase E5 refresh — matches code as of 2026-08-08  
**Identity:** Migration Assurance Platform (heterogeneous move + prove), not an ELT SaaS fleet.

Companion docs: `PRODUCT_SCOPE.md`, `BUYER_EVIDENCE_PACK.md`, `CONVERSION_CONTRACT.md`, `MIGRATION_ASSURANCE_PLATFORM_PLAN.md`, `IDENTITY_PERSISTENCE_ROADMAP.md`.

---

## Planes

```
┌─────────────────────────────────────────────────────────────┐
│  apps/web — Transfer Studio (Map / Validate / Execute)      │
│  Decision Artifact hash surfaced; honesty controls on Validate│
└────────────────────────────┬────────────────────────────────┘
                             │ REST + SSE
┌────────────────────────────▼────────────────────────────────┐
│  apps/api — FastAPI                                         │
│  Middleware: Tenant (Host hint + IP) → Auth (jti sessions)  │
│              → RBAC → routers                               │
├─────────────────────────────────────────────────────────────┤
│  Migration Decision Kernel (`services/decision_kernel/`)    │
│  Immutable Decision Artifact → conversion / invent / risk / │
│  validation / proof / execute_gate                          │
├─────────────────────────────────────────────────────────────┤
│  Preflight G1–G9 · semantic_mapper · type_system            │
│  UniversalTransferEngine · stream · writers · CDC           │
│  Reconciliation (full SHA-256 fingerprint) · quarantine     │
├─────────────────────────────────────────────────────────────┤
│  Connectors (~35 distinct engines certified via SKU path)   │
│  Catalog tiles ≫ engines — honesty via enrich_catalog_entry │
└─────────────────────────────────────────────────────────────┘
         MongoDB (jobs/checkpoints) · Redis (CDC leases)
```

---

## TODAY (code-true)

| Capability | Reality |
|------------|---------|
| Transfer | Multi-driver `UniversalTransferEngine`; Map → Validate → Execute |
| Decision Artifact | Stamped on Validate; Execute refuses without it (skip_preflight may inline-stamp) |
| Types | Canonical logical vocabulary with integer/float **width**; invent never silently narrows |
| Mapping | BM25 + Hungarian + calibration; post-greedy label is `hungarian_with_greedy_patch` |
| Proof | Full-population order-independent checksum (64 hex SHA-256); sample ≠ population |
| CDC | At-least-once upsert; peek-poll transport (streaming = Phase F) |
| Auth | Env users + jti sessions + logout revoke; tenant Host bind in production |
| Secrets | `SECRETS_KEY` HKDF Fernet; decrypt raises (no `[decryption-failed]` password) |
| Catalog | `unique_drivers` is the public live count; hosted twins are `is_hosted_alias` |
| SaaS certified | Salesforce + HubSpot only until incremental/OAuth/Retry-After proven |
| Scheduler | Process-local thread pool — **not** multi-replica safe yet (Phase F5) |

---

## NOT TODAY (do not document as shipped)

- Exactly-once CDC
- Horizontal distributed scheduler
- Auditor-issued SOC 2 / HIPAA / ISO
- “Any SaaS” or catalog tile count as live engines
- Debezium-class replication connection throughput

---

## Decision Kernel (SSOT)

All invent, conversion class, risk, validation buckets, and execute gates consume the same immutable **Decision Artifact**. Writers and UI must not fork datatype or risk rules.

Package: `apps/api/services/decision_kernel/`  
Facades: `types`, `conversion`, `ddl`, `structural`, `invent`, `risk`, `proof`, `execute_gate`.

---

## Screens (operator)

| Screen | Status |
|--------|--------|
| Overview / Jobs | Built |
| Transfer Studio (Map / Validate / Execute) | Built — Decision Artifact path |
| Connectors (certified + Planned roadmap) | Built — honesty enrichment |
| Schedules | Built (single-process) |
| Pilot / Copilot | Built — SQL identifier allow-list (D6) |
| Settings (SSO, API keys, audit) | Built — identity still env-primary (D7 roadmap) |
| Benchmarks / proof artifacts | Partial — matrix reports in CI |

---

## Certified inventory

- **Engines:** `catalog_summary.unique_drivers` / `transfer_live_driver_types()`
- **Routes:** `PRODUCTION_SKU` in `src/transfer/registry.py`
- **Published matrix:** `apps/api/data/proofs/transfer_ready_matrix.json` (script: `scripts/transfer_ready_matrix_report.py`)

Tile count in `connector_catalog.json` is a **marketplace + roadmap** surface, not transfer-live.

---

## Security posture (Phase D)

| Control | Implementation |
|---------|----------------|
| Public bootstrap | No email enumeration |
| Login | Rate limit + lockout |
| Client IP | Trusted-proxy hop count (default 0 → ignore XFF spoof) |
| Tenant | Host hint must match identity `tenant_ids` when bind strict |
| Sessions | Server-side `jti` + `/auth/logout` |
| Secrets | Separated from `AUTH_SECRET`; HKDF; decrypt raises |

---

## Evolution

Follow `MIGRATION_ASSURANCE_PLATFORM_PLAN.md` Phases F–G for CDC streaming, bulk readers, distributed scheduling, and module decomposition. Architecture docs that still describe “MongoDB-only TODAY + legacy PG/Snowflake API” are obsolete — this file supersedes them.
