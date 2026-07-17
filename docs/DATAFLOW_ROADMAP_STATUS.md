# DataFlow Research-Report Gap Status

**Report:** `dataflow_research_report.md` (July 2026)  
**Branch:** `devin/backend-hardening-p5`  
**Last updated:** 2026-07-11  

This document tracks every item from the market/algorithm research report against the current codebase so progress is transparent and the remaining work is explicit.

---

## Executive summary

Backend batch reliability is now solid:

- `pytest tests`: **2003 passed / 182 skipped / 0 failed** (clean after Elasticsearch shard cleanup)
- 5×5 production SKU matrix verified
- Universal route matrix: **529 passed / 121 skipped**
- Full backend test suite green on PR #16

The product is **beta / early production** for batch transfers on supported drivers. It is **not yet a 10/10 Airbyte/Fivetran-scale platform** because the remaining gaps are architectural and require multi-week implementation (CDC at parity, vector destinations, GitOps, Iceberg, broad cloud validation).

---

## G1: Real-time CDC (log-based)

**Target:** Debezium-style log-based capture from Postgres, MySQL, MongoDB, SQL Server, Oracle.

### Status: partial / in progress

| What is implemented | Where |
|---------------------|-------|
| Query-cursor CDC fallback | `src/transfer/cdc_transfer.py` |
| MongoDB Change Streams CDC | `connectors/mongodb_change_stream.py` |
| PostgreSQL logical decoding (`test_decoding`) CDC | `connectors/postgresql_change_stream.py` |
| MySQL binlog CDC (`python-mysql-replication`) | `connectors/mysql_change_stream.py` |
| `ChangeBatch` with `resume_token` | `services/cdc_engine.py` |
| Watermark persistence | `services/sync_cursor.py`, `services/atomic_file.py` |

### What is still missing

- **Debezium Engine / `pgoutput` integration** for PostgreSQL. The current implementation uses `test_decoding`, which is simpler but does not give the structured change events that `pgoutput` provides and does not handle `TOAST` columns cleanly.
- **Consistent snapshot + streaming LSN handoff:** the current path snapshots then polls, but does not prove that no gap exists between the snapshot end and the stream start. Production Debezium uses a transaction snapshot and starts streaming from the exact LSN captured at snapshot time.
- **Incremental snapshots (DBLog algorithm)** for large tables so backfills do not lock the source.
- **Replication-slot lag monitoring** for Postgres to prevent unconsumed WAL from filling disk.
- **Schema-change events** carried through the same stream so the mapper can re-run semantic mapping on drift instead of failing.
- **Exactly-once materializations** via idempotent upsert on `_lsn` or merge on PK.
- **CDC for SQL Server CT/CDC tables and Oracle LogMiner**.

### Why it matters

This is the #1 disqualifier in 2026 evaluations. Batch-only or cursor-polling is acceptable for analytics, but operational use cases expect near real-time CDC.

### Recommended next step

Embed Debezium Engine (or Debezium Server without Kafka) behind the existing connector adapter contract. Maintain the current query-cursor CDC as a fallback tier selected by endpoint capability probing.

---

## G2: Vectorization / RAG destinations

**Target:** Move data into vector DBs so it is AI-ready.

### Status: not started

| What exists today | Where |
|-------------------|-------|
| Internal ChromaDB RAG store for mapping suggestions | `packages/ml` / `services` |
| Sentence-transformers fallback | `packages/ml` |
| PDF/Word structure parsing | `services/document_parser.py` |

### What is missing

- **pgvector destination writer** (default under ~5M vectors).
- **Pinecone, Qdrant, Weaviate, Milvus destinations**.
- **Structure-aware / semantic chunking** reuse of existing PDF/Word parsers.
- **Semantic-type-driven field routing:** e.g., a `text` column becomes embedded text, a `pii` column is excluded, metadata columns become filterable metadata.
- **Embedding cache by content hash** to avoid re-embedding unchanged rows.
- **OpenAI / Cohere managed embedding option**.

### Why it matters

Airbyte ships five vector destinations and an official RAG pipeline guide. DataFlow can beat them because its semantic mapper already knows what each column means and can auto-decide embed vs metadata vs exclude.

### Recommended next step

Start with a `pgvector` destination writer using PostgreSQL + `vector` extension, then build a `services/vectorization.py` chunking/embedding service that is reused by all vector destinations.

---

## G3: Data contracts + circuit breakers

**Target:** Convert preflight gates into versioned, enforceable, CI-reviewable contracts with fail-closed breaker states.

### Status: partially implemented / in active hardening

| What is implemented | Where |
|---------------------|-------|
| `DataContract`, `CircuitBreaker`, `ContractEnforcer` primitives | `services/data_contract.py` |
| Contract persistence (MongoDB + in-memory fallback) | `services/contract_store.py` |
| Contract lifecycle API | `src/routers/contracts_router.py` |
| Contract creation/enforcement in transfer engine | `src/transfer/contract_engine.py` |
| Circuit-breaker gate before transfer execution | `src/transfer/contract_engine.py` |
| Preflight gates G1-G8 | `services/preflight_service.py`, `packages/preflight` |
| Quarantine / rejected rows / dead-letter details | `src/transfer/engine.py` |

### What is still missing

- **YAML artifact + GitOps export** so contracts can be reviewed and versioned.
- **Pre-ingestion quarantine staging table** that is promoted only after validation passes.
- **Dead-letter table/collection** per transfer for row-level rejects with full context.
- **Composite trust score** (completeness, freshness, drift, violation rate) surfaced per job.
- **Breaker state machine exposed in UI:** CLOSED / OPEN / HALF-OPEN with manual reset.
- **Contract signing workflow** so a data owner approves the auto-generated contract before the pipeline runs unattended.

### Recommended next step

Export `build_contract_from_preflight` output as `dataflow-contract.yaml` and add `POST /api/v1/contracts/{id}/sign` enforcement toggle in the scheduler.

---

## G4: Unstructured data intelligence

**Target:** Extract typed rows and chunks from PDF/Word/HTML with semantic type inference.

### Status: partially implemented

- PDF/Word parsing exists and is used in the mapping layer.
- File→DB transfer can accept PDF/Word uploads.
- No end-to-end extraction + chunking + embedding pipeline yet.

### Why it matters

Combining extraction, semantic typing, and embedding is whitespace no ELT vendor owns.

### Recommended next step

Build `services/document_chunking.py` that produces chunks with provenance (page, heading, table) and feeds `services/vectorization.py`.

---

## G5: GitOps / DataFlow-as-Code

**Target:** Pipelines as code, versioned and CI-tested.

### Status: not started

- No `dataflow.yaml` spec.
- No CLI `plan/apply`.
- No export of UI-built pipelines to YAML.

### Why it matters

Enterprise buyers want reviewable pipeline definitions and drift detection in CI. Meltano proved demand but lacks DataFlow's UI/AI story.

### Recommended next step

Define `dataflow.yaml` schema (`source`, `contract`, `mapping`, `destination`, `schedule`) and a CLI entrypoint in `apps/cli`.

---

## G6: Semantic registry / OSI export

**Target:** Org-wide ontology of 210 semantic types with OSI v1.0 interchange.

### Status: partially implemented

- `services/pattern_engine.py` and `services/semantic_type_service.py` define semantic types.
- RAG store learns from human corrections.
- No public OSI export, no versioned tenant ontology.

### Recommended next step

Add `GET /api/v1/semantic/export?format=osi` and version the type registry per tenant.

---

## G7: Explainability + undo

**Target:** "Why did the AI map this?" + one-click rollback.

### Status: partially implemented

- Pipeline explanations exist in `services/pipeline_explanation.py`.
- No per-mapping evidence record.
- No transfer-level rollback via staging-table swap or Iceberg branch.

### Recommended next step

Make `_auto_map` return an `evidence` field for each mapping and store it on the job record.

---

## G8: Iceberg / lakehouse destination

**Target:** Apache Iceberg v3 writer with REST catalog, schema evolution, time travel.

### Status: not started

- BigQuery, Snowflake, S3, GCS, ADLS writers exist.
- No Iceberg writer.

### Why it matters

Iceberg is the de-facto table-format standard; an Iceberg writer makes DataFlow compatible with Snowflake, Databricks, Athena, DuckDB, Trino.

### Recommended next step

Spike `pyiceberg` destination writer with S3/GCS catalog and schema evolution from the pivot schema.

---

## G9: Pricing wedge

**Target:** Transparent, anti-MAR pricing model.

### Status: not started

- UI has pricing pages (`apps/web/src/pages/pricing.tsx`).
- No backend billing/metering integration.

### Recommended next step

Implement usage metering in `services/usage_metering.py` and expose a pricing calculator endpoint.

---

## G10: Connector expansion

**Target:** 131 → 300+ connectors, prioritized by Airbyte/Fivetran gap analysis.

### Status: in progress

- ~15 native drivers proven locally.
- 734 catalog entries; most are stubs or generic SQL.
- Connector capability registry marks `transfer_ready` truthfully.

### Recommended next step

Ship a generic Singer tap/target bridge and a connector SDK so the community can add sources the same way Airbyte's CDK does.

---

## Competitive maturity score

| Dimension | DataFlow today | Airbyte/Fivetran | Gap |
|-----------|----------------|------------------|-----|
| Batch reliability | 8/10 | 9/10 | small |
| Connector depth | 4/10 | 9/10 | large |
| CDC / real-time | 4/10 | 8/10 | large |
| Vector / AI-ready | 2/10 | 6/10 | large |
| Data contracts / governance | 6/10 | 5/10 | small lead |
| GitOps / as-code | 1/10 | 5/10 | large |
| Lakehouse / Iceberg | 0/10 | 5/10 | large |
| Semantic mapping | 7/10 | 3/10 | lead |
| UX (Transfer Studio) | 7/10 | 6/10 | small lead |
| Enterprise SSO/audit/RBAC | 5/10 | 8/10 | medium |
| Overall | 6.5–7/10 | 8.5/10 | 1.5–2 years of focused work |

The defensible moat is **provable, AI-assisted data movement**: semantic mapping + preflight gates + reconciliation + contracts. The gaps that prevent enterprise parity are **CDC depth, vector destinations, and operational maturity** (orchestration, GitOps, lakehouse).

---

## Files to monitor

- `apps/api/src/transfer/engine.py` — orchestration
- `apps/api/src/transfer/contract_engine.py` — contract enforcement
- `apps/api/services/data_contract.py` — contract/breaker primitives
- `apps/api/services/contract_store.py` — contract persistence
- `apps/api/services/data_quality_history.py` — historical anomaly detection
- `apps/api/services/worker_leases.py` / `transfer_scheduler.py` — distributed job control
- `apps/api/connectors/postgresql_change_stream.py`, `mysql_change_stream.py`, `mongodb_change_stream.py` — CDC
- `apps/api/src/transfer/cdc_transfer.py` — CDC coordinator
- `apps/api/services/rbac.py` — role/permission model
- `apps/api/connectors/*_writer.py` — destination coverage

---

## Test verification

- Full backend suite: `python -m pytest tests` — 2003 passed / 182 skipped / 0 failed
- Route matrix: `tests/test_execute_tracked_universal_matrix.py` — 529 passed / 121 skipped
- Contract + data-quality tests: `tests/test_contracts_router.py`, `tests/test_data_contract.py`, `tests/test_data_quality_history.py` — green
- CI: `api-and-web` on PR #16 — green
