# DataFlow Research-Report Gap Status

**Report:** `dataflow_research_report.md` (July 2026)  
**Branch:** `devin/backend-hardening-p5`  
**Last updated:** 2026-07-20  

This document tracks every item from the market/algorithm research report against the current codebase so progress is transparent and the remaining work is explicit.

---

## Executive summary

Backend batch reliability is now solid:

- `pytest tests`: **2003 passed / 182 skipped / 0 failed** (clean after Elasticsearch shard cleanup)
- 5×5 production SKU matrix verified
- Universal route matrix: **529 passed / 121 skipped**
- Full backend test suite green on PR #16

The product is **beta / early production** for batch transfers on supported drivers. CDC is a **strong partial** (PG/MySQL/Mongo live-proven; shared multi-table reader + Redis/file leases with fencing; SQL Server/Oracle thinner). It is **not** “100% CDC” and **not** platform-wide better than Airbyte/Debezium — integrity (mapping/preflight/quarantine/reconcile) can win *trust*; Airbyte/Debezium still win *CDC fleet coverage, edge-case years, and Connect-scale ops*.

---

## G1: Real-time CDC (log-based)

**Target:** Debezium-style log-based capture from Postgres, MySQL, MongoDB, SQL Server, Oracle.

### Status: strong partial (at-least-once) — load-hardened July 2026

| What is implemented | Where |
|---------------------|-------|
| Query-cursor CDC fallback | `src/transfer/cdc_transfer.py` |
| MongoDB Change Streams (+ signal collection, peek stream-wins) | `connectors/mongodb_change_stream.py` |
| PostgreSQL `pgoutput` binary peek/ack + publication-before-slot + txn hold | `connectors/postgresql_change_stream.py` |
| MySQL binlog + GTID auto_position + XidEvent commit boundary | `connectors/mysql_change_stream.py` |
| SQL Server native CDC + Change Tracking (+ LSN handoff, capture discovery) | `connectors/sqlserver_cdc_native.py`, `connectors/sqlserver_change_stream.py` |
| Oracle LogMiner (+ incremental peek) | `connectors/oracle_logminer.py`, `connectors/oracle_change_stream.py` |
| Incremental snapshot signals (API + DB signal table) | `services/cdc_incremental_snapshot.py`, `services/cdc_signal_table.py` |
| Side-channel token isolation (no watermark clobber under load) | `services/cdc_resume_tokens.py` |
| Distributed CDC leases (Redis Lua + fencing; file/memory fallbacks) | `services/cdc_lease.py`, `services/cdc_lease_store.py` |
| Multi-table single reader (one PG slot / one MySQL `server_id`, demux + ack barrier) | `services/cdc_multi_table.py`, `src/transfer/cdc_transfer.py` `_run_cdc_shared_multi_table`, live IT `tests/test_cdc_shared_reader_integration.py` |
| Mixed `_df_lsn` upsert guard + effectively-once PK sink contract | `connectors/writer_common.py`, `services/cdc_effectively_once.py` |
| PG TOAST-aware update merge + typed txn buffer overflow | `services/cdc_toast.py`, `services/cdc_transaction_buffer.py`, `connectors/pgoutput_decoder.py` |
| `ChangeBatch` with `resume_token` | `services/cdc_engine.py` |
| Watermark persistence | `services/sync_cursor.py`, `services/atomic_file.py` |

### Delivery honesty

- Default apply is **at-least-once upsert** (not exactly-once).
- Live IT green locally for PG (`wal_level=logical`), MySQL ROW+GTID, Mongo single-node `rs0`.
- Multi-worker **leases** via `CdcLeaseGuard` + pluggable store:
  - **Redis** (`DATAFLOW_CDC_LEASE_BACKEND=redis` / `auto` + URL) — multi-node, Lua-atomic acquire, fencing `generation`, fail-closed if Redis down.
  - **File** — single-host `fcntl` flock (default when no Redis URL).
  - **Memory** — tests only.
- **Multi-table shared reader** for PG + MySQL when ≥2 stream contracts are selected (one publication/slot or one binlog `server_id`; demux + `ack_barrier`). Falls back to sequential N readers if the shared path cannot start.
- Shared-reader **ack-barrier chaos** (`tests/test_cdc_shared_ack_chaos.py`) + **live concurrent-write IT** (`tests/test_cdc_shared_reader_integration.py`).
- **Effectively once for PK sinks** when destinations stamp/guard `_df_lsn` (`services/cdc_effectively_once.py`, `tests/test_cdc_effectively_once.py` incl. live PG upsert). Still **not** platform exactly-once.
- **TOAST / large txn**: pgoutput merges unchanged TOAST from old tuple; incomplete sparse updates **fail closed**. Open txns **spill to disk** after ``DATAFLOW_CDC_TXN_SPILL_AFTER``; hard overflow still raises ``CdcTxnBufferOverflow`` (no silent drop).
- Job Theater surfaces lease holder + backend + conflict (`cdc_lease_*` job fields).
- CI: Postgres logical CDC in main job; Redis lease backend on CDC matrix; **SQL Server CT + native CDC** in `cdc-sqlserver`; **Oracle** optional `cdc-oracle` when `ENABLE_ORACLE_CDC` + secrets are set.
- **Do not claim** “100% CDC”, “Debezium parity”, or “better than Airbyte CDC platform-wide” without a named live matrix.

### What is still missing

- **Exactly-once pipeline delivery** — only PK-sink effectively-once via `_df_lsn`; append-only / no-guard sinks remain at-least-once.
- **Oracle always-on CI** (image/license); optional gated job exists, default forks skip.
- **SQL Server** multi-table shared reader; AG/failover LSN quirks.
- Non-CDC multi-stream still runs the **primary stream only** (UI warns; CDC multi-table shared/sequential is the multi-object path).

### July 20 operator + CDC streamline pass

- Durable job `event_log` + Jobs Log / Gate-8 / actor / duration / DDL persistence.
- CDC UI parity: lease + shared reader + snapshot mode + per-stream watermark on Jobs/Theater/Results.
- Advanced: Debezium `snapshot_mode` → `stream_contracts`; priority column + row limit.
- Open-txn **disk spill** (`DATAFLOW_CDC_TXN_SPILL_AFTER`) — still fail-closed past `DATAFLOW_CDC_TXN_BUFFER_MAX_EVENTS`.

### Why it matters

This is the #1 disqualifier in 2026 evaluations. Batch-only or cursor-polling is acceptable for analytics, but operational use cases expect near real-time CDC.

### Recommended next step

1. SQL Server multi-table shared reader / AG failover proofs.
2. Sequential non-CDC multi-stream execute loop (or hard-gate Studio to CDC-only for N streams).
3. Lease metrics / Redis HA runbook.

---

## CDC competitive honesty (July 2026)

| Claim | Status |
|-------|--------|
| Better than Airbyte/Debezium **platform-wide** | **No** |
| “100% CDC” / exactly-once | **No** — default is at-least-once upsert |
| Better on **integrity wedge** (mapping · preflight · quarantine · reconcile · contracts on CDC path) | **Yes — defensible lead** |
| PG/MySQL shared multi-table reader | **Shipped** — unit chaos + live concurrent-write IT |
| Effectively once (PK + `_df_lsn`) | **Proven** for guarded upserts — not EO delivery |
| Multi-node CDC leases | **Shipped** — Redis Lua + fencing; file single-host |
| SQL Server native CDC | **Shipped** — capture discovery, LSN IT, net/before-image filters |
| PG TOAST / large open txn | **Shipped** — merge + disk spill + fail-closed hard cap (no silent drop) |
| SQL Server / Oracle fleet depth | **Behind** Debezium on multi-table/AG; Oracle CI optional |
| Connect-scale ops / years of edge cases | **Behind** |
| Operator CDC signals (lease / shared reader / snapshot) | **Shipped** — Jobs + Theater + Results |

**Verdict:** DataFlow can win evaluations that prioritize *provable trust*. It cannot yet win evaluations that prioritize *CDC platform coverage*. Say so in sales decks.

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
| CDC / real-time | **5.7/10** | 8/10 | large (shared reader + spill + UI signals; EO/SQL Server multi-table still open) |
| Vector / AI-ready | 2/10 | 6/10 | large |
| Data contracts / governance | 6/10 | 5/10 | small lead |
| GitOps / as-code | 1/10 | 5/10 | large |
| Lakehouse / Iceberg | 2/10 | 5/10 | medium (CoW upsert + LSN path landed) |
| Semantic mapping | 7/10 | 3/10 | lead |
| UX (Transfer Studio) | 7/10 | 6/10 | small lead |
| Enterprise SSO/audit/RBAC | 5/10 | 8/10 | medium |
| Overall | 6.5–7/10 | 8.5/10 | still ~1.5–2 years of focused work |

The defensible moat is **provable, AI-assisted data movement**: semantic mapping + preflight gates + quarantine + reconciliation + contracts **on the same path as CDC**. That integrity wedge can beat Airbyte on *trust*; it does **not** yet beat Airbyte on *CDC platform coverage* (multi-table reader, Connect-scale ops, Oracle/SQL Server fleet, years of edge cases).

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
