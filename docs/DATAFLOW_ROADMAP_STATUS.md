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


| What is implemented                                                                  | Where                                                                                                                                              |
| ------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| Query-cursor CDC fallback                                                            | `src/transfer/cdc_transfer.py`                                                                                                                     |
| MongoDB Change Streams (+ signal collection, peek stream-wins)                       | `connectors/mongodb_change_stream.py`                                                                                                              |
| PostgreSQL `pgoutput` binary peek/ack + publication-before-slot + txn hold           | `connectors/postgresql_change_stream.py`                                                                                                           |
| MySQL binlog + GTID auto_position + XidEvent commit boundary                         | `connectors/mysql_change_stream.py`                                                                                                                |
| SQL Server native CDC + Change Tracking (+ LSN handoff, capture discovery)           | `connectors/sqlserver_cdc_native.py`, `connectors/sqlserver_change_stream.py`                                                                      |
| Oracle LogMiner (+ incremental peek)                                                 | `connectors/oracle_logminer.py`, `connectors/oracle_change_stream.py`                                                                              |
| Incremental snapshot signals (API + DB signal table)                                 | `services/cdc_incremental_snapshot.py`, `services/cdc_signal_table.py`                                                                             |
| Side-channel token isolation (no watermark clobber under load)                       | `services/cdc_resume_tokens.py`                                                                                                                    |
| Distributed CDC leases (Redis Lua + fencing; file/memory fallbacks)                  | `services/cdc_lease.py`, `services/cdc_lease_store.py`                                                                                             |
| Multi-table single reader (one PG slot / one MySQL `server_id`, demux + ack barrier) | `services/cdc_multi_table.py`, `src/transfer/cdc_transfer.py` `_run_cdc_shared_multi_table`, live IT `tests/test_cdc_shared_reader_integration.py` |
| Mixed `_df_lsn` upsert guard + effectively-once PK sink contract                     | `connectors/writer_common.py`, `services/cdc_effectively_once.py`                                                                                  |
| PG TOAST-aware update merge + typed txn buffer overflow                              | `services/cdc_toast.py`, `services/cdc_transaction_buffer.py`, `connectors/pgoutput_decoder.py`                                                    |
| `ChangeBatch` with `resume_token`                                                    | `services/cdc_engine.py`                                                                                                                           |
| Watermark persistence                                                                | `services/sync_cursor.py`, `services/atomic_file.py`                                                                                               |


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
- **TOAST / large txn**: pgoutput merges unchanged TOAST from old tuple; incomplete sparse updates **fail closed**. Open txns **spill to disk** after `DATAFLOW_CDC_TXN_SPILL_AFTER`; hard overflow still raises `CdcTxnBufferOverflow` (no silent drop).
- Job Theater surfaces lease holder + backend + conflict (`cdc_lease_*` job fields).
- CI: Postgres logical CDC in main job; Redis lease backend on CDC matrix; **SQL Server CT + native CDC** in `cdc-sqlserver`; **Oracle** optional `cdc-oracle` when `ENABLE_ORACLE_CDC` + secrets are set.
- **Do not claim** “100% CDC”, “Debezium parity”, or “better than Airbyte CDC platform-wide” without a named live matrix.

### What is still missing

- **Exactly-once pipeline delivery** — only PK-sink effectively-once via `_df_lsn`; append-only sinks are **fail-gated** unless `allow_append_only`.
- **Oracle always-on CI** (image/license); optional gated job exists, default forks skip.
- SQL Server **LSN-gap fail-closed** shipped (unit); Oracle **SCN/redo-gap fail-closed** shipped (unit); source HA role probe shipped; dual-node AG failover IT still thinner.
- Lease **Redis HA runbook** shipped (`docs/ops/CDC_LEASE_REDIS.md`); freshness SLO alerts on Overview + Pipelines.

### July 20 operator + CDC streamline pass

- Durable job `event_log` + Jobs Log / Gate-8 / actor / duration / DDL persistence.
- CDC UI parity: lease + shared reader + snapshot mode + per-stream watermark on Jobs/Theater/Results.
- Advanced: Debezium `snapshot_mode` → `stream_contracts`; priority column + row limit.
- Open-txn **disk spill** (`DATAFLOW_CDC_TXN_SPILL_AFTER`) — still fail-closed past `DATAFLOW_CDC_TXN_BUFFER_MAX_EVENTS`.
- **Non-CDC multi-stream sequential** execute (full/incremental) with per-stream remap + watermarks.
- **SQL Server / Oracle shared multi-table CDC** readers + Pipelines contract breaker UX.
- **CDC lease force-release** (`POST /ops/cdc-leases/force-release`) + Theater/Jobs Next-step CTAs (fencing-aware).
- **Freshness SLO alerts** (`GET /ops/freshness` → `alerts` / `slo_status`) + Overview Open pipeline/job CTAs.
- **Incremental snapshot operator UI** — job-scoped `GET/POST /transfer/{job_id}/cdc/snapshots` (+ cancel) resolves `source_key` from the job fingerprint; Theater + Jobs `CdcIncrementalSnapshotPanel` request/cancel/monitor. Still at-least-once upsert; not destination undo.

### Why it matters

This is the #1 disqualifier in 2026 evaluations. Batch-only or cursor-polling is acceptable for analytics, but operational use cases expect near real-time CDC.

### Recommended next step

1. Connect-scale CDC ops / Oracle·SQL Server fleet depth (retention probe shipped; deepen live cleanup IT).
2. Optional dual-node AG/DG live matrix when infra is available (`DATAFLOW_AG_LIVE` / `DATAFLOW_DG_LIVE`).

---

## CDC competitive honesty (July 2026)


| Claim                                                                                                | Status                                                                                                    |
| ---------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| Better than Airbyte/Debezium **platform-wide**                                                       | **No**                                                                                                    |
| “100% CDC” / exactly-once                                                                            | **No** — default is at-least-once upsert                                                                  |
| Better on **integrity wedge** (mapping · preflight · quarantine · reconcile · contracts on CDC path) | **Yes — defensible lead**                                                                                 |
| PG/MySQL shared multi-table reader                                                                   | **Shipped** — unit chaos + live concurrent-write IT                                                       |
| SQL Server / Oracle shared multi-table reader                                                        | **Shipped** — unit proofs; SQL Server LSN-gap + Oracle SCN/redo-gap fail-closed                           |
| Non-CDC multi-stream sequential                                                                      | **Shipped** — full/incremental; SCD2/mirror still blocked                                                 |
| Effectively once (PK + `_df_lsn`)                                                                    | **Proven** for guarded upserts — not EO delivery                                                          |
| Append-only CDC sinks                                                                                | **Gated** — fail-fast unless `allow_append_only` (mapping proof + transfer)                               |
| Multi-node CDC leases                                                                                | **Shipped** — Redis Lua + fencing; file single-host                                                       |
| Operator lease break (force-release + gen)                                                           | **Shipped** — ops API + Theater/Jobs CTAs                                                                 |
| Freshness SLO alerts                                                                                 | **Shipped** — Overview + Pipelines lag badges; Redis HA runbook                                           |
| SQL Server native CDC                                                                                | **Shipped** — capture discovery, LSN IT, net/before-image filters                                         |
| PG TOAST / large open txn                                                                            | **Shipped** — merge + disk spill + fail-closed hard cap (no silent drop)                                  |
| SQL Server / Oracle fleet depth                                                                      | **Behind** Debezium on dual-node AG; HA role probe + gap gates shipped                                   |
| Connect-scale ops / years of edge cases                                                              | **Behind**                                                                                                |
| Operator CDC signals (lease / shared reader / snapshot)                                              | **Shipped** — Jobs + Theater + Results                                                                    |
| CDC cursor gap recovery UI                                                                           | **Shipped** — LSN/SCN fail-closed + Theater/Jobs Reset watermark CTA                                      |
| Append-only CDC gate UI                                                                              | **Shipped** — Destination Advanced toggle + mapping-proof CTA                                             |
| GitOps signed CD gate UI                                                                             | **Shipped** — Pipelines Import “Require signed”                                                           |
| Destination DLQ table + Promote UI                                                                   | **Shipped** — `{table}_df_quarantine` on SQL sinks; Theater/Jobs/Results Promote stamps `_df_promoted_at` |
| Pre-ingestion staging + Studio toggle                                                                | **Shipped** — `{table}_df_staging` → promote clean only; Advanced “Write via staging”; Results chips      |
| pgvector / Qdrant Studio wiring                                                                      | **Shipped** — catalog live + Advanced embed fields → `endpoint.extra`; writers were already real          |
| Weaviate / Pinecone destinations                                                                     | **Shipped** — REST upsert writers + catalog live + Studio Advanced vector fields; at-least-once           |
| Milvus destination                                                                                   | **Shipped** — REST v2 upsert + catalog live + Studio Advanced vector fields; at-least-once                |
| Document chunking → vector                                                                           | **Shipped** — PDF/DOCX/HTML → provenance rows; Studio upload + vector defaults                          |
| OCR for scanned PDFs                                                                                 | **Shipped** — opt-in Tesseract via Studio toggle; pypdfium2 render; fail-closed if binary missing      |
| Semantic vector routing                                                                              | **Shipped** — embed / metadata / exclude_pii / skip; Studio Apply + writer enforcement                 |
| Durable embedding cache                                                                              | **Shipped** — SQLite L2 + process L1; Studio toggle / stats / clear; `endpoint.extra`                 |
| Source HA role probe (AG / Data Guard)                                                               | **Shipped** — live DMV/`v$database` probe + Theater/Results/Trust + MultiSubnetFailover; dual-node failover IT still open |
| CDC retention health probe                                                                           | **Shipped** — ok/at_risk/gap vs min_lsn/oldest SCN; ops API + Validate/Theater/Results; gap recovery CTA                 |


**Verdict:** DataFlow can win evaluations that prioritize *provable trust*. It cannot yet win evaluations that prioritize *CDC platform coverage*. Say so in sales decks.

**Target:** Move data into vector DBs so it is AI-ready.

### Status: shipped (five vector dests + Studio wiring + OCR + durable cache)


| What exists today                                          | Where                                         |
| ---------------------------------------------------------- | --------------------------------------------- |
| Internal ChromaDB RAG store for mapping suggestions        | `packages/ml` / `services`                    |
| Sentence-transformers / OpenAI embed + L1/L2 cache         | `services/vectorization.py` + `embedding_cache.py` |
| pgvector + Qdrant + Weaviate + Pinecone + Milvus writers   | `connectors/*_writer.py` (REST; no fake SDKs) |
| Studio catalog + Advanced vector fields → `endpoint.extra` | Transfer Studio / Destination Advanced        |
| Opt-in OCR for scanned PDFs                                | `services/pdf_ocr.py` + Studio upload toggle  |
| Semantic vector field routing                              | `services/semantic_vector_routing.py` + Studio Apply |
| Durable embedding cache (SQLite)                           | `services/embedding_cache.py` + Studio Advanced |


### What is missing

- Dual-node AG / Data Guard failover IT against a real secondary (probe + gap class are shipped; topology failover reconnect not claimed).
- Cross-node shared embedding cache (Redis/shared volume) — not claimed; SQLite is per volume.

### Why it matters

Airbyte ships five vector destinations and an official RAG pipeline guide. DataFlow now matches that destination set with Studio-wired writers and can beat them on semantic mapping + integrity.

### Recommended next step

Oracle·SQL Server fleet depth / optional dual-node AG·DG live matrix when infra exists.

---

## G3: Data contracts + circuit breakers

**Target:** Convert preflight gates into versioned, enforceable, CI-reviewable contracts with fail-closed breaker states.

### Status: partially implemented / in active hardening


| What is implemented                                             | Where                                                 |
| --------------------------------------------------------------- | ----------------------------------------------------- |
| `DataContract`, `CircuitBreaker`, `ContractEnforcer` primitives | `services/data_contract.py`                           |
| Contract persistence (MongoDB + in-memory fallback)             | `services/contract_store.py`                          |
| Contract lifecycle API                                          | `src/routers/contracts_router.py`                     |
| Contract creation/enforcement in transfer engine                | `src/transfer/contract_engine.py`                     |
| Circuit-breaker gate before transfer execution                  | `src/transfer/contract_engine.py`                     |
| Preflight gates G1-G8                                           | `services/preflight_service.py`, `packages/preflight` |
| Quarantine / rejected rows / dead-letter details                | `src/transfer/engine.py`                              |


### What is still missing

- **CLI plan/apply** + **CI `gitops`** + optional `**gitops-cd-staging**` (signed-contract gate) shipped.
- Source HA role probe + MultiSubnetFailover (dual-node AG failover IT still open; pre-ingestion staging is shipped).

### Shipped (integrity wedge)

- **Composite trust score** (completeness · quarantine · Gate-8 · freshness) on terminal jobs — Theater / Jobs / Results / Pipelines.
- **Breaker state** CLOSED / OPEN / HALF-OPEN with reset on Contracts + Pipelines.
- **Contract signing** + schedule `require_signed_contract` enforcement.
- **GitOps** `dataflow.yaml` / `dataflow-contract.yaml` export + plan/apply (HTTP + Pipelines UI + CLI).
- **SQL Server LSN gap fail-closed** when resume < CDC `min_lsn` (cleanup / AG failover class).
- **Oracle SCN/redo gap fail-closed** when resume < oldest available redo (or LogMiner ORA-01291 class).
- **Append-only CDC sink gate** — transfer + mapping proof; opt-in via `allow_append_only`.
- **GitOps CD staging gate** — `--require-signed-contracts` / `?require_signed_contracts=true` + optional CI job.
- **Destination DLQ table** — rejected rows also land in `{table}_df_quarantine` on SQL sinks; Studio Promote/Replay stamps `_df_promoted_at` (control-plane JSONL remains the audit index).
- **Pre-ingestion staging** — opt-in `write_via_staging`: load `{table}_df_staging`, promote only clean rows to primary (strict blocks promote); Studio Advanced toggle + Results staging chips.
- **pgvector / Qdrant / Weaviate / Pinecone / Milvus Studio** — catalog + Advanced embedding controls wire into real writers via `endpoint.extra` (at-least-once upsert).
- **Document chunking** — PDF/DOCX/HTML → page/heading provenance rows (`document_chunking.py`); Studio upload accept + vector field defaults.
- **OCR scanned PDFs** — opt-in Studio toggle → `pypdfium2` render + Tesseract; upload + transfer honor `enable_ocr`; fail-closed when binary/deps missing.
- **Semantic vector routing** — `recommend_vector_field_roles` → Studio Apply + `exclude_pii_columns` enforced in `vectorize_records`.
- **Durable embedding cache** — SQLite L2 (`embedding_cache.py`) + process L1; Studio Advanced toggle/stats/clear; writers honor `durable_embedding_cache`.
- **Source HA probe** — SQL Server AG DMVs + Oracle `v$database`; stamped on CDC jobs; connector Test + Theater/Results/Trust; MultiSubnetFailover for AG listeners. Dual-node failover IT still open.
- **CDC retention health** — proactive ok/at_risk/gap vs min_lsn / oldest SCN; `POST /ops/cdc-retention/probe`; Validate Check + Theater/Results; reset-watermark CTA.

### Recommended next step

Oracle·SQL Server fleet depth / optional dual-node AG·DG live matrix when infra exists.

---

## G4: Unstructured data intelligence

**Target:** Extract typed rows and chunks from PDF/Word/HTML with semantic type inference.

### Status: partially shipped

- PDF/Word/HTML → provenance chunk rows via `services/document_chunking.py` + `FileParser` (Studio upload accept).
- Opt-in OCR for scanned PDFs via `services/pdf_ocr.py` (pypdfium2 + Tesseract); Studio checkbox + capabilities probe.
- Tabular/text and document chunks feed pgvector/Qdrant/Weaviate/Pinecone/Milvus writers (`_df_prechunked` avoids double-chunk).

### Why it matters

Combining extraction, semantic typing, and embedding is whitespace no ELT vendor owns.

### Recommended next step

Oracle·SQL Server fleet depth / optional dual-node AG·DG live matrix when infra exists.

---

## G5: GitOps / DataFlow-as-Code

**Target:** Pipelines as code, versioned and CI-tested.

### Status: shipped (CLI + HTTP + UI + CI + CD gate)

- `dataflow.yaml` manifest + per-schedule / per-contract YAML artifacts.
- `POST /api/v1/schedules/gitops/plan` and `/apply` (YAML via `{yaml: ...}`).
- Pipelines UI: Export YAML / Import YAML (plan confirm → apply).
- Contracts export as kind-wrapped `dataflow-contract-*.yaml`; import → DRAFT.
- CLI: `apps/cli` — `validate` / `plan` / `apply` / `export` (`npm run dataflow -- …`).
- CI: `.github/workflows/ci.yml` job `gitops` validates examples + plan/apply proofs.
- CD: `--require-signed-contracts` + optional `gitops-cd-staging` job (staging API vars).

### Why it matters

Enterprise buyers want reviewable pipeline definitions and drift detection in CI. Meltano proved demand but lacks DataFlow's UI/AI story.

### Recommended next step

Wire a protected GitHub Environment for staging apply approvals; expand staging manifest to real connector IDs.

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

### Status: explainability + repair approve loop shipped; transfer undo still open

- Pipeline explanations exist in `services/pipeline_explanation.py`.
- Per-mapping evidence is built by `services/mapping_proof.py` and returned from the mapping pipeline.
- **Shipped:** `mapping_proof` is persisted on transfer-plan revisions, stamped onto jobs at create + terminal status, exposed via `GET /transfer/{job_id}/mapping-proof`, and opened from Transfer Studio Results, Job Theater, and Jobs → Mapping (deep-link `#/jobs?jobId=…&panel=mapping-proof`).
- **Shipped:** Agentic repair propose → human decide → apply (`services/agentic_repair.py`, `/repair/*`). Studio Validate: **Propose durable repair** + Approve & apply to mappings. Jobs Quarantine: **Propose repair** (audit trail; apply mappings from Validate).
- Honesty: proof explains column match / transform / confidence — not Gate-8 row-level write fidelity, and not exactly-once CDC. Repair does **not** roll back written destination rows.
- **Still open:** transfer-level undo/rollback via staging-table swap or Iceberg branch.

### Recommended next step

Ship undo/rollback (staging swap or Iceberg branch) — do not claim rollback until that path is real.

---

## G8: Iceberg / lakehouse destination

**Target:** Apache Iceberg writer with schema evolution, CoW upsert, and (later) REST/Glue catalog + time travel.

### Status: filesystem writer live — catalog committers open

- **Shipped:** `connectors/iceberg_writer.py` — filesystem / mounted warehouse, Iceberg V2 metadata + Parquet/JSONL, additive schema evolution, CoW upsert with `_df_lsn` guard. Transfer-live type `iceberg` (+ aliases `apache_iceberg` / `iceberg_rest` / `nessie` → driver). Studio Destination form: warehouse path, namespace, table. Connector form: warehouse auth mode.
- **Proof:** `tests/test_iceberg_upsert.py`.
- **Not yet:** REST / Glue / Nessie catalog committers, Iceberg v3, multi-engine time-travel UI, branch-based undo. Do not claim “Snowflake/Databricks/Athena catalog-compatible writer” until a catalog committer is proven.

### Why it matters

Iceberg is the de-facto table-format standard; closing the catalog gap makes DataFlow compatible with Snowflake, Databricks, Athena, DuckDB, Trino without a second copy path.

### Recommended next step

Spike `pyiceberg` REST catalog committer against a real warehouse + assert schema evolution from the pivot schema; keep filesystem CoW as the default offline path.

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


| Dimension                   | DataFlow today | Airbyte/Fivetran | Gap                                                              |
| --------------------------- | -------------- | ---------------- | ---------------------------------------------------------------- |
| Batch reliability           | 8/10           | 9/10             | small                                                            |
| Connector depth             | 4/10           | 9/10             | large                                                            |
| CDC / real-time             | **7.2/10**     | 8/10             | large (incremental snapshot UI + row_filter evidence; AG dual-node gated) |
| Vector / AI-ready           | 2/10           | 6/10             | large                                                            |
| Data contracts / governance | 6/10           | 5/10             | small lead                                                       |
| GitOps / as-code            | **7/10**       | 5/10             | lead (CLI+HTTP+UI+CI+signed CD gate)                             |
| Lakehouse / Iceberg         | **4/10**       | 5/10             | medium (filesystem CoW + Studio form; REST catalog open)         |
| Semantic mapping            | 7/10           | 3/10             | lead                                                             |
| UX (Transfer Studio)        | 7/10           | 6/10             | small lead                                                       |
| Enterprise SSO/audit/RBAC   | 5/10           | 8/10             | medium                                                           |
| Overall                     | 6.5–7/10       | 8.5/10           | still ~1.5–2 years of focused work                               |


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

