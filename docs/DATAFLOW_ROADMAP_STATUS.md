**Report:** `dataflow_research_report.md` (July 2026)  
**Branch:** `devin/deep-audit-1784855991`  
**Last updated:** 2026-07-26  

This document tracks every item from the market/algorithm research report against the current codebase so progress is transparent and the remaining work is explicit.

---

## Executive summary

Backend batch reliability is **beta / early production** for batch transfers on supported drivers; continuous verification runs on PR #28.

- Test counts on this branch are reported by CI/pytest artifacts on each push — do not rely on stale marketing numbers.
- `pytest` full-suite status and pass/fail/skip counts are captured in the current PR verification section, not in this static doc.
- CDC is **at-least-once upsert** by default. PG/MySQL/Mongo shared-reader paths have live integration coverage; SQL Server/Oracle need cred-gated live matrices before they can be called certified.
- Lakehouse Iceberg has tested catalog commits, schema/partition evolution, time travel, and guarded snapshot expiry; REST is the only live catalog exercised, and runtime exactly-once selection remains blocked (`tests/test_iceberg_commit.py::test_live_rest_concurrent_upserts_keep_one_row_per_key`, `tests/test_iceberg_schema_evolution.py::test_sql_catalog_rename_preserves_field_id_and_old_values`, `tests/test_iceberg_partitioning.py::test_live_rest_partition_create_evolve_and_duckdb_readback`, `tests/test_iceberg_time_travel.py::test_read_by_snapshot_id_timestamp_ms_and_iso_as_of`, `tests/test_iceberg_maintenance.py::test_live_rest_expiry_retains_snapshot_for_time_travel`, `tests/test_cdc_exactly_once.py::test_named_matrix_artifact_matches_measured`).
- SaaS reverse-ETL is transfer-ready for **Stripe, Airtable, and Shopify**; Zendesk and Notion remain source-only until write paths are added.
- The product is **not** “100% CDC” and **not** platform-wide better than Airbyte/Debezium — integrity (mapping/preflight/quarantine/reconcile) can win *trust*; Airbyte/Debezium still win *CDC fleet coverage, edge-case years, and Connect-scale ops*.

---

## G1: Real-time CDC (log-based)

**Target:** Debezium-style log-based capture from Postgres, MySQL, MongoDB, SQL Server, Oracle.

### Status: strong partial — at-least-once by default; exactly-once **per destination** for transactional SQL sinks only


| What is implemented                                                                  | Where                                                                                                                                              |
| ------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| Query-cursor CDC fallback                                                            | `src/transfer/cdc_transfer.py`                                                                                                                     |
| MongoDB Change Streams (+ signal collection, peek stream-wins, idle postBatchResumeToken) | `connectors/mongodb_change_stream.py`                                                                                                         |
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
| Dest-owned transactional offset (`_df_cdc_eos_watermarks`, same txn as apply, CAS write) | `services/cdc_exactly_once.py`, `connectors/cdc_eos_sa.py`, `connectors/cdc_eos_sql.py`                                                          |
| Watermark persistence                                                                | `services/sync_cursor.py`, `services/atomic_file.py`                                                                                               |


### Delivery honesty

- Default apply is **at-least-once upsert** (not exactly-once). `EXACTLY_ONCE_CLAIMED` / `PLATFORM_EXACTLY_ONCE_CLAIMED` stay **False**.
- **Exactly-once per destination** (`delivery_guarantee=exactly_once`, or dest config `require_exactly_once=true`) only for transactional SQL sinks with a PK: PostgreSQL, MySQL/MariaDB, SQL Server, Oracle, Snowflake, DuckDB, SQLite, generic SQLAlchemy SQL. The batch rows and its resume LSN are committed in **one dest transaction** into `_df_cdc_eos_watermarks`; the dest offset is the resume source of truth and the control-plane cursor / source ack advance only after the dest COMMIT is verified.
  - Redelivery at or below the dest offset (family-aware `compare_lsn`) is a no-op; **cross-family LSNs fail closed** (never treated as equal).
  - Offset write is compare-and-set (INSERT for first commit, `UPDATE … WHERE epoch AND fence_epoch` with rowcount=1) on top of the lease `writer_fence`, so two workers cannot both commit a batch.
  - `require_exactly_once=true` **fails closed** with a reason on ClickHouse, Athena/Hive/Impala, BigQuery/Redshift/Databricks routes, object stores, files, Kafka/streams, document/NoSQL sinks, append-only mode, or no PK — these stay **at-least-once**.
  - Named live matrix: `tests/test_cdc_exactly_once_postgres_restart_live.py` (real PG logical slot through `run_cdc_database_transfer`: crash before dest COMMIT → no partial apply; crash after COMMIT before watermark/ack → redelivery no-op; final dest = source rows exactly once; two writers racing the first offset commit → one wins), `tests/test_cdc_exactly_once_live_engines.py` (PG, MySQL; Oracle/SQL Server when reachable), `tests/test_mysql_cdc_postgres_eos_crash_replay.py`, unit `tests/test_cdc_exactly_once.py` + `tests/test_cdc_exactly_once_txn_offset.py`.
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

- **Platform-wide exactly-once** — not claimed. Exactly-once is per destination for transactional SQL sinks; non-transactional / append-only sinks remain at-least-once (append-only is **fail-gated** unless `allow_append_only`). Oracle / SQL Server / Snowflake EOS live proofs run only when those engines are reachable (skipped by default in CI).
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
- **Incremental snapshot operator UI** — job-scoped `GET/POST /transfer/{job_id}/cdc/snapshots` (+ cancel) resolves `source_key` from the job fingerprint; Theater + Jobs `CdcIncrementalSnapshotPanel` request/cancel/monitor. Still at-least-once upsert; not destination undo. **Filtered incremental snapshots** (Debezium `additional-conditions` equivalent): signals carry a structured `row_filter` (never raw SQL; `services/cdc_snapshot_filter.py`) compiled to bound predicates for PostgreSQL, MySQL, SQL Server, Oracle and a `$match` for MongoDB; invalid/`regex` filters are a 400 at request time. The filter bounds only the snapshot read — stream events for all rows keep flowing, and rows outside the filter are not deleted. Live proof: `tests/test_cdc_postgres_filtered_incremental_snapshot_live.py` (PG); other engines are unit-proven only (`tests/test_cdc_snapshot_filter.py`).

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
| “100% CDC” / platform-wide exactly-once                                                              | **No** — default is at-least-once upsert; `PLATFORM_EXACTLY_ONCE_CLAIMED` stays False. |
| Exactly-once for **transactional SQL sinks** (PG, MySQL, SQL Server, Oracle, Snowflake, DuckDB, SQLite) | **Shipped, per destination** — dest-owned offset in the apply txn + CAS + lease fence. Live: PG forced mid-batch restart (`test_cdc_exactly_once_postgres_restart_live.py`), PG/MySQL engine matrix, MySQL CDC → PG crash-replay. Oracle/SQL Server/Snowflake live only when reachable. |
| Exactly-once for ClickHouse, Athena/Hive/Impala, object stores, files, streams, NoSQL, append-only | **No** — at-least-once; `require_exactly_once=true` fails closed |
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


**Verdict:** Datawrap can win evaluations that prioritize *provable trust*. It cannot yet win evaluations that prioritize *CDC platform coverage*. Say so in sales decks.

**Target:** Move data into vector DBs so it is AI-ready.

### G-VEC status: partially verified

| Capability | Status | Evidence test file | Verified-live engine |
| --- | --- | --- | --- |
| pgvector source-id btree / optional HNSW indexes and vector / halfvec storage | M7 live coverage; default remains `vector` with no HNSW index | `apps/api/tests/test_pgvector_m7_live.py` | pgvector extension 0.8.7 at `:5434` |
| Stable vector writes, unchanged-document skip, fingerprints, usage, verified delete and stale cleanup | Live writer contracts cover only the named engines; Milvus was not available | `apps/api/tests/test_vector_m6_live.py`, `apps/api/tests/test_vector_document_skip.py`, `apps/api/tests/test_vector_fingerprint_live.py` | Weaviate 1.26.6; Pinecone Local `v1.0.0.rc0`; pgvector and Qdrant have separate live suites |
| CDC document-key delete, cleanup and redelivery | End-to-end proof is at-least-once and currently limited to pgvector and Qdrant | `apps/api/tests/test_cdc_postgres_vector_live.py` | pgvector `:5434`; Qdrant `:6335` |
| Embedding provider routing and usage accounting | Paid-provider tests use fake responses; deterministic hash embeddings are used for live writer tests | `apps/api/tests/test_embedding_providers.py` | pgvector, Qdrant, Weaviate, Pinecone Local (hash model only) |
| Chunking strategies and safe record templates | Unit coverage uses injected tokenizers; real tiktoken has not been tested | `apps/api/tests/test_document_chunking.py`, `apps/api/tests/test_vector_template.py` | No live embedding provider |
| Run-detail embedding usage estimate | Component-rendered summary with a test for tokens, calls and estimated cost | `apps/web/src/components/transfer/EmbeddingUsageSummary.test.tsx` | No engine required; not an engine-live capability |

**Parity score:** Datawrap **5/10** vs Airbyte/Fivetran **6/10**.

Gaps keeping Datawrap below parity:
- Paid providers are tested only against fake responses.
- Milvus has not run live; Pinecone evidence is Pinecone Local only.
- CDC for Weaviate, Pinecone, and Milvus has not run end to end.
- No managed rerank or hybrid search.
- The rate limiter is process-local.
- Token chunking has not been tested with real tiktoken.

CDC is at-least-once; exactly-once is not claimed. No retrieval-quality claim is made.

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

## G5: GitOps / Datawrap-as-Code

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

Enterprise buyers want reviewable pipeline definitions and drift detection in CI. Meltano proved demand but lacks Datawrap's UI/AI story.

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

**Target:** Catalog-backed Iceberg writes with conflict-safe commits, controlled schema/partition evolution, destination-owned CDC watermarks, time travel, and guarded maintenance.

### Status: catalog capabilities implemented; runtime exactly-once remains unselected

- **Commit layer:** Conflicts reload the table and re-decide before retry; unknown commit state is resolved by commit ID; PyIceberg's automatic retry is disabled for this path. Evidence: `tests/test_iceberg_commit.py::test_conflict_retries_reload_the_table_and_back_off`, `tests/test_iceberg_commit.py::test_unknown_commit_is_recovered_by_commit_id`, `tests/test_iceberg_commit.py::test_builtin_pyiceberg_retry_is_disabled`, `tests/test_iceberg_commit.py::test_live_rest_concurrent_upserts_keep_one_row_per_key`.
- **Namespace check:** Namespace existence errors other than a confirmed missing namespace fail closed, while a namespace-creation race is tolerated. Evidence: `tests/test_iceberg_catalog_namespace.py::test_namespace_exists_only_swallows_no_such_namespace`, `tests/test_iceberg_catalog_namespace.py::test_ensure_namespace_tolerates_a_creation_race`.
- **Exactly-once adapter (catalog path only):** One CDC batch stages data and the table-property watermark in one commit; the watermark is mirrored in the snapshot summary and writer fences are enforced. Evidence: `tests/test_iceberg_eos.py::test_apply_mirrors_watermark_properties_and_snapshot_summary`, `tests/test_iceberg_eos.py::test_stale_writer_fence_is_refused`, `tests/test_iceberg_eos.py::test_zombie_writer_redecides_after_fence_steal`, `tests/test_iceberg_eos.py::test_crash_hooks_leave_table_unchanged[after_apply_before_watermark]`, `tests/test_iceberg_eos.py::test_crash_hooks_leave_table_unchanged[after_watermark_before_commit]`.
- **Live exactly-once proof:** Repository-owned pytest coverage exercises PG CDC → REST + MinIO, both crash points, post-commit redelivery without a second snapshot, fresh-process resume from table properties with no job cursor, stale-fence refusal, source/Iceberg/DuckDB value equality, and one commit ID per LSN. Evidence: `tests/test_iceberg_eos_live_pg.py::test_live_pg_cdc_to_iceberg_eos_survives_restarts`, `tests/test_iceberg_eos_live_pg.py::test_live_pg_cdc_to_iceberg_eos_duckdb_readback`, plus `tests/test_iceberg_eos.py::test_open_raises_fence_with_snapshot_and_resume_from_properties` and `tests/test_iceberg_eos.py::test_zombie_writer_redecides_after_fence_steal`.
- **Runtime routing:** **NOT route-selected.** The classifier marks Iceberg ineligible; direct dispatcher coverage does not change that. The lead decision on threading `dest_cfg` remains pending. Evidence: `tests/test_cdc_exactly_once.py::test_named_matrix_artifact_matches_measured`, `tests/test_iceberg_eos.py::test_exactly_once_dispatch_applies_to_catalog_iceberg`. The filesystem writer remains outside the EOS adapter and at-least-once: `tests/test_iceberg_eos.py::test_filesystem_iceberg_is_refused`.
- **Multi-table bundles:** Unsupported; the adapter refuses them rather than claiming an atomic multi-table commit. Evidence: `tests/test_iceberg_eos.py::test_iceberg_bundle_dispatch_refuses_multi_table_commit`.
- **Schema evolution:** Optional adds, spec-legal widening, explicit rename by field ID, and strict refusal are covered; nested-type evolution is not covered. Evidence: `tests/test_iceberg_schema_evolution.py::test_plan_adds_new_column_as_optional`, `tests/test_iceberg_schema_evolution.py::test_sql_catalog_widens_int_to_long_and_keeps_values`, `tests/test_iceberg_schema_evolution.py::test_sql_catalog_rename_preserves_field_id_and_old_values`, `tests/test_iceberg_schema_evolution.py::test_sql_catalog_strict_refusal_does_not_mutate_table`.
- **Partition specs and key lookup:** Create/evolution are guarded, primary-key destination lookups use sliced predicates, and partitioned writes require `pyiceberg-core`; unpartitioned writes are unaffected when it is absent. Evidence: `tests/test_iceberg_partitioning.py::test_sql_catalog_creates_bucket_and_day_partition_spec`, `tests/test_iceberg_partitioning.py::test_sql_catalog_refuses_partition_drift_without_mutating_table`, `tests/test_iceberg_partitioning.py::test_sql_catalog_evolves_partition_spec_and_keeps_old_data`, `tests/test_iceberg_partitioning.py::test_eos_and_delete_pk_lookups_use_sliced_row_filters`, `tests/test_iceberg_partitioning.py::test_partitioned_table_fails_closed_when_partition_core_is_missing`, `tests/test_iceberg_partitioning.py::test_unpartitioned_writer_is_unaffected_when_partition_core_is_missing`.
- **Time travel and snapshot expiry:** Reads support snapshot IDs and timestamps; expiry protects the current, referenced, and newest retained snapshots. The M2 table-property watermark still resolves after snapshot expiry. Evidence: `tests/test_iceberg_time_travel.py::test_read_by_snapshot_id_timestamp_ms_and_iso_as_of`, `tests/test_iceberg_maintenance.py::test_expiry_preserves_current_tag_and_newest_and_rejects_expired_id`, `tests/test_iceberg_maintenance.py::test_m2_watermark_resolves_after_m5_expiration`.
- **Unsupported:** Compaction/rewrite-data-files and orphan-file removal raise typed unsupported errors; do not hand-roll deletion. Evidence: `tests/test_iceberg_maintenance.py::test_compaction_and_orphan_removal_are_typed_unsupported_operations`. Catalog writes use copy-on-write rather than equality-delete merge-on-read; filesystem equality-delete behavior is distinct. Evidence: `tests/test_iceberg_eos.py::test_registry_describes_filesystem_and_catalog_delete_paths`. Delta Lake has no Datawrap writer; Databricks Iceberg UniForm/federation is not a claim of native Delta support. Evidence for the product boundary: `tests/test_first_party_capability_contract.py::test_loss_upsert_and_airbyte_pack_leads_do_not_steal_neighbors`.
- **Not verified live:** Glue, Hive, Nessie, and Polaris catalogs; throughput at scale; and a real catalog 5xx during commit-state-unknown recovery. The live catalog tests exercise REST (`tests/test_iceberg_commit.py::test_live_rest_concurrent_upserts_keep_one_row_per_key`, `tests/test_iceberg_partitioning.py::test_live_rest_partition_create_evolve_and_duckdb_readback`); unknown-state recovery is injected, not a live 5xx (`tests/test_iceberg_commit.py::test_unknown_commit_is_recovered_by_commit_id`).
- **Known pre-existing MinIO-auth failures:** These 14 PG-backed live copy tests fail on the M4 parent too: `tests/test_iceberg_pg_copy.py::test_live_iceberg_pg_dest_count`, `tests/test_iceberg_pg_copy.py::test_live_iceberg_pg_empty_string_and_null_preserved`, `tests/test_iceberg_pg_copy.py::test_live_iceberg_pg_skip_when_dest_count_matches`, `tests/test_iceberg_pg_copy.py::test_live_iceberg_pg_occupied_mismatch_declines`, `tests/test_iceberg_pg_copy.py::test_live_iceberg_pg_overwrite_replaces_dest`, `tests/test_iceberg_pg_copy.py::test_live_iceberg_pg_source_count_is_not_scan`, `tests/test_iceberg_pg_copy.py::test_live_iceberg_pg_stream_load_method`, `tests/test_pg_iceberg_copy.py::test_live_pg_iceberg_dest_count`, `tests/test_pg_iceberg_copy.py::test_live_pg_iceberg_empty_string_and_null_preserved`, `tests/test_pg_iceberg_copy.py::test_live_pg_iceberg_skip_when_dest_count_matches`, `tests/test_pg_iceberg_copy.py::test_live_pg_iceberg_occupied_mismatch_declines`, `tests/test_pg_iceberg_copy.py::test_live_pg_iceberg_overwrite_replaces_snapshot`, `tests/test_pg_iceberg_copy.py::test_live_pg_iceberg_dest_count_is_not_scan_count`, `tests/test_pg_iceberg_copy.py::test_live_pg_iceberg_stream_load_method`.

Lakehouse score remains **6/10**. Proposed 7/10 pending runtime exactly-once enablement; not raised.

### Why it matters

The live cross-engine readback evidence is limited to DuckDB on the REST path (`tests/test_iceberg_partitioning.py::test_live_rest_partition_create_evolve_and_duckdb_readback`, `tests/test_iceberg_maintenance.py::test_live_rest_expiry_retains_snapshot_for_time_travel`); it does not establish compatibility with every Iceberg engine.

### Recommended next step

Resolve the pending runtime `dest_cfg` routing decision and add runtime-selected PG CDC → REST regression coverage before reconsidering the lakehouse score (`tests/test_cdc_exactly_once.py::test_named_matrix_artifact_matches_measured`, `tests/test_iceberg_eos.py::test_exactly_once_dispatch_applies_to_catalog_iceberg`).

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

### Status: in progress — G-CONN M5

- ~15 native drivers proven locally.
- 734 catalog entries; most are stubs or generic SQL.
- Connector capability registry marks `transfer_ready` truthfully.
- GitHub, Jira Cloud, and Intercom are descriptor-backed SDK sources routed through the transfer engine. Synthetic catalog enrichment reports `beta` / `source_only`, `source_ready=true`, and destination/transfer readiness false.
- Transfer capabilities are full-refresh-only; SDK descriptors preserve manifest-declared sync modes. Evidence: `apps/api/tests/test_gconn_sdk_transfer_routing.py`.
- Synthetic engine evidence: `apps/api/tests/test_gconn_sdk_transfer_e2e.py` covers GitHub/Jira full-reread PK-upsert recovery, Intercom fresh-destination pagination and fault-then-full-reread retry, and GitHub SDK resume refusal before side effects. The verified SQLite existing-destination epoch remap is fixed; SDK checkpoint resume remains unsupported and is refused, with the strict Gate-8 path unchanged. See `docs/CONNECTOR_CERTIFICATION.md` for tests and details.
- The added breadth is three sources plus the existing HubSpot SDK connector, still far short of the hundreds of sources offered by Airbyte/Fivetran.

Live vendor compatibility, quota behavior, real-volume performance, and destination-role readiness remain unverified. The existing-destination epoch remap fix is proven for the SQLite physical-carrier path; other dialect introspection paths were not changed. SDK `resume=True` is fail-closed and remains unsupported until the engine can reconcile the resumed population at Gate-8. Keep catalog readiness and roadmap claims source-only until that evidence exists.

### Recommended next step

Add SDK source reread / population reconciliation support before enabling checkpoint resume, and expand live-vendor proof before broadening the certified surface.

---

## Competitive maturity score


| Dimension                   | Datawrap today | Airbyte/Fivetran | Gap                                                              |
| --------------------------- | -------------- | ---------------- | ---------------------------------------------------------------- |
| Batch reliability           | 8/10           | 9/10             | small                                                            |
| Connector depth             | 5/10           | 9/10             | large (3 transfer-ready SaaS writers added; still far behind)    |
| CDC / real-time             | **7.2/10**     | 8/10             | large (incremental snapshot UI + row_filter evidence; AG dual-node gated) |
| Vector / AI-ready           | **5/10**       | **6/10**         | large                                                            |
| Data contracts / governance | 6/10           | 5/10             | small lead                                                       |
| GitOps / as-code            | **7/10**       | 5/10             | lead (CLI+HTTP+UI+CI+signed CD gate)                             |
| Lakehouse / Iceberg         | **6/10**       | 5/10             | small lead; catalog functions are tested, runtime EOS remains blocked (`tests/test_cdc_exactly_once.py::test_named_matrix_artifact_matches_measured`) |
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

Do not trust stale counts — the authoritative pass/fail/skip artifacts are in the current PR description. As of the latest push to PR #28:

- `pytest tests/test_iceberg_upsert.py` — 9 passed
- `pytest tests/test_cdc_*.py` — 97 passed / 8 skipped
- `pytest tests/test_snowflake_*.py tests/test_bigquery_*.py tests/test_execute_tracked_*_to_bigquery.py tests/test_execute_tracked_*snowflake*.py` — ~40 passed / honest emulator+credential skips
- `pytest tests/test_saas_connectors.py` — 23 passed
- `pytest tests/test_connector_wiring.py` — 110 passed
- `pytest --collect-only` — 10,533 tests, 0 collection errors
- `ruff check --select F`, `bandit -r`, `pip-audit --local`, `npm audit` — clean
- CI: `api-and-web` on PR #28 — re-running on each push
