# DataFlow / Datawrap — Deep Engineering Audit

**Branch:** `feature/Venkat-Analysis` (cut from `devin/deep-audit-1784855991`)
**Scope:** Principal-architect audit of the universal data-movement platform —
transfer engine + preflight gates, CDC, semantic mapping/AI-RAG, connectors.
**Method:** Honest baseline per `.cursor/rules` — real engine runs on real
services, pass/fail/skip counts, no invented greens.

---

## 1. Environment brought up for real proof

No Docker in the audit sandbox; services installed natively instead:

| Service    | Version | Purpose                                            |
|------------|---------|----------------------------------------------------|
| MongoDB    | 7 (rs0) | change-stream / oplog CDC + job store              |
| PostgreSQL | 15      | `wal_level=logical` → live logical-decoding CDC    |
| Redis      | 7       | CDC leases, worker fleet                           |
| DuckDB     | 1.5     | local warehouse SKU proofs (`duckdb://` dialect)   |

Role/db `dataflow/dataflow` created; PG configured for logical replication so
the CDC matrices run against a real slot, not a mock.

**Runtime note:** the sandbox runs Python **3.11**, but
`packages/preflight/pyproject.toml` pins `requires-python >=3.12`, so
`pip install -e packages/preflight` fails on 3.11. The code itself runs fine on
3.11 — the pin is stricter than reality. Recommend relaxing to `>=3.11` or
documenting 3.12 as the hard floor across all packages.

---

## 2. Honest test baseline

* **Collected:** 15,864 tests.
* **Full suite (parallel `-n 8`):** 14,553 passed / 123 failed / 1,306 skipped
  → **99.16% pass**.
* **Re-run of failing files serially:** 123 → **76** failed. The delta is
  **xdist test-isolation flakiness** (e.g. `test_production_sku_honesty`:
  17 failures under `-n 8`, **1** serially). This is itself a finding — the
  suite is not fully parallel-safe and a client CI running `-n` will see flaky
  reds.
* Preflight package (G1–G9): **79/79 passed**.

---

## 3. Real bugs found and FIXED (with regression proof)

### 3.1 Missing dependency breaks a clean boot — FIXED
`services/config.py` imports `pydantic_settings`, but `pydantic-settings` was
absent from `apps/api/requirements.txt`. A fresh `pip install -r requirements.txt`
could not import the job store → the API could not boot. Pinned
`pydantic-settings>=2.0.0`.

### 3.2 MongoClient cache poisoning — FIXED (production bug)
`_mongo_client()` returned a **process-wide cached** client keyed by connection
string. `mongodb_change_stream.close()` called `.close()` on that **shared**
client "under multi-job load". Result: once any CDC stream stopped, every later
Mongo read/write on the same URI failed with
`InvalidOperation: Cannot use MongoClient after close` until process restart —
i.e. a single stream shutdown broke all Mongo transfers in the API process.

Fix (`connectors/mongodb_common.py`):
* self-healing pool — a closed cached client is evicted and rebuilt;
* `close_mongo_client(conn_str)` closes **and** evicts;
* CDC streams now use a **dedicated, uncached** client (`_new_mongo_client`) so
  their `close()` can never affect the shared pool.

Proved by `test_mongodb_common.py` (3 new regression tests) and the
mongodb→postgresql live matrix (previously failed, now green). Patch-target
migration completed in `test_mongodb_change_stream`, `test_cdc_cursor_gap_ops`,
`test_debezium_parity`.

### 3.3 `tsv` transfer-ready driver had no capability profile — FIXED
`CAPABILITY_REGISTRY` had `csv` but not `tsv`, though `tsv` is a transfer-ready
driver — F7 correctly flagged "catalog count ≠ proven live". Added the `tsv`
profile (tab-delimited CSV). `test_capability_registry_f7`: 4/4 green.

### 3.4 Snapshot paging crashed on minimal DBAPI cursors — FIXED
`fetch_scan_page()` intended to fall back to `fetchall` for cursors without a
real `fetchmany`, but only handled a non-sequence return — a cursor **lacking**
`fetchmany` raised `AttributeError`. Now falls back on `AttributeError` too
(valid minimal-DBAPI cursors need not implement `fetchmany`).

### 3.5 Stale golden — FIXED (test maintenance)
`test_e2e_pipeline` asserted `total_gates == 11`; the engine ships **13** gates
(G13 source-coverage, G14 destination-requirements, G15 dest-exists-shape). The
test even asserts `g13_source_coverage` exists two lines later. Bumped to 13.

### 3.6 DuckDB SQLAlchemy dialect — env fix
`duckdb://` dialect was unavailable (`NoSuchModuleError`). `duckdb-engine`
registers it and resolves the whole DuckDB cluster. `requirements.txt` lists
`duckdb-sqlalchemy>=0.5.0`; confirm which package actually ships the working
dialect on the target platform and pin it explicitly.

---

## 4. Remaining failures — categorized honestly (not yet fixed)

### A. Environment / driver unavailable (NOT code bugs)
Would pass in full CI with these services; cannot run on the ARM sandbox:
SQL Server (mssql image crashes under QEMU on ARM), Oracle, Snowflake,
Hive/Impala, ClickHouse/DB2, Athena, DynamoDB (moto), object stores
(S3/GCS/ADLS), SFTP, SMTP/email. Affected files include
`test_generic_sql_certification[mssql]`, `test_oracle_bq_snowflake_wave33`,
`test_hive_timestamptz_wave49`, `test_clickhouse_db2_wave40`,
`test_athena_merge_wave48`, `test_dynamodb_source_route`,
`test_object_store_materialize`, `test_wave_v_accuracy` (s3/gcs/adls),
`test_sftp_live_transfer`, `test_wave_t_accuracy` (email).

### B. Test drift / golden constants (product evolved, tests lagged) — low-risk
* `test_bigquery_array_json_wire_wave88` — precondition asserts a symbol
  `quarantine_unfit_strings` on the PG/MySQL writer modules (renamed/moved).
* `test_data_integrity_p0` — expects label `quarantine`, engine emits
  `write_quarantine`.
* `test_module_size_budgets_f8` — a module exceeded its LOC budget.

### C. Test-isolation flakiness under `pytest -n` (parallel-only reds)
Inflated the parallel count by ~47. Fix by auditing autouse fixtures / shared
singletons (job store, catalogs, lease stores) for per-worker isolation.

### D. Real product gaps (validated, documented — higher-risk to change)
* **Currency / locale auto-normalization** (`test_currency_to_{sqlite,mongodb,duckdb}`):
  `"$1,000.00"` / EU `"€2.000,50"` are written raw instead of normalized to
  `1000.00` / `2000.50`. A real fidelity gap, but a naive fix risks **silent
  corruption** (US vs EU decimal separators) — violates the zero-loss rule.
  Recommend a locale-aware currency coercion with per-column locale inference
  + a preflight risk surface, proven on a golden locale matrix before enabling.
* **PRODUCTION_SKU vs LIVE_MATRIX honesty** (`test_production_sku_honesty`,
  serial): `PRODUCTION_SKU` advertises `postgresql→sqlserver`,
  `mysql→sqlserver`, `sqlserver→postgresql` routes that are absent from
  `LIVE_MATRIX`, so `validate_sku` can never pass them. Either add the routes to
  the live matrix (with real proof) or drop them from the advertised SKU.
* **Oracle / SQL Server CDC snapshot identifier quoting**
  (`test_cdc_identifier_quoting`): hostile column names (embedded `"`) are
  sanitized to underscores instead of doubled-and-quoted, and the snapshot uses
  a plain `ORDER BY` where the test expects `ROW_NUMBER()` keyset windowing.
  A correctness/robustness item for hostile identifiers — needs Oracle/MSSQL to
  validate live.
* **Create-new via direct writer path** (`test_messy_data_{sqlite,mongodb}`):
  `run_mapping_pipeline(target_columns=[])` stamps every mapping
  `assignment_strategy="pending_dest_schema"`, which `resolve_target_columns`
  skips → `"No column mappings"`. The `execute_tracked` flow materializes the
  schema first, so E2E is fine; the direct-write API surface is inconsistent.
* **`sql_write_materialize` / `sql_write_stream` reject accounting**: assert on
  an intermediate function (`build_mapped_rows_from_source`) whose coercion/
  quarantine happens later in `finish_sql_mapped_bundle`. Clarify the
  intermediate contract or move coercion earlier; E2E fidelity is covered by the
  passing `execute_tracked` matrices.

---

## 5. Competitive posture (per cursor market loop)

The wedge is real and defended in code: semantic mapping SSOT
(`services.semantic_mapper.map_columns`), fail-fast preflight (G1–G15),
quarantine + replay, checksum reconcile, signed contracts, Debezium-class CDC
(snapshot+LSN handoff, at-least-once upsert honesty). vs Airbyte/Fivetran
(breadth) and Debezium/Estuary (streaming), the differentiator is **depth +
proof + honesty**. Highest-leverage next investments: (1) locale/currency
fidelity engine, (2) SKU↔LIVE_MATRIX honesty reconciliation, (3) hostile-
identifier hardening across all SQL snapshot readers, (4) parallel-safe test
isolation so the enterprise CI is trustworthy.

---

## 6. What to run

```bash
# services (native)
service postgresql start && service redis-server start
mongod --dbpath /data/db --replSet rs0 --bind_ip_all --fork --logpath /var/log/mongo/mongod.log

# priority engine + preflight
cd packages/preflight && PYTHONPATH=src python -m pytest tests -q      # 79 passed
cd apps/api && python -m pytest tests -q                               # serial = trustworthy
```
