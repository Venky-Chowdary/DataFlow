# Zero-Loss Properties — Migration Assurance Ledger

Each property is either **PROVEN** (executable proof against real services or an
exhaustive engine matrix attached below), **PARTIAL**, **UNPROVEN**, or
**NOT_GUARANTEED**. There is no third option between proven and documented-absent.

| # | Property | Status | Proof command | Engines covered | Engines NOT covered |
|---|----------|--------|---------------|-----------------|---------------------|
| 1 | Type identity is referentially transparent | **PROVEN** | `cd apps/api && python -m pytest tests/test_property1_type_identity_case_transparent.py -q` (424 passed) + live PG introspect when reachable | All `DDL_TYPES` destinations (case×logical matrix); live PostgreSQL introspect `integer`→`INT4` | Docker MySQL/ClickHouse/Iceberg not run on this host (no Docker); matrix covers their invent DDL |
| 2 | The legitimate path is never blocked | **PARTIAL** | `cd apps/api && python -m pytest tests/test_property2_golden_path_never_blocked.py -q` (19 passed, 4 skipped MySQL on this host) | SQLite↔SQLite + CSV→SQLite resume + SQLite checkpoint resume (always); live PG→PG / CSV→PG / PG→SQLite / PG→Parquet / Mongo→PG; CI `no-config-transfer` now boots PG+MySQL+Mongo and fails on any skip | MySQL 8 on this Windows host (no Docker); CI-wired but not yet green-proven in this run |
| 3 | Source reads are snapshot-consistent | **PARTIAL** | `cd apps/api && python -m pytest tests/test_property3_source_snapshot_consistent.py -q` (3 passed) | PostgreSQL full-refresh REPEATABLE READ + LSN/export_snapshot; SQLite deferred txn; inline write-pass fingerprints (no second scan by default) | MySQL consistent snapshot; Mongo majority/clusterTime; Oracle flashback SCN; SQL Server SI; Snowflake/BQ time-travel; incremental sync (watermark by design) |
| 4 | Writes are exactly-once observable | **PARTIAL** | `cd apps/api && python -m pytest tests/test_property4_observable_exactly_once.py -q` (3 passed) | SQLite+PostgreSQL insert ledger (same-txn; row_start/row_end/attempt); kill-mid-chunk resume = clean checksum | Mongo/Kafka/object-store/warehouse sinks (NOT_GUARANTEED); MySQL live kill proof (Docker down); quarantine salvage path still not same-txn |
| 5 | Five-layer verification, not sampling | UNPROVEN | — | — | — |
| 6 | Schema fidelity is more than column types | UNPROVEN | — | — | — |
| 7 | Referential integrity across multi-table migration | UNPROVEN | — | — | — |
| 8 | Semantic value fidelity | UNPROVEN | — | — | — |
| 9 | Every row is accounted for | UNPROVEN | — | — | — |
| 10 | Determinism | UNPROVEN | — | — | — |
| 11 | The migration certificate | UNPROVEN | — | — | — |
| 12 | Adversarial and chaos testing | UNPROVEN | — | — | — |

---

## Property 1 — PROVEN (2026-08-09)

### Defect
`ddl_type` disambiguated native vs logical integer/float by **surface case**:
`integer`→64-bit, `INTEGER`/`Integer`/`INT`→32-bit; `float`→64-bit, `FLOAT`→32-bit
on ClickHouse/Iceberg/MySQL. Five spellings normalized to the same logical type
but invented different widths.

### Fix
1. **`LogicalType` / `NativeType`** in `services/decision_kernel/logical_type.py` —
   width-bearing logical carriers; `ddl_type` accepts them.
2. **Ambiguous tokens** (`INTEGER`/`INT`/`FLOAT` any case, plus bare logical
   `integer`/`float`) → width unknown → invent via `DDL_TYPES` (64-bit / IEEE-64).
3. **Unambiguous carriers** only select 32-bit: `INT4`/`INT32`/`SERIAL`,
   `REAL`/`FLOAT4`/`FLOAT32`/`FLOAT(p≤24)`.
4. **Introspect** emits `INT4` (PG int4) and MySQL `INT4` / `FLOAT32`.
5. **Case-variant × destination matrix** + monotonicity vs `DDL_TYPES`.

### Proof output (excerpt)

```
CASE MATRIX (ambiguous spellings → identical invent)
  postgresql   integer/INTEGER/Integer/INT/int → BIGINT
  clickhouse   integer/INTEGER/Integer/INT/int → Int64
  iceberg      integer/INTEGER/Integer/INT/int → long
  clickhouse   float/FLOAT/Float             → Float64
  iceberg      float/FLOAT/Float             → double
  mysql        float/FLOAT/Float             → DOUBLE

unambiguous:
  INT4  → postgresql INTEGER / clickhouse Int32 / iceberg int
  FLOAT32 → iceberg float / clickhouse Float32

introspect:
  PG integer/int4 → INT4
  MySQL int/float → INT4 / FLOAT32

pytest: tests/test_property1_type_identity_case_transparent.py — 424 passed
monotonicity: 0 failures across all DDL_TYPES × {integer,float}
```

### Live PostgreSQL (this host)

```
LIVE_PG_FORMAT_TYPE [('i', 'integer'), ('b', 'bigint'), ('r', 'real'), ('d', 'double precision')]
i integer -> INT4
b bigint -> BIGINT
r real -> REAL
d double precision -> DOUBLE PRECISION
LIVE_PG_OK
```

### NOT claimed
End-to-end transfer matrices on MySQL/ClickHouse/Iceberg Docker were **not** run
on this Windows host (Docker unavailable). Invent DDL for those engines is proven
by the case matrix against `ddl_type` / `DDL_TYPES` authority.

---

## Property 2 — PARTIAL (2026-08-09, tightened)

### Defect
Plain create-new / overwrite transfers with auto-derived identity mappings
(no Map `target_type`) were blocked by `g6_additive_stamp`:
`Additive column(s) … lack Map target_type under partial Studio`.
`stamp_additive_mapping_types` listed every blank mapping as “unstamped” even
when invent authority was create-table, and `_auto_map` overwrite identity maps
did not request CREATE invent.

### Fix
1. Overwrite auto-maps stamp `create_new` + `source_type` so Kernel invent runs.
2. `stamp_additive_mapping_types(..., dest_table_exists=False)` invents CREATE
   TABLE stamps; `unstamped` is invent-required-but-failed only.
3. Golden-path suite + gate ALLOW/BLOCK pair + CI job `no-config-transfer`
   (PG + MySQL + Mongo services; fails if any test skips).
4. Golden asserts now require `reconciliation.passed` (+ checksum match when
   both sides present).
5. Resume-after-kill: CSV→SQLite partial+resume; SQLite→SQLite seeded
   checkpoint resume (no duplicates, full row set).
6. Real PG→MySQL golden path (parametrized maps × skip_preflight) when 3306 up.

### Proof output (this host, 2026-08-09)

```
pytest tests/test_property1_type_identity_case_transparent.py -q
424 passed in 2.27s   # re-verify before trusting P1

pytest tests/test_property2_golden_path_never_blocked.py -q
19 passed, 4 skipped in 52.12s
  skipped = PG→MySQL × maps × skip_preflight (MySQL DOWN — no Docker)
  passed  = g6 BLOCK/ALLOW; SQLite↔SQLite ×4; CSV→SQLite resume;
            SQLite checkpoint resume; PG→PG ×4; CSV→PG ×4;
            PG→SQLite; Mongo→PG; PG→Parquet
```

### NOT claimed / remaining for PROVEN
* Paste a **zero-skip** Property 2 run with MySQL reachable (CI
  `no-config-transfer` is wired for that; this host cannot boot MySQL).
* Resume-after-kill on every cross-engine golden route (SQLite/CSV proven
  here; PG/MySQL/Mongo resume still rely on adjacent checkpoint suites).

---

## Property 3 — PARTIAL (2026-08-09)

### Defect
DB→DB streaming opened a **new source connection per page** under default
READ COMMITTED. Concurrent source writers could make page N describe a
different table state than page 1. Optional `RECONCILE_SOURCE_REREAD` was a
second independent scan (another snapshot). Inline write-pass fingerprints
already avoided the second scan by default, but page-to-page MVCC consistency
was missing.

### Fix
1. `services/source_snapshot.py` — transfer-scoped snapshot bind (ContextVar).
2. PostgreSQL full-refresh: one connection, `SET TRANSACTION ISOLATION LEVEL
   REPEATABLE READ`, capture `pg_current_wal_lsn` + `pg_export_snapshot`.
3. SQLite full-refresh: one connection + deferred `BEGIN` for the whole read.
4. `postgresql_reader` / `sqlite_reader` reuse the bound connection; COUNT runs
   on the **same** connection as page reads.
5. Stamp `source_snapshot` onto `destination_summary` and reconciliation
   (certificate surface). Other engines/modes stamp `guarantee=not_guaranteed`
   with an explicit note.
6. Incremental sync intentionally does **not** freeze a snapshot (watermark).

### Proof output (this host)

```
pytest tests/test_property3_source_snapshot_consistent.py -q
3 passed in 8.79s

LIVE PG: 10-row source, CHUNK_SIZE=2, concurrent INSERT id=100..109 after
first page → records_transferred=10, dest ids=[1..10], no late rows;
source_snapshot.isolation=repeatable_read, snapshot_lsn present.

SQLite: deferred transaction snapshot stamped; recon passed.
```

### NOT claimed / remaining for PROVEN
* MySQL `consistent snapshot` / locked handoff on the transfer path
* MongoDB majority read concern + clusterTime
* Oracle flashback SCN / SQL Server snapshot isolation / warehouse time-travel
* Binding bulk COPY export to the same RR session when `BULK_EXPORT` is on
