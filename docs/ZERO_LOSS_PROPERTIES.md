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
| 5 | Five-layer verification, not sampling | **PARTIAL** | `cd apps/api && python -m pytest tests/test_property5_five_layer_verification.py -q` (6 passed) | L1–L5 ladder in `verification_ladder.py`; SQLite always + live PG localization; screening rename | MySQL/warehouse SQL pushdown; >250k-row in-memory cap (honest skip); UI copy sweep |
| 6 | Schema fidelity is more than column types | **PARTIAL** | `cd apps/api && python -m pytest tests/test_property6_schema_fidelity.py tests/test_check_constraint_carry.py tests/test_inherit_measured_string_width.py tests/test_generic_sql_create_new_fidelity.py tests/test_identity_carry_create_new.py tests/test_identity_generator_probe.py tests/test_identity_restart_cutover.py tests/test_sqlserver_identity_seed_carry.py -q` (90 passed on this host) | SQLite/PG/MariaDB create-new PK/NOT NULL/DEFAULT/UNIQUE + portable CHECK dest-catalog certified; bare Map VARCHAR inherits `(n)`; TEXT UNIQUE refused; identity seed/increment measured and cutover INSERT proven (PG stepped IDENTITY → 110, MariaDB AUTO_INCREMENT, sqlite AUTOINCREMENT→PG) | Oracle/SQL Server dedicated-writer DDL carry; unportable CHECK stays unsupported; SQLite dest cannot declare AUTOINCREMENT; partitioning; views/triggers |
| 7 | Referential integrity across multi-table migration | **PARTIAL** | `cd apps/api && python -m pytest tests/test_foreign_key_carry.py tests/test_foreign_key_metadata.py tests/test_property7_referential_integrity.py -q` (44 passed on this host: unit + SQLite + live PG 16 + live MariaDB 10.11) | Parents-first load (not alphabetical); post-load ALTER certified from dest catalog; orphan ALTER is `integrity_violation`; SQLite dest refuses rebuild; PG dest schema isolation; single-table child when parent already on dest | Oracle/SQL Server live ALTER; SQLite dest cannot ADD FK (by design); CDC with FKs enabled; cross-schema FKs; composite live matrix |
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
* MongoDB majority read concern + clusterTime (standalone Mongo cannot
  `start_session(snapshot=True)` — requires replica set)
* Oracle flashback SCN / SQL Server snapshot isolation / warehouse time-travel
* Binding bulk COPY export to the same RR session when `BULK_EXPORT` is on

---

## Property 4 — PARTIAL (2026-08-09)

### Defect
Observable exactly-once was incomplete: the SQLite stream path never passed
`job_id` / `write_batch_key`, so insert retries could duplicate despite the
ledger existing in the writer. PG/MySQL armed the ledger even for upserts
(could suppress legitimate updates). Ledger schema lacked `row_start` /
`row_end` / `attempt`. No kill-mid-chunk + checksum golden existed.

### Fix
1. `stream.py` SQLite `_write_batch` arms ledger like PG/MySQL.
2. PG/MySQL `use_ledger` only for insert (not keyed upsert) — parity with SQLite.
3. Ledger DDL + mark record `row_start` / `row_end` / `attempt`; migrate older
   tables; PG mark uses SAVEPOINT so missing-column degrade cannot abort the
   data transaction.
4. Golden proof: clean run checksum == kill-after-chunk-0 + resume.

### Proof output (this host)

```
pytest tests/test_property4_observable_exactly_once.py tests/test_chunk_ledger_accounting.py -q
19 passed in 18.59s

SQLite: insert ledger armed; kill after chunk 0 → resume → 6 rows, no dupes,
identical SHA-256 vs clean run; ledger row_start/row_end present.

PostgreSQL (live): same kill/resume/checksum proof green.
```

### Honesty
* Delivery remains **at-least-once**; observable result is exactly-once via
  same-txn ledger skip (insert) or conflict-key upsert.
* Mongo / Kafka / object stores / warehouses: **NOT_GUARANTEED** (no ledger).
* Quarantine salvage (per-row commit then ledger) is still not same-txn.

---

## Property 5 — PARTIAL (2026-08-09)

### Defect
Gate-8 had strong L1 (row balance) + L3 (full checksum) but no column-level
or row-level localization. Preflight 500-row probes were easy to misread as
population proof. A checksum failure told operators “something’s wrong” —
not which column or which row.

### Fix
1. `services/verification_ladder.py` — enterprise L1–L5 (aggregates, typed
   per-column digests, binary-search PK localization).
2. Wired into `run_reconciliation` for SQLite/PostgreSQL when both populations
   are loadable; engine passes `source_endpoint`.
3. On L3 fail (or `validation_mode=maximum`), L4/L5 run and enrich the Gate-8
   message with `column` + `pk` + source/target values.
4. `DEFAULT_SCREENING_LIMIT` alias; sample-success copy says “screening only”.
5. Memory cap `VERIFICATION_LADDER_MAX_ROWS` (default 250k) — refuse to fake
   population localization above the cap.

### Proof output (this host)

```
pytest tests/test_property5_five_layer_verification.py -q
6 passed in 6.47s

Inject amount drift on id=424 (SQLite) / id=77 (PG):
  L1 pass, L2 fail on amount, L3 fail,
  L4 mismatched_columns=['amount'],
  L5 pk + source_value + target_value exact.
```

### NOT claimed / remaining for PROVEN
* SQL pushdown aggregates/digests for MySQL and warehouses (no full load)
* UI copy sweep so no card says “proof” for sample screening
* Streaming path without source SQL still depends on buffered records

---

## Property 6 — PARTIAL (2026-08-09)

### Defect
Create-new DDL emitted `col type` only. PRIMARY KEY, NOT NULL, DEFAULT, and
UNIQUE were silently dropped. CHECK / FK / views / triggers had no certificate
line — operators could not tell carry from loss.

### Fix
1. `services/schema_fidelity.py` — plan + `SchemaFidelityReport` covering every
   required aspect (`carried` / `unsupported` / `skipped` + reason).
2. SQLite introspect now surfaces PRAGMA `notnull` + `dflt_value` (was always
   nullable=True). PG introspect surfaces non-`nextval` defaults.
3. Rich introspect keys include `defaults` / identity / generated / collation.
4. Stream builds `source_schema_catalog` for every SQL source (a contract PK
   is not a substitute for nullability/defaults/unique keys) and passes it to
   SQLite / PostgreSQL / MySQL writers; CREATE TABLE emits PK / NOT NULL /
   safe DEFAULT / UNIQUE; report stamped on `destination_summary.schema_fidelity`.
5. Unsafe defaults (arbitrary SQL) refuse silently — `unsupported`, not emitted.
6. MySQL/MariaDB: `mysql_writer` consumes the same planner +
   `settle_create_new_on_destination` certify path. TEXT/BLOB/JSON UNIQUE/PK
   is refused rather than inventing a prefix length. MariaDB 10.x index
   catalog is read without MySQL-8-only `STATISTICS.EXPRESSION`.
7. Widthless Map `VARCHAR` (Studio default) inherits measured source `(n)` via
   `inherit_measured_string_width` — SSOT in the Decision Kernel, wired through
   dest-type resolution, CREATE, generic_sql, and DDL identity. Over-cap widths
   promote to LONGTEXT/CLOB/MAX; explicit TEXT/CLOB stay unbounded; bounded
   Map `VARCHAR(10)` stays Map≡CREATE.
8. Portable CHECK predicates are planned (dialect rewrite + whitelist), emitted
   on CREATE, and **certified from the destination catalog**. SQLite introspect
   now measures CHECK/FK/index catalogs (same contract as PG/MySQL). Unportable
   CHECKs stay `unsupported` — never a lying predicate. Live SQLite / PG /
   MariaDB dest CHECK rejects violating rows.
9. Identity **generator** (not just key values) is measured from the source
   catalog (`probe_identity_generators`): PostgreSQL `pg_sequence` start/
   increment, SQL Server `sys.identity_columns` (sql_variant decoded), Oracle
   `IDENTITY_OPTIONS`, SQLite `AUTOINCREMENT`, MySQL `AUTO_INCREMENT` without
   inventing a per-column step. Stamp onto `inferred_type` so the existing
   planner emits `START WITH n INCREMENT BY m`. Cutover proof is a real
   `INSERT` that omits the key.

### Proof output (this host)

```
pytest tests/test_inherit_measured_string_width.py \
       tests/test_property6_schema_fidelity.py \
       tests/test_check_constraint_carry.py \
       tests/test_generic_sql_create_new_fidelity.py \
       tests/test_identity_carry_create_new.py \
       tests/test_identity_generator_probe.py \
       tests/test_identity_restart_cutover.py \
       tests/test_sqlserver_identity_seed_carry.py -q
90 passed in 3.71s

Includes prior CHECK / VARCHAR-width / create-new structure, plus:
  Live PG 16: source IDENTITY (START WITH 5 INCREMENT BY 10), explicit key
    100 loaded, dest pg_sequence.seqincrement = 10, cutover
    INSERT (name) RETURNING id = 110 (not 101).
  Live MariaDB 10.11: AUTO_INCREMENT carried, cutover INSERT without id
    returns LAST_INSERT_ID > migrated max.
  SQLite AUTOINCREMENT → PG: dest attidentity set, cutover INSERT > max.
  SQLite INTEGER PRIMARY KEY without AUTOINCREMENT is not flagged
    (rowid reuse is not a never-reuse generator).
```

### NOT claimed / remaining for PROVEN
* Oracle / SQL Server dedicated-writer create-new constraint carry
  (`generic_sql` already plans; native writers are the remaining hole)
* FOREIGN KEY carry is Property 7 (post-load ALTER; not on single-table CREATE)
* Unportable CHECK predicates (casts, subqueries, unknown functions) stay
  `unsupported` by design; no trigger-emulation of CHECK (AWS SCT class)
* Views, triggers, generated expressions, partitioning
* SQLite destination cannot declare per-column AUTOINCREMENT (rowid aliasing;
  sqlite→PG/MySQL generator carry is proven)
* MySQL/MariaDB per-column increment is the server's
  `@@auto_increment_increment` — not invented as column `INCREMENT BY`
* Oracle / SQL Server live identity cutover not run on this host (unit probe
  + sql_variant decode proven)
* Name-collision remaps under adversarial identifier fixtures (policy coded;
  not yet matrix-proven)

---

## Property 7 — PARTIAL (2026-08-14)

### Defect (competitor class)

AWS DMS does not migrate foreign keys. Full load is alphabetical (or parallel),
so operators are told to disable enforcement (`FOREIGN_KEY_CHECKS=0` /
`session_replication_role=replica`) and re-add constraints by hand. Airbyte and
Fivetran drop FKs. A row-count and a checksum of copied rows stay green while
orphans exist.

### Algorithm

1. Measure source FKs (`probe_foreign_keys`) — `unavailable` ≠ empty.
2. Load **parents first**. Operator order and alphabetical order are both
   ignored when they would insert children first. Each table is its own
   population on the shared job checkpoint (offset/keyset/quarantine reset).
3. After every selected table has landed, `ALTER TABLE … ADD CONSTRAINT …
   FOREIGN KEY`. The engine then validates the loaded rows.
4. Certify `carried` only from the destination catalog (structural match —
   child columns, parent table, parent columns — not constraint name).
5. An ALTER rejected because of orphans is `integrity_violation` (a data
   finding). Duplicate-object on resume or nested single-table carry stays
   planned so the catalog can still certify (MariaDB errno 121 included).
6. SQLite destination cannot ALTER ADD FK without rebuilding the table; that
   is `unsupported`, never a silent drop.

Identity maps (empty Map document) and parent-key remaps are part of the
planner. Catalog lookups use `catalog_namespace` (MySQL = database name, never
leaked `public`).

### Proof output (this host)

```
cd apps/api && python -m pytest tests/test_foreign_key_carry.py \
       tests/test_foreign_key_metadata.py \
       tests/test_property7_referential_integrity.py -q
44 passed in 2.29s

Includes:
  SQLite dest: parents-first order (zzz_cust before aaa_ord), ALTER refused
    (rebuild), dest PRAGMA foreign_key_list empty, rows landed.
  SQLite→PG: dest information_schema FOREIGN KEY present, orphan INSERT
    raises IntegrityError.
  SQLite→MariaDB 10.11: dest CONSTRAINT_TYPE FOREIGN KEY present, orphan
    INSERT raises 1452.
  PG→PG via dest schema: source pg_constraint measured, dest schema isolated,
    orphan INSERT refused.
  Orphan source rows (SQLite FK defined, enforcement off): dest ALTER
    rejected, verdict referential_integrity_violated, dest catalog has no FK.
  Single-table child with parent already on PG dest: FK carried.
```

### NOT claimed / remaining for PROVEN

* Oracle / SQL Server live `ALTER TABLE ADD CONSTRAINT` (planner covers them;
  this host has no those engines)
* SQLite destination ADD FK (table rebuild would rewrite proven rows — refused
  by design)
* CDC apply with destination FKs enabled (full-load carry only; CDC remains
  at-least-once upsert)
* Cross-schema / cross-database references; composite live matrix
* Exactly-once / 100% of all routes — not claimed

