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
| 8 | Semantic value fidelity | **PARTIAL** | `cd apps/api && python -m pytest tests/test_collation_equality_carry.py tests/test_property8_collation_equality.py tests/test_timezone_instant_carry.py tests/test_timezone_policy_pg_mysql.py tests/test_property8_timezone_instant.py tests/test_mysql_strict_sql_mode.py tests/test_json_polarity_carry.py tests/test_property8_json_polarity.py tests/test_offset_label_carry.py tests/test_property8_offset_label.py tests/test_encoding_capacity_carry.py tests/test_property8_encoding_capacity.py tests/test_decimal_identity_carry.py tests/test_property8_decimal_identity.py tests/test_unicode_form_carry.py tests/test_property8_unicode_form.py -q` (137 passed on this host: collation 11 + instant 38 + JSON 12 + offset-label 19 + encoding 20 + decimal 16 + unicode-form 21; unit + live PG 16 ↔ MariaDB 10.11) | Collation CS `utf8mb4_bin`; session-independent instant; JSON polarity `"1"`≠`1`; offset-label unsupported on TIMESTAMPTZ; encoding `OCTET_LENGTH` of 😀 is 4; decimal unscaled integer; unicode form: PG TEXT / MariaDB `general_ci`/`bin` UNIQUE BOTH_LAND for NFC vs NFD; MariaDB `unicode_ci` SECOND_REJECT; dest HEX `C3A9` vs `CC81`; bind does not NFC | UCA 0900 vs 1400 live MySQL 8; Oracle/SQL Server live offset certify (`DATEPART(TZOFFSET)`); GB18030 live; generic_sql SA `collation=` |
| 9 | Every row is accounted for | **PARTIAL** | `cd apps/api && python -m pytest tests/test_tombstone_polarity.py tests/test_row_conservation.py tests/test_property9_row_conservation.py tests/test_migration_certificate.py tests/test_transfer_mirror.py tests/test_non_cdc_multistream_sequential.py -q` (82 passed in 5.58s on this host). Frontend: `npx tsx --test src/lib/conservationLedger.test.ts src/lib/transferConstants.test.ts` (24 passed); `npm run build` tsc+vite | Overwrite: dest COUNT(*). Keyed/CDC: dest-engine `dest_delta == inserts - deletes` on **keys**, not at-least-once events. Mirror: `COUNT(*) WHERE NOT _deleted`. Multi-stream: job closed iff every stream ledger is closed; dest COUNT summed only for same additive kind. Writer ack never closes. | Inferred deletes on upsert/CDC without tombstone and not mirror; stream-path this-run `soft_deleted` census; Oracle/SQL Server live COUNT; dest-only / file-export; CDC shared-reader per-table dest-before; exactly-once |
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

---

## Property 8 — PARTIAL (2026-08-14, collation equality)

### Defect (competitor class)

AWS DMS copies bytes into the destination's **default collation**. MySQL and
MariaDB default to Unicode case-insensitive equality. A PostgreSQL `UNIQUE`
that accepted both `Alpha` and `alpha` then loses a row on the destination
(`MISSING_TARGET` in DMS validation) while checksums of *accepted* rows stay
green. Airbyte and Fivetran paste a source collation *name* when the dest
happens to know it, and drop it otherwise — name-copy is not equality.
SQL Server `CI_AS` (accent-sensitive) mapped to MySQL `unicode_ci`
(accent-insensitive) would equate `café` and `cafe`.

### Algorithm

1. Classify the source into an **equality class** (case / accent polarity),
   including engine defaults when the catalog has no `COLLATE` name
   (PostgreSQL empty = CS; MySQL empty = CI).
2. Emit a destination-native spelling that preserves that class when one
   exists: PostgreSQL CS → MySQL `CHARACTER SET utf8mb4 COLLATE utf8mb4_bin`
   (type-adjacent, before `NOT NULL`).
3. Refuse a lying stand-in: PostgreSQL has no portable Unicode CI collation
   (`citext` would change the type); SQLite `NOCASE` is ASCII-only; MySQL
   `unicode_ci` is not SQL Server `CI_AS`.
4. Record UNIQUE polarity (`preserved` / `widened` / `tightened`) on the
   certificate. Widened uniqueness is `unsupported`, not `carried`.
5. Certify `carried` from the destination catalog (`information_schema` /
   `pg_collation`), not from emitted DDL.

UCA version (0900 vs 1400) is an extension point on `EqualityClass`, not a
claim that `utf8mb4_unicode_ci` equals `utf8mb4_0900_ai_ci`.

### Proof output (this host)

```
cd apps/api && python -m pytest tests/test_collation_equality_carry.py \
       tests/test_property8_collation_equality.py -q
11 passed in 1.53s

Includes:
  Live PG 16 → MariaDB 10.11: UNIQUE (code) with Alpha and alpha both land;
    dest collation_name contains _bin; schema_fidelity collation=carried.
  Live MariaDB utf8mb4_unicode_ci UNIQUE (only Alpha) → PG: certificate
    collation=unsupported (uniqueness would widen); dest INSERT alpha succeeds.
  Unit: CS→utf8mb4_bin; CI_AS not mapped to unicode_ci; NOCASE refused;
    COLLATE prepended before NOT NULL (MySQL grammar).
```

### NOT claimed / remaining for PROVEN (collation slice)

* UCA 0900 vs 1400 linguistic equality
* Oracle / SQL Server live COLLATE certify (planner covers SQL Server BIN/CI_AS)
* generic_sql SQLAlchemy `collation=` (native PG/MySQL writers emit suffixes)
* Exactly-once / 100% of all routes — not claimed

---

## Property 8 — PARTIAL (2026-08-14, session-independent instant)

### Defect (competitor class)

MySQL `TIMESTAMP` is stored UTC and converted with session `time_zone` on
both read and write. AWS DMS documents `initstmt=SET time_zone='+00:00'`
plus `serverTimezone` and still gets DST / offset wrong (GMT vs BST,
Australia/Sydney, Asia/Calcutta). A source session at `+05:30` returns IST
wall-clock digits; a dest writer that stores those digits as UTC silently
shifts the instant by 5.5 hours. Checksums of the copied digits stay green.
Airbyte/Fivetran treat bare `TIMESTAMP` as wall-clock everywhere, so a
MySQL instant becomes a PostgreSQL `TIMESTAMP WITHOUT TIME ZONE` with no
polarity marker — dest `TIMESTAMPTZ` then refuses naive digits or invents
UTC.

### Algorithm

1. **One pin, every connection.** `pin_mysql_session_utc` / `MYSQL_UTC_PIN_SQL`
   is the only `SET SESSION time_zone = '+00:00'`. Native reader/writer
   (`apply_mysql_session_guards`), generic_sql pooled `connect` event, and
   column-profile sessions all call it. After the pin, TIMESTAMP civil
   digits *are* the UTC instant.
2. **Instant wire, not naive ISO.** A UTC-pinned TIMESTAMP cell is attached
   `+00:00` before `cell_to_string`. `DATETIME` is not. Dest `TIMESTAMPTZ`
   therefore sees an instant, not wall-clock digits it would refuse or invent.
3. **Physical carrier wins.** generic_sql bind: a reflected MySQL `TIMESTAMP`
   wins over a collapsed Map logical of `datetime` (same rule as
   `timezone=True` already winning for `TIMESTAMPTZ`). Offset wire → naive UTC
   bind under the pinned session.
4. **Wall-clock is not shifted.** PostgreSQL `TIMESTAMP WITHOUT TIME ZONE` and
   MySQL `DATETIME` keep civil digits. Policy `utc_invent` stays a named
   contract, never a silent conversion.
5. **Proof is epoch, not display.** After load, dest session is set to
   `+05:30` / `Asia/Kolkata`. `UNIX_TIMESTAMP(col)` / `EXTRACT(EPOCH FROM col)`
   must equal the source instant. Displayed wall clock may change; the
   instant must not.

### Proof output (this host)

```
cd apps/api && python -m pytest tests/test_timezone_instant_carry.py \
       tests/test_timezone_policy_pg_mysql.py \
       tests/test_mysql_strict_sql_mode.py \
       tests/test_property8_timezone_instant.py -q
38 passed in 1.79s

Includes:
  Live PG 16 TIMESTAMPTZ 2024-03-01 12:00:00+05:30 → MariaDB TIMESTAMP(6);
    dest session +05:30; UNIX_TIMESTAMP = 1709271000; display 12:00:00 IST;
    companion DATETIME/PG TIMESTAMP wall column stays 12:00:00 digits.
  Live MariaDB TIMESTAMP written under source session +05:30 (stored UTC
    06:30) → PG TIMESTAMPTZ; dest TIME ZONE Asia/Kolkata;
    EXTRACT(EPOCH) = 1709271000; DATETIME wall column stays hour=12.
  Live generic_sql MySQL pool: SELECT @@SESSION.time_zone = +00:00.
  Unit: pin helper; instant wire attaches UTC without shifting 06:30;
    DATETIME is not stamped Z; generic_sql TIMESTAMP(+05:30)→06:30 naive UTC;
    collapsed datetime logical still uses physical TIMESTAMP; PG TIMESTAMP
    is not UTC-shifted.
```

### NOT claimed / remaining for PROVEN (instant slice)

* Oracle `TIMESTAMP WITH TIME ZONE` live (generic_sql already renders via
  `TO_CHAR`; not this matrix)
* SQL Server `DATETIMEOFFSET` live `DATEPART(TZOFFSET)` certify
* MySQL `TIMESTAMP` year-2038 range is quarantined (unit-proven), not a
  live 2038 row in this run
* Exactly-once / 100% of all routes — not claimed

## Property 8 — PARTIAL (2026-08-14, JSON scalar polarity)

**Claim:** A JSON document is a typed tree, not a Python object. Number `1`
and string `"1"` are different cells. Boolean `true` and string `"true"`
are different cells. JSON `null` is a document value; SQL NULL is absence.
Integers past `2**53` stay JSON numbers with their digits — they are not
stringified to “save” JavaScript. Certification is dest-engine polarity
(`jsonb_typeof` / `JSON_TYPE`), not a second Python parse.

**Why this is the unique product:** psycopg2 and mysql-connector both
deserialize JSON into Python. `cell_to_string` + `json.loads` then
collapses `"1"` → `1`. Airbyte/Fivetran/DMS will happily land that as a
number. DataFlow refuses that collapse: the reader projects engine JSON
text (`col::text` / `CAST AS CHAR`), the writer binds that exact text, and
the dest engine reports the polarity. Operators who store `"1"` as a
string id and `1` as a count get both back.

**Algorithm (canonical, one place):**
1. JSON/JSONB columns travel as **engine JSON text**, never a deserialized
   Python tree (`apps/api/services/json_polarity.py`). SQL NULL stays SQL
   NULL; JSON `null` stays the four characters `null`.
2. Classify polarity on that text: number / string / boolean / null /
   array / object. MySQL `INTEGER`/`DOUBLE`/`DECIMAL` are the same JSON
   number family as Postgres `number`.
3. Bind the same text as JSON (`coerce_json_wire` uses `json_loads_exact`
   so Python `True`/`1` exist only as a driver bind, not as the wire).
4. Certify from the dest engine: `jsonb_typeof` / `JSON_TYPE` on each
   pointer, plus `->>'big'` / `JSON_UNQUOTE` for digits past 2^53.

**Measured (this host, PostgreSQL 16 + MariaDB 10.11):**
```
12 passed in 1.57s
  Live PG JSONB → MariaDB JSON: n=INTEGER, s=STRING, b=BOOLEAN, z=NULL,
    big=INTEGER, JSON_UNQUOTE($.big)=9007199254740993; SQL NULL row stays
    SQL NULL.
  Live MariaDB JSON → PG JSONB: jsonb_typeof n=number s=string;
    payload->>'big'=9007199254740993.
  Unit: polarity of 1 vs "1" vs true vs "true" vs null; round-trip of
    9007199254740993 as a number; SQL NULL vs JSON null; engine text
    projection SQL; coerce_json_wire keeps original text.
```

Combined Property 8 collation + instant + JSON polarity:
```
61 passed in 3.81s
```

### NOT claimed / remaining for PROVEN (JSON polarity slice)

* JSON object key order / duplicate keys (RFC 8259: last-wins at parse;
  engines may canonicalize)
* JSON number canonicalization (`1.0` vs `1`, `1e2` vs `100`) — polarity
  is proven; digit-for-digit text of every number is not
* Postgres JSON vs JSONB (JSONB canonicalizes); this slice uses JSONB
* MySQL JSON vs MariaDB JSON binary storage differences
* Nested array element polarity beyond the document-level pointers in
  this fixture (array JSON element fidelity is a separate wave-86 proof)
* Exactly-once / 100% of all routes — not claimed

## Property 9 — PARTIAL (2026-08-14, dest COUNT(*) conservation)

**Claim:** Every source row is on the destination, quarantined, or
intentionally skipped. Overwrite uses dest `COUNT(*)`. Keyed upsert uses
a dest-engine key census: `dest_delta == inserts - deletes`. Updates do
not change cardinality. Mirror (Fivetran-style `_deleted`) uses dest-engine
`COUNT(*) WHERE NOT _deleted` — physical `COUNT(*)` does not drop.
Writer `records_processed` is diagnostic only.
Hard-delete tombstones that dest actually holds drop `COUNT(*)`; a
tombstone for a key dest does not hold is a no-op, never an insert.

**Why this is the unique product:** AWS DMS documents Full Load success
and later `MISSING_TARGET` (writer counted rows dest does not hold). The
inverse after source delete is leftover dest rows (`EXTRA_TARGET`):
Airbyte incremental often never deletes; Fivetran inferred deletes are
usually soft (`_fivetran_deleted`) so `COUNT(*)` does not drop. DataFlow
probes dest-engine `COUNT(DISTINCT key)` of *live* keys and of
*tombstone* keys *before* the write, hard-DELETEs dest-held tombstones,
and proves `dest_delta == inserts - deletes`. Writer `ON CONFLICT`
rowcount is not that proof.

**Algorithm (canonical):** `apps/api/services/tombstone.py` (polarity) +
`apps/api/services/row_conservation.py` (census + apply) + dest-engine
`destination_key_hits` in `dest_precount.py`. CDC apply already called
`delete_by_primary_keys`; it now stamps the same census.

1. Conservative polarity: exact column names only. `is_active` is not a
   tombstone. `deleted_by` / `delete_count` are not. Unrecognised boolean
   tokens are present, not deleted. `__deleted` / `__op in {d, delete}`
   are CDC envelope flags; bare business `op` is not.
2. Last-op-wins per key inside a batch (DELETE then INSERT is a live
   recreation).
3. Live unique keys and tombstone keys are disjoint. Mixing tombstones
   into the live unique set invented inserts for missing-key deletes.
4. `inserts = live_keys - dest_hits(live)`. `deletes = dest_hits(tombstones)`.
   `expected_delta = inserts - deletes`.
5. Strip tombstone rows from the upsert, then hard-DELETE dest-held keys.
   Census without apply would not drop COUNT; apply without strip would
   upsert the tombstone back.
6. Writer ack is diagnostic (`writer_ack_delta`); it never closes the
   identity.
7. Multi-stream: `job is closed iff every stream ledger is closed`. Dest
   COUNT(*) is summed only when every stream closed the same additive
   kind. Last-table Gate-8 is not the job.

**Measured (this host, SQLite + PostgreSQL 16 → MariaDB 10.11):**
```
70 passed in 5.22s
  (polarity + identity + live execute_tracked + certificate + live
   file→sqlite mirror)

  Event vs key (this host, after 2026-08-14 slice): 73 passed in 5.26s
    Unit: at-least-once redelivery of PK 1 is inserts=1 not 2; 10 events /
      3 keys dest Δ 0; writer ack 10,000 does not close.
    Live SQLite: 6 upsert events for 3 dest-held keys; unique_batch_keys=3;
      events_read=6; last-op-wins write emits 3 rows; expected_delta=0.

  Live SQLite overwrite: dest COUNT(*)=4; certificate uses 4 even when
    records_processed is forged to 10,000.
  Live PG 16 → MariaDB 10.11 overwrite: dest COUNT(*)=4; Gate-8
    target_rows=4; certificate rows_written_source=gate8_dest_readback.
  Live SQLite upsert: dest held 3 keys; batch 3 updates + 1 insert;
    dest COUNT(*) 3 → 4; inserts=1 updates=3 dest_delta=1; writer ack
    forged to 10,000; keyed ledger balanced.
  Live PG → MariaDB upsert: same split; dest labels updated; census
    dest_preexisting=3.
  Live SQLite tombstone upsert: dest 3 rows; batch 1 update + 1 insert
    + 1 is_deleted tombstone of an existing key; dest COUNT(*) stays 3;
    id=2 gone; inserts=1 deletes=1 dest_delta=0; writer ack 10,000.
  Live PG → MariaDB tombstone upsert: same; dest labels {1:A, 3:c, 4:d}.
  Live SQLite mirror (CSV snapshot 2): source keys {2,3,4}; dest physical
    COUNT(*)=4 with id=1 _deleted; active_count=3; inferred_deletes=1;
    conservation_kind=mirror; rows_written_source=gate8_dest_active_readback;
    writer ack does not close. Gate-8 target_rows is stuffed active (3),
    not physical (4).

  Multi-stream job rollup (this host, after 2026-08-14 slice): 82 passed
    in 5.58s (Property 9 command including sequential multi-stream).
    Identity: job is closed iff every stream ledger is closed.
    Unit: last-table dest COUNT(*)=3 with customers=2 + orders=3 → job dest=5;
      first stream unmeasured → job open, dest=None; mixed overwrite+keyed →
      dest not summed (per_stream); single stream still table identity.
    Live SQLite overwrite: customers 2 + orders 3; last table is 3; job dest
      COUNT(*)=5; conservation_kind=job_rollup; writer ack 10,000 does not close.

  MySQL DELETE persists after connection close (PyMySQL autocommit method).
  Unit: last-op DELETE then INSERT is live; missing-key tombstone is not
    an insert; delete-only accumulator expected_delta=-2; is_active /
    deleted_by are not tombstones; stuffed Gate-8 target_rows=3 with
    rows_scanned=4 stays dest_count=4 / active_count=3.
```

Frontend (this host):
```
npx tsx --test src/lib/conservationLedger.test.ts src/lib/transferConstants.test.ts
  24 passed
npm run build  tsc + vite  clean
```

**Operator UI (same identity, display-only):** Terminal jobs stamp
`row_accounting` next to trust (`attach_conservation_to_updates`). Studio
sync `TransferResult.row_accounting` is stamped in `execute_tracked`.
The Jobs **list** whitelist keeps that ledger so Overview / Jobs rows /
connection sync history never fall dest COUNT back to `records_processed`.
Schedule run history copies the same dict. Writer ack is labeled
diagnostic; dest unmeasured renders "—" not a forged destination total.
Mirror jobs headline **Active at dest** (not physical COUNT, not writer
ack). Gate-8 `target_rows` on this path is the active census; stuffing it
as dest COUNT(*) would hide leftover dest keys (the Fivetran
`_fivetran_deleted` hole). Multi-stream jobs stamp per-stream
`row_accounting` and a job rollup (`conservation_kind=job_rollup`): the
Jobs Streams drawer shows dest COUNT(*) per stream; the job headline is
the sum only when every stream closed the same additive kind.

### NOT claimed / remaining for PROVEN (conservation slice)

* Duplicate CDC events per key — **PARTIAL** on named SQLite fixture: 6 events / 3 keys, dest Δ 0. Stream/CDC run-level accumulator ignores at-least-once redelivery. Multi-batch live log replay still CDC at-least-once (not exactly-once).
* Inferred deletes on **upsert/CDC** without a tombstone and **not** mirror mode
* Stream-path this-run `soft_deleted` / `reactivated` census (module-size freeze on `stream.py`)
* Oracle / SQL Server live dest COUNT certify
* dest-only sinks (pgvector, Milvus) and file/object exports — no SQL
  read-back by design
* Multi-table job rollup — **PARTIAL** on named SQLite fixture: two overwrite
  tables (2+3) close as job dest 5, not last-table 3. Mixed/keyed kinds are not
  summed. CDC shared-reader per-table dest-before still unproven.
* Exactly-once / 100% of all routes — not claimed

## Property 8 — PARTIAL (2026-08-14, originating offset-label)

**Claim:** Instant and offset-label are independent. PostgreSQL
`TIMESTAMPTZ` stores UTC and discards the INSERT offset. SQL Server
`DATETIMEOFFSET` stores `+05:30`. We never claim the first did the second's
job. Bind keeps the originating offset only when the dest engine physically
stores it. Certification is dest-engine (`EXTRACT(TIMEZONE)` under
`TimeZone=UTC` is 0 — the label is gone) plus the fidelity aspect
`carried` / `unsupported` / `skipped`.

**Why this is the unique product:** AWS DMS documents that PostgreSQL
normalizes `TIMESTAMP WITH TIME ZONE` to UTC and does not retain the
offset literal. AWS SCT still maps `DATETIMEOFFSET` → `TIMESTAMP WITH
TIME ZONE`. Npgsql will not round-trip a non-zero `DateTimeOffset`.
Python `astimezone(UTC)` before bind does the same to a dest that *could*
store the label. DataFlow classifies physical storage, extracts minutes
east of UTC *before* UTC normalize, binds `DATETIMEOFFSET` with `+05:30`,
and refuses to say `carried` for TIMESTAMPTZ. No companion offset column
is invented.

**Algorithm (canonical, one place):** `apps/api/services/offset_label.py`

1. `stores_originating_offset(engine, type)` — SQL-standard `WITH TIME
   ZONE` without an engine is not claimed (Oracle stores, PostgreSQL does
   not).
2. `extract_offset_label` from the cell before `astimezone(UTC)`. `Z` is
   0 minutes, a stored UTC label, not "no label".
3. Bind: dest that stores the label gets the original offset back on the
   instant; dest that does not stays UTC-normalized.
4. Fidelity aspect: source TIMESTAMPTZ → `skipped` (never had a label);
   `DATETIMEOFFSET` → PG TIMESTAMPTZ → `unsupported`;
   `DATETIMEOFFSET` → `DATETIMEOFFSET` → `carried`.

**Measured (this host, PostgreSQL 16 + MariaDB 10.11):**
```
80 passed in 4.37s  (Property 8 combined, including this slice)
  Live PG: INSERT 2024-03-01 12:00:00+05:30; SET TIME ZONE UTC;
    EXTRACT(TIMEZONE)=0; ts::text has +00 not +05:30; epoch matches
    the instant.
  Live PG TIMESTAMPTZ → MariaDB TIMESTAMP: offset_label status skipped
    (source never stored a label); not claimed carried.
  Unit: DATETIMEOFFSET bind keeps +05:30; PG TIMESTAMPTZ bind is UTC;
    SCT mapping DATETIMEOFFSET → PG WITH TIME ZONE is unsupported.
```

### NOT claimed / remaining for PROVEN (offset-label slice)

* SQL Server live `DATEPART(TZOFFSET, col)` certify
* Oracle live `EXTRACT(TIMEZONE_HOUR/MINUTE)` certify
* Snowflake `TIMESTAMP_TZ` live
* Exactly-once / 100% of all routes — not claimed

## Property 8 — PARTIAL (2026-08-14, encoding capacity)

**Claim:** Charset *names* are not capacity. MySQL `utf8`/`UTF8` is three-byte
(BMP). Oracle `UTF8` is CESU-8 (UTR #26). PostgreSQL `UTF8` is Unicode.
We classify physical form, recompose CESU-8 / UTF-16 surrogate leaks to
Unicode scalars, quarantine cells dest cannot encode, and certify from the
destination engine (`OCTET_LENGTH` / `HEX`). We never substitute `?` and
never copy PostgreSQL `UTF8` onto MySQL as `CHARACTER SET UTF8`.

**Why this is the unique product:** AWS DMS documents character substitution
and historically lacked utf8mb4. Oracle `UTF8` → PostgreSQL UTF8 yields
`invalid byte sequence 0xed 0xa0 0xbd` (CESU-8 high surrogate). Python
`bytes.decode('latin-1')` fallbacks make checksums of `str` look green.
MySQL non-strict SQL mode stores `?`. DataFlow: dest-engine proof that 😀
is four UTF-8 bytes (`F09F9880`), not six CESU-8 bytes; utf8mb3 INSERT
under `STRICT_TRANS_TABLES` errors and the row is absent; bind of U+1F600
into utf8mb3 raises and the write matrix quarantines.

**Algorithm (canonical, one place):** `apps/api/services/encoding_capacity.py`

1. `classify_capacity(engine, type, charset)` — MySQL `latin1` is cp1252
   (euro fits); ISO-8859-1 is not. MySQL `utf8` is utf8mb3. Oracle `UTF8`
   is CESU-8; `AL32UTF8` is UTF-8. Unmeasured dest charset is
   `unsupported`, not `carried`.
2. Decode to Unicode scalars. CESU-8 six-byte supplementary sequences and
   surrogate pairs leaked into Python `str` recompose. Unpaired surrogates
   and ill-formed UTF-8 raise — U+FFFD invent is silent loss. U+FFFD
   already in the source is prior loss and still a character.
3. Bind / quarantine: dest that cannot encode a scalar holds the cell out.
   Create-new MySQL canonicalizes `UTF8`/`utf8mb3` → `utf8mb4` so we do
   not invent a BMP dest.
4. Fidelity aspect `encoding`: PG TEXT → MySQL utf8mb4 is `carried`;
   NVARCHAR → utf8mb3 is `unsupported`. Independent of collation equality
   and of `CHARACTER SET` DDL cosmetics.

**Measured (this host, PostgreSQL 16 + MariaDB 10.11):**
```
100 passed in 4.95s  (Property 8 combined, including this slice)
  Live MariaDB utf8mb3: INSERT 😀 under STRICT_TRANS_TABLES raises;
    COUNT(*)=0. Non-strict stores '?' / HEX 3F — dest-engine proof we
    must never take that path.
  Live PG TEXT 😀 → MariaDB: dest CHARACTER_SET utf8mb4; OCTET_LENGTH=4;
    HEX=F09F9880 (not EDA0BDEDB880); encoding aspect carried; SQL NULL
    stays SQL NULL.
  Unit: CESU-8 bytes recompose; surrogate leak recomposes; utf8mb3
    bind/quarantine holds U+1F600; PG UTF8 is not copied as MySQL utf8.
```

### NOT claimed / remaining for PROVEN (encoding slice)

* Oracle live CESU-8 (`UTF8`) → PG `convert_to` certify
* GB18030 / Shift-JIS live
* SQL Server VARCHAR code-page matrix beyond cp1252 default
* Exactly-once / 100% of all routes — not claimed

## Property 8 — PARTIAL (2026-08-14, decimal identity)

**Claim:** Exact decimal identity is an unscaled integer and a scale, not a
float and not "fits after the destination rounds." `1.2300` (money scale 4)
and `1.23` are different stored identities. PostgreSQL `NUMERIC(10,2)` and
MariaDB `DECIMAL(10,2)` under `STRICT_TRANS_TABLES` both store `1.23` for
`INSERT 1.225`. Bind may still match dest rounding (`fits_decimal` /
`coerce_decimal_wire`) so INSERT succeeds — the certificate must not say
`carried` for a narrower scale or a FLOAT dest. SQLite `DECIMAL` affinity
is IEEE `REAL`; create-new emits TEXT.

**Why this is the unique product:** Airbyte's JSON `number` and Fivetran
unspecified BIGDECIMAL→FLOAT push money through binary64 (`2**53`). AWS DMS
documents FLOAT as approximate and still lets DECIMAL→FLOAT look green.
Python `float()` then `Decimal(str(x))` invents a spelling. Strict SQL is
not a guarantee — MariaDB 10.11 rounds excess DECIMAL scale with no error
and no warning. DataFlow classifies exact vs approximate vs SQLite
affinity, keeps trailing zeros as the money contract, refuses to claim
FLOAT or narrower scale as `carried`, and certifies digits from dest
`::text` / `CAST AS CHAR`. Integers past `2**53` are the same mantissa
bound as JSON polarity.

**Algorithm (canonical, one place):** `apps/api/services/decimal_identity.py`

1. `extract_decimal_identity` — `(sign, unscaled digits, scale)` before
   `float()`. Trailing zeros stay. `float` input is marked approximate.
2. `classify_storage` — exact DECIMAL/NUMERIC/MONEY, approximate
   FLOAT/REAL/DOUBLE, SQLite NUMERIC affinity (IEEE), SQLite create-new
   TEXT (digit-text). Unmeasured dest is `unsupported`.
3. Fidelity aspect `decimal`: source FLOAT → `skipped` (never had an
   exact identity); DECIMAL → FLOAT / narrower declared scale / SQLite
   affinity → `unsupported`; dest that can store the source scale →
   `carried`. No companion integer-cents column.
4. Certify from dest-engine text, not Python `Decimal` after a second parse.

Bind compatibility is unchanged: PostgreSQL still rounds excess scale at
INSERT so quarantine≡bind; MySQL still refuses integer overflow. Identity
is a different axis.

**Measured (this host, PostgreSQL 16 + MariaDB 10.11):**
```
116 passed in 5.78s  (Property 8 combined, including this slice)
  Live PG NUMERIC(10,2): INSERT 1.225; amt::text is 1.23 — dest rounded.
  Live MariaDB DECIMAL(10,2) + STRICT_TRANS_TABLES: INSERT 1.225
    succeeds; CAST AS CHAR is 1.23 — strict mode is not exact identity.
  Live PG NUMERIC(20,4) 1.2300 + (2**53+1).25 → MariaDB DECIMAL:
    dest DATA_TYPE decimal; CAST AS CHAR matches magnitude; beyond-IEEE
    digits survive; SQL NULL stays SQL NULL; decimal aspect carried.
  Unit: 1.2300 ≠ 1.23 as identity; DECIMAL→DOUBLE unsupported;
    NUMERIC(10,4)→DECIMAL(10,2) unsupported; FLOAT source skipped;
    SQLite affinity unsupported, create-new TEXT carried.
```

### NOT claimed / remaining for PROVEN (decimal slice)

* Oracle `NUMBER` live `TO_CHAR` certify
* SQL Server `MONEY` / `DECIMAL` live
* Banker's rounding (ROUND_HALF_EVEN) vs commercial ROUND_HALF_UP as an
  operator-selectable policy — dest engines here round ties away from zero
* Exactly-once / 100% of all routes — not claimed

## Property 8 — PARTIAL (2026-08-14, unicode form / UCA canonical class)

**Claim:** Unicode form identity is NFC vs NFD (UAX #15) plus UCA version,
not CS/CI polarity. `café` (U+00E9, UTF-8 `C3A9`) and `café` (e + U+0301,
UTF-8 `CC81`) are different stored keys under PostgreSQL TEXT, MariaDB
`utf8mb4_bin`, and MariaDB `utf8mb4_general_ci`. MariaDB
`utf8mb4_unicode_ci` (UCA 4.0) UNIQUE rejects the second (NFC=NFD and
`ß`=`ss`). MySQL documents incomplete combining-mark support on
`unicode_ci`, so we never claim MySQL `unicode_ci` equals MariaDB
`unicode_ci` — protocol `mysql` is not dest-engine proof. UCA 4.0 / 5.2 /
9.0 / 14.0 are different weight tables; `utf8mb4_unicode_ci` is not
`utf8mb4_0900_ai_ci`. Bind does not NFC. `normalize_unicode` on Map is an
explicit NFKC transform, not this path.

**Why this is the unique product:** AWS DMS copies bytes into the dest
default collation; checksums of *accepted* rows stay green while UNIQUE
silently drops the NFD twin (MISSING_TARGET). Competitors paste a
collation *name* when the dest happens to know it. CS/CI carry already
emits `utf8mb4_bin` for PostgreSQL UNIQUE — that preserves form on
create-new. The hole CS/CI cannot see is **CI → CI**: `general_ci` keeps
NFC≠NFD as keys; `unicode_ci` collapses them. DataFlow classifies weight
table (codepoint / general / UCA) and version, marks identity→UCA
`unsupported`, and certifies HEX + UNIQUE second-insert from the dest
engine.

**Algorithm (canonical, one place):** `apps/api/services/unicode_form.py`

1. `classify_form` — NFC / NFD / identity / mixed via
   `unicodedata.normalize`. The cell is not rewritten.
2. `classify_uca` — codepoint (`_bin` / PG default), `general_ci` (not
   UCA, no expansions), UCA 4.0/5.2/9.0/14.0. Canonical equivalence of
   `unicode_ci` is True only when the engine is MariaDB; MySQL 4.0 stays
   unknown.
3. Fidelity aspect `unicode_form`: source codepoint/general → dest UCA →
   `unsupported`; same UCA version on the same engine → `carried`; 0900 vs
   1400 → `unsupported`; MySQL 4.0 vs MariaDB 4.0 → `unsupported`. No
   companion composed-form column.
4. Certify from dest-engine HEX (`C3A9` vs `CC81`) and UNIQUE BOTH_LAND /
   SECOND_REJECT. PostgreSQL `normalize(col, NFC)` — `NFC` is a keyword,
   never an identifier.

Collation CS/CI, encoding capacity, and this aspect are independent.
`collation_carry.EqualityClass` is not patched with a UCA version field.

**Measured (this host, PostgreSQL 16 + MariaDB 10.11):**
```
137 passed in 6.42s  (Property 8 combined, including this slice)
  Live PG TEXT PK: NFC + NFD both land; HEX C3A9 vs CC81;
    normalize(col, NFC) true only for the NFC row.
  Live MariaDB UNIQUE: bin / general_ci BOTH_LAND (NFC/NFD and ß/ss);
    unicode_ci SECOND_REJECT; unicode_520_ci SECOND_REJECT when present.
    No utf8mb4_0900_ai_ci / uca1400 on this MariaDB — not claimed.
  Live PG TEXT both forms → MariaDB create-new: dest collation *_bin;
    both HEX spellings land; unicode_form aspect carried.
  Unit: general_ci → unicode_ci unsupported; unicode_ci ≠ unicode_520;
    0900 ≠ 1400; mysql unicode_ci ≠ mariadb unicode_ci; PG libc
    en_US.utf8 is code-point (not unknown UCA); PG → mysql bin carried;
    empty MySQL collation is unknown (5.7 general vs 8.0 0900).
```

### NOT claimed / remaining for PROVEN (unicode-form slice)

* UCA 0900 vs 1400 live on MySQL 8 (`utf8mb4_0900_ai_ci` /
  `utf8mb4_uca1400_*`) — this MariaDB has neither
* Oracle linguistic vs BINARY live HEX
* SQL Server Windows vs `_UTF8` collations live NFC/NFD UNIQUE
* ICU versioned collations on PostgreSQL (`und-x-icu`)
* Exactly-once / 100% of all routes — not claimed

