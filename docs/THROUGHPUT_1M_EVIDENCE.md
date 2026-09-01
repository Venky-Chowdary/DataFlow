# 1M-row throughput: measured before / after

Reproduce with the committed harness (no mocks, production engine):

```bash
cd apps/api
BENCH_ROWS=1000000 BENCH_DEST=bench_1m python scripts/bench_pg_to_mysql_million.py
```

The harness discovers the reachable local pair (`5432`/`3306` first, then
`5433`/`3307`), uses the memory job store when Mongo is down, and calls
`src.transfer.stream.stream_database_transfer` — the same entry point a UI job
uses. It always prints destination `COUNT(*)` and fails closed unless that
count equals the source with `rejected_rows = 0`.

Conservation algorithm: `services/million_row_proof.py` (`row_conservation`).

Identity PostgreSQL→MySQL append now takes **server COPY text + STRICT
`LOAD DATA LOCAL INFILE`** when every mapping is a no-op carry and the
types are LOAD-DATA-safe. Python does not materialize a row on that path.
`SHOW WARNINGS` Warning/Error rolls back; dest `COUNT(*)` must equal the
source snapshot. Transforms, jsonb/bytea/timestamptz, upsert/CDC, and
non-empty append stay on the row path (quarantine intact).

## Reproduced on this workspace (2026-09-01, FIFO + 4 heap shards)

Same named fixture. `load_method=copy_text_pg_to_mysql_load_data`,
`copy_workers=4`. COPY and LOAD DATA overlap on a FIFO; workers share
`pg_export_snapshot()`.

| Item | Value |
|------|-------|
| Host | Linux container, Python 3.12 |
| Source | PostgreSQL **5432** (`bench_emp_1000000`) |
| Destination | MySQL **3306**, create-new `bench_1m`, `full_refresh_append` |
| Job store | **mongo** (27017 open) |
| Rows | **1,000,000** |
| Elapsed | **2.580 s** |
| rows/s | **387,619** |
| dest `COUNT(*)` | **1,000,000** |
| `rejected_rows` | **0** |
| Conservation | **OK** |
| Proof | dest COUNT equals source snapshot count |
| Artifact | `/opt/cursor/artifacts/pg_mysql_1000000_proof.json` |

Spot-check `EMP0000001` / `EMP0500000` / `EMP1000000`: source cells equal dest cells.

Same-host progression on this fixture:

| Path | Elapsed | rows/s | dest COUNT(*) |
|------|--------:|-------:|--------------:|
| Row `executemany` (2026-08-25) | 170.3 s | 5,871 | 1,000,000 |
| COPY+LOAD DATA tempfile | 6.151 s | 162,578 | 1,000,000 |
| **FIFO + 4 ctid shards** | **2.580 s** | **387,619** | **1,000,000** |

Not an SLA. Not a 200M claim.

## Prior tempfile COPY+LOAD DATA (same day, still valid history)

Same named fixture as 2026-08-25 (10 columns, PK `employee_id`).
`load_method=copy_text_pg_to_mysql_load_data`. Server `local_infile=ON`.

| Item | Value |
|------|-------|
| Host | Linux container, Python 3.12 |
| Source | PostgreSQL **5432** (`bench_emp_1000000`) |
| Destination | MySQL **3306**, create-new `bench_1m`, `full_refresh_append` |
| Job store | **mongo** (27017 open) |
| Rows | **1,000,000** |
| Elapsed | **6.151 s** |
| rows/s | **162,578** |
| dest `COUNT(*)` | **1,000,000** |
| `rejected_rows` | **0** |
| Conservation | **OK** |
| Proof | dest COUNT equals source snapshot count |
| Artifact | `/opt/cursor/artifacts/pg_mysql_1000000_proof.json` |

Same-host 50k insert vs COPY+LOAD DATA (quality: dest COUNT = 50,000 both):

| Path | Elapsed | rows/s | `load_method` |
|------|--------:|-------:|----------------|
| Row `executemany` (`DATAFLOW_MYSQL_LOAD_DATA=0`) | 10.9 s | 4,578 | `insert` |
| COPY text + STRICT LOAD DATA | 0.4 s | 132,909 | `copy_text_pg_to_mysql_load_data` |

1M vs the 2026-08-25 row-path measurement on this fixture: **170.3 s → 6.151 s**
(~28×). Not an SLA. Not a 200M claim.

## Prior measurement on this workspace (2026-08-25) — row path, still valid history

| Item | Value |
|------|-------|
| Host | Linux container, Python 3.12 |
| Source | PostgreSQL **5432** (`bench_emp_1000000`, 10 columns, PK `employee_id`) |
| Destination | MySQL **3306**, create-new `bench_1m`, `full_refresh_append` |
| Job store | **memory** (Mongo 27017 closed) |
| Rows | **1,000,000** |
| Elapsed | **170.3 s** |
| rows/s | **5,871** |
| dest `COUNT(*)` | **1,000,000** |
| `rejected_rows` | **0** |
| Conservation | **OK** |
| Artifact | `/opt/cursor/artifacts/pg_mysql_1000000_proof.json` |

These numbers replace nothing from the earlier 5433/3307 Mongo run below. They
are this fixture, this hardware, these ports. Not an SLA.

## Environment (must be restated with any quote of these numbers)

| Item | Value |
|------|-------|
| Host | Linux container, Python 3.12.8 |
| Source | PostgreSQL 5433 (`bench_emp_1000000`, 10 columns, PK `employee_id`) |
| Destination | MySQL 3307, create-new table, `full_refresh_append` |
| Job store | MongoDB 27017 (durable checkpoints enabled, fail-closed) |
| Chunk size | 20,000 (engine default) |
| `PARALLEL_WORKERS` | 4 (default) |
| Reconcile | independent source re-read (`checksum_mode=source_reread`) |

## Measured

| Run | Rows | Elapsed | rows/s |
|-----|-----:|--------:|-------:|
| Baseline (before this work) | 50,000 | 49.9 s | 1,002 |
| After bind/type-route caching | 50,000 | 30.2 s | 1,658 |
| After encoding + parse fast paths | 50,000 | 24.5 s | 2,039 |
| After single write-pass fingerprint | 50,000 | 19.6 s | 2,556 |
| After PII scan + width fast paths | 50,000 | 10.9 s | 4,605 |
| **Full fixture** | **1,000,000** | **221.5 s** | **4,515** |

At the baseline rate the same 1M fixture projects to ~16.6 min, which matches
the ~20 min a customer reported. Measured now: **3.7 min, 4.5× faster**, with
`destination COUNT(*) = 1,000,000` and `rejected_rows = 0`.

### Phase profile of the 1M run (engine's own accounting)

| Phase | Seconds | Share of busy time | rows/s |
|-------|--------:|-------------------:|-------:|
| Transform + write | 300.2 | 73.4% | 3,331 |
| Verify checksum | 65.2 | 15.9% | 15,343 |
| Read source | 43.4 | 10.6% | 23,027 |

`busy_seconds` 408.8 vs `elapsed_seconds` 221.3 → overlap factor 1.85: read,
transform/write and reconcile genuinely run concurrently. The dominant cost is
per-cell CPU in transform/validate, not database I/O.

## What was changed (all shared, no route special-casing)

1. **Compiled bind routes** — `connectors/sql_bind_route.py` resolves the
   `(ddl_type, engine)` decision once (`lru_cache`) instead of re-parsing the
   type string per cell; `sql_bind.normalize_sql_bind_value` dispatches on it.
2. **Cached type-system answers** — `sql_type_is_temporal`,
   `instant_date_carrier`, `integer_storage_bounds`, `integer_bit_width`,
   `string_length_unit`, and reconcile's `_TextFoldPlan` (collation / width /
   kana / accent / UUID decisions) are memoised per type token.
3. **One write-pass fingerprint** — on routes where an independent source
   re-read is the authoritative proof (`REREAD_SCAN_SOURCES`), the inline
   write-pass digest is no longer also computed. Routes without a supported
   re-read keep the inline digest, so no route loses its proof.
4. **Exact fast paths, never a weakened check**:
   - PII: every pattern requires `@` or a digit, so a value with neither cannot
     match; otherwise one union-regex pass gates the five per-label passes.
     Verified equivalent to the old loop on 200,011 values (0 mismatches).
   - Column audits call the new `pii_findings()` instead of `detect_pii()`, so
     they stop building a masked sample they discarded.
   - VARCHAR width: ASCII text spends one unit per character under every length
     rule (code points, UTF-8 bytes, UTF-16 units), so `len(text) <= width` and
     `isascii()` proves fit without resolving the dialect unit.
   - Encoding capacity, integer parsing and ISO-date validation get the same
     class of ASCII/shape fast path, falling through to the full check whenever
     the fast test does not *prove* the answer.

## Honesty / limits

- These numbers are **this fixture, this hardware, these two engines**. Do not
  quote them as a product SLA or extrapolate to TB/hour.
- Transform work on the **row path** is still Python CPU inside a thread pool,
  so `PARALLEL_WORKERS` is GIL-bound. Identity PG→MySQL append now skips that
  path (COPY + STRICT LOAD DATA). Other sources, transforms, CDC, and upsert
  still use the row path.
- Do **not** extrapolate 1M to 200M. Disk, indexes, tempfile size, and
  dest-side secondary indexes change the curve. 200M is a partitioned job.
- Only PostgreSQL→MySQL identity append is measured here. Warehouse sources,
  CDC and upsert/MERGE routes are timed separately and are not covered by
  this artifact.
- Copy-path proof is dest `COUNT(*)` vs the source snapshot, not a second
  1M checksum reread. Unfit cells on this path fail closed (STRICT + rollback),
  they do not silently coerce. Quarantine remains on the row path.
- Profiled runs (`BENCH_PROFILE=1`) are ~2× slower by construction and are
  diagnostic only — never quote them as throughput.
