# 1M / 10M-row throughput: measured before / after

Reproduce with the committed harness (no mocks, production engine):

```bash
cd apps/api
BENCH_ROWS=1000000 BENCH_DEST=bench_1m python scripts/bench_pg_to_mysql_million.py
BENCH_ROWS=10000000 BENCH_DEST=bench_10m python scripts/bench_pg_to_mysql_million.py
```

The harness discovers the reachable local pair (`5432`/`3306` first, then
`5433`/`3307`), uses the memory job store when Mongo is down, and calls
`src.transfer.stream.stream_database_transfer` — the same entry point a UI job
uses. It always prints destination `COUNT(*)` and fails closed unless that
count equals the source with `rejected_rows = 0`. PK-range runs also require
each partition dest COUNT to equal that range's source snapshot COUNT.

Conservation algorithm: `services/million_row_proof.py` (`row_conservation`).

Identity PostgreSQL→MySQL append now takes **server COPY text + STRICT
`LOAD DATA LOCAL INFILE`** when every mapping is a no-op carry and the
types are LOAD-DATA-safe. Python does not materialize a row on that path.
`SHOW WARNINGS` Warning/Error rolls back; dest `COUNT(*)` must equal the
source snapshot. Transforms, jsonb/bytea/timestamptz, upsert/CDC, and
non-empty append stay on the row path (quarantine intact).

When the source has exactly one mapped PK, shards are **equal-height PK
ranges** (`percentile_disc`). Each range must match dest `COUNT(*)` for
that key interval. No mapped single PK: heap `ctid` shards (total dest
COUNT only).

## Reproduced on this workspace (2026-09-02, PK-range shards)

Same production entry point. `load_method=copy_text_pg_to_mysql_load_data`,
`shard_mode=pk`, `proof_scope=partition_dest_count_equals_source_snapshot`.
4 workers (host has 4 CPUs, so auto-8 at ≥5M is capped).

### Named 1M fixture

| Item | Value |
|------|-------|
| Host | Linux container, Python 3.12, 4 CPUs |
| Source | PostgreSQL **5432** (`bench_emp_1000000`) |
| Destination | MySQL **3306**, create-new `bench_1m`, `full_refresh_append` |
| Job store | **mongo** (27017 open) |
| Rows | **1,000,000** |
| Elapsed | **4.241 s** |
| rows/s | **235,786** |
| dest `COUNT(*)` | **1,000,000** |
| `rejected_rows` | **0** |
| Conservation | **OK** |
| PK partitions | 249999 + 250000 + 250000 + 250001 (each dest COUNT matched) |
| Spot-check | `EMP0000001` / `EMP0250000` / `EMP0500000` / `EMP1000000` cells equal |
| Artifact | `/opt/cursor/artifacts/pg_mysql_1000000_pk_proof.json` |

PK-range 1M is slower than FIFO+ctid 2.580 s on this host because the
coordinator pays for `percentile_disc`, per-range source COUNTs, and
per-range dest COUNTs — those are the partition proof ctid cannot give.
Still ~40× the 170.3 s row path. Quality held: STRICT + fail-closed COUNT.

### Named 1M fixture — ctid COPY + PK dest COUNT (current empty-dest path)

Empty dest now COPYs by heap `ctid` (sequential I/O) and still fail-closes
each PK range dest COUNT. `copy_split=ctid`, `shard_mode=pk`.

| Item | Value |
|------|-------|
| Rows | **1,000,000** |
| Elapsed | **3.480 s** |
| rows/s | **287,316** |
| dest `COUNT(*)` | **1,000,000** |
| PK partitions | 249999 + 250000 + 250000 + 250001 (each dest COUNT matched) |
| Artifact | `/opt/cursor/artifacts/pg_mysql_1000000_ctid_pk_proof.json` |

Faster than PK-range COPY (4.241 s) because LOAD is sequential heap, not
index ranges. Still slower than proof-less ctid (2.580 s) by the COUNT
queries. Resume into a non-empty dest still COPYs by PK (`copy_split=pk`).

### Named 10M fixture

| Item | Value |
|------|-------|
| Host | Linux container, Python 3.12, 4 CPUs |
| Source | PostgreSQL **5432** (`bench_emp_10000000`, 8-digit `EMP` pad) |
| Destination | MySQL **3306**, create-new `bench_10m`, `full_refresh_append` |
| Job store | **mongo** (27017 open) |
| Seed | **27.8 s** (`generate_series` INSERT) |
| Rows | **10,000,000** |
| Elapsed (transfer) | **42.331 s** |
| rows/s | **236,232** |
| dest `COUNT(*)` | **10,000,000** |
| `rejected_rows` | **0** |
| Conservation | **OK** |
| PK partitions | 2499999 + 2500000 + 2500000 + 2500001 (each dest COUNT matched) |
| Spot-check | `EMP00000001` / `EMP02500000` / `EMP05000000` / `EMP10000000` cells equal |
| Artifact | `/opt/cursor/artifacts/pg_mysql_10000000_proof.json` |

Same-host progression (dest COUNT equals source on every row):

| Path | Rows | Elapsed | rows/s | dest COUNT(*) | partition dest COUNT |
|------|-----:|--------:|-------:|--------------:|----------------------|
| Row `executemany` (2026-08-25) | 1M | 170.3 s | 5,871 | 1,000,000 | n/a |
| COPY+LOAD DATA tempfile | 1M | 6.151 s | 162,578 | 1,000,000 | n/a |
| FIFO + 4 ctid shards | 1M | 2.580 s | 387,619 | 1,000,000 | n/a (ctid) |
| **FIFO + 4 PK-range COPY** | **1M** | **4.241 s** | **235,786** | **1,000,000** | **4/4 match** |
| **FIFO + 4 PK-range COPY** | **10M** | **42.331 s** | **236,232** | **10,000,000** | **4/4 match** |
| **FIFO + 4 ctid COPY + PK dest COUNT** | **1M** | **3.480 s** | **287,316** | **1,000,000** | **4/4 match** |

Not an SLA. 10M stayed linear with 1M PK-range on this host (~236k rows/s).

### Restartable PK partitions (2026-09-02)

A range whose dest COUNT already equals the source snapshot is **skipped**.
A partial range is **DELETE + LOAD**. Disjoint ranges may LOAD into a dest
that already holds other keys. ctid shards still refuse non-empty append.

Live 8_000-row integer PK: 4 partitions, delete one key in the third range,
resume `replace_destination=False` → 3 skip + 1 reload, dest COUNT 8_000.
Pytest: `test_live_pk_partition_resume_reloads_partial_range`.

Named 1M skip-all (dest already complete, `BENCH_KEEP_DEST=1`):

| Item | Value |
|------|-------|
| dest `COUNT(*)` | **1,000,000** |
| `partitions_skipped` | **4 / 4** |
| `action` | skip, skip, skip, skip |
| Elapsed | **1.172 s** (range COUNTs only — not a copy-speed figure) |
| Artifact | `/opt/cursor/artifacts/pg_mysql_1000000_resume_skip_proof.json` |

### Named 1M fixture — MySQL→PostgreSQL COPY FROM STDIN (2026-09-02)

Identity reverse of the PG→MySQL pipe. One InnoDB consistent snapshot.
Unbuffered SELECT → FIFO TSV → `COPY FROM STDIN`.
`load_method=copy_text_mysql_to_pg_stdin`. Python still formats TSV (MySQL
has no `COPY TO STDOUT`); it does not run transform / quarantine /
fingerprint. Parallel MySQL reads are not used. `SELECT INTO OUTFILE` is
not used: the `dataflow` user has no FILE privilege.

Reproduce: `BENCH_SRC=bench_1m BENCH_DEST=bench_pg_from_mysql python scripts/bench_mysql_to_pg_million.py`

First named run used the canonical `_copy_text_value` encoder (per-cell
import + four `str.replace` on every string):

| Item | Value |
|------|-------|
| Source | MySQL **3306** (`bench_1m`) |
| Destination | PostgreSQL **5432** `bench_pg_from_mysql` |
| Rows | **1,000,000** |
| Elapsed | **12.566 s** |
| rows/s | **79,581** |
| dest `COUNT(*)` | **1,000,000** |
| `rejected_rows` | **0** |
| PK partitions | 249999 + 250000 + 250000 + 250001 (each dest COUNT matched) |
| Spot-check | `EMP0000001` / `EMP0500000` / `EMP1000000` cells equal |
| Artifact | `/opt/cursor/artifacts/mysql_pg_1000000_proof.json` |

Re-measure with `tsv_encoder=fast_copy_text` (binary FIFO, escape only when
a COPY metachar is present; other types still use `_copy_text_value`):

| Item | Value |
|------|-------|
| Source | MySQL **3306** (`bench_1m`) |
| Destination | PostgreSQL **5432** `bench_pg_from_mysql` |
| Rows | **1,000,000** |
| Elapsed | **7.605 s** |
| rows/s | **131,484** |
| dest `COUNT(*)` | **1,000,000** |
| `rejected_rows` | **0** |
| `tsv_encoder` | `fast_copy_text` |
| PK partitions | 249999 + 250000 + 250000 + 250001 (each dest COUNT matched) |
| Spot-check | `EMP0000001` / `EMP0500000` / `EMP1000000` cells equal |
| Artifact | `/opt/cursor/artifacts/mysql_pg_1000000_fast_tsv_proof.json` |

~22× the 170.3 s row path; **1.65×** the prior 12.566 s encoder. Still
slower than PG→MySQL ctid COPY because TSV is encoded in Python. Quality
held: dest COUNT + per-range COUNT. Pytest:
`test_mysql_pg_copy` + `test_mysql_load_data` + `test_million_row_proof`
**24 passed / 0 failed**.

### 200M named fixture — not run on this host

10M source is **1.4 GB** on disk. 200M projects to **~28 GB** source plus
~22 GB dest. This box has **15 GiB RAM** (about 5 GiB available with the
10M tables cached). A 200M COPY would page, not prove. The partitioned
job (up to 32 ranges, CPU-sized waves, skip/reload) is what a larger box
runs. Chunked seed is 10M INSERTs. Do not quote 10M × 20 as a 200M time.

## Prior: FIFO + 4 heap shards (2026-09-01)

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
- Do **not** extrapolate 1M or 10M to 200M. This host did not run 200M
  (15 GiB RAM vs ~28 GB projected source heap). 200M is the partitioned
  skip/reload job on a larger box.
- Only PostgreSQL→MySQL identity append is measured here. Warehouse sources,
  CDC and upsert/MERGE routes are timed separately and are not covered by
  this artifact.
- Copy-path proof is dest `COUNT(*)` vs the source snapshot, not a second
  checksum reread. With `shard_mode=pk`, each key range also has dest COUNT
  equal to that range's snapshot COUNT. Unfit cells on this path fail closed
  (STRICT + rollback), they do not silently coerce. Quarantine remains on the
  row path.
- Profiled runs (`BENCH_PROFILE=1`) are ~2× slower by construction and are
  diagnostic only — never quote them as throughput.
