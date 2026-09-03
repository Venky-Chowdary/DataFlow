# 1M / 10M-row throughput: measured before / after

Reproduce with the committed harness (no mocks, production engine):

```bash
cd apps/api
BENCH_ROWS=1000000 BENCH_DEST=bench_1m python scripts/bench_pg_to_mysql_million.py
BENCH_ROWS=10000000 BENCH_DEST=bench_10m python scripts/bench_pg_to_mysql_million.py
BENCH_SRC=bench_1m BENCH_DEST=bench_pg_from_mysql python scripts/bench_mysql_to_pg_million.py
BENCH_SRC=bench_1m BENCH_DEST=bench_mysql_clone python scripts/bench_mysql_to_mysql_million.py
BENCH_DEST=bench_pg_clone python scripts/bench_pg_to_pg_million.py
BENCH_ROWS=1000000 BENCH_SRC=bench_ss_1000000 BENCH_DEST=bench_ss_clone \\
  python scripts/bench_sqlserver_sqlserver_million.py
BENCH_ROWS=1000000 BENCH_SRC=BENCH_ORA_1000000 BENCH_DEST=BENCH_ORA_CLONE \\
  python scripts/bench_oracle_oracle_million.py
BENCH_ROWS=1000000 BENCH_DEST=bench_ss_from_pg \\
  python scripts/bench_pg_to_sqlserver_million.py
BENCH_ROWS=1000000 BENCH_SRC=bench_ss_from_pg BENCH_DEST=bench_pg_from_ss \\
  python scripts/bench_sqlserver_to_pg_million.py
BENCH_ROWS=1000000 BENCH_DEST=BENCH_PG_ORA \\
  python scripts/bench_pg_to_oracle_million.py
BENCH_ROWS=1000000 BENCH_SRC=BENCH_PG_ORA BENCH_DEST=bench_pg_from_ora \\
  python scripts/bench_oracle_to_pg_million.py
BENCH_ROWS=1000000 BENCH_SRC=bench_1m BENCH_DEST=bench_ss_from_mysql \\
  python scripts/bench_mysql_to_sqlserver_million.py
BENCH_ROWS=1000000 BENCH_SRC=bench_ss_from_mysql BENCH_DEST=bench_mysql_from_ss \\
  python scripts/bench_sqlserver_to_mysql_million.py
BENCH_ROWS=1000000 BENCH_SRC=bench_1m BENCH_DEST=BENCH_MY_ORA \\
  python scripts/bench_mysql_to_oracle_million.py
BENCH_ROWS=1000000 BENCH_SRC=BENCH_MY_ORA BENCH_DEST=bench_mysql_from_ora \\
  python scripts/bench_oracle_to_mysql_million.py
BENCH_ROWS=1000000 BENCH_SRC=bench_ss_from_mysql BENCH_DEST=BENCH_SS_ORA \\
  python scripts/bench_sqlserver_to_oracle_million.py
BENCH_ROWS=1000000 BENCH_SRC=BENCH_SS_ORA BENCH_DEST=bench_ss_from_ora \\
  python scripts/bench_oracle_to_sqlserver_million.py
BENCH_ROWS=1000000 BENCH_SRC=bench_emp_1000000 BENCH_DEST=bench_pg_iceberg \\
  python scripts/bench_pg_to_iceberg_million.py
BENCH_ROWS=1000000 BENCH_SRC=bench_pg_iceberg BENCH_DEST=bench_pg_from_iceberg \\
  python scripts/bench_iceberg_to_pg_million.py
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
held: dest COUNT + per-range COUNT.

### Named 1M fixture — MySQL→MySQL INSERT SELECT (2026-09-02)

Same-engine identity. One InnoDB consistent snapshot. Dest CREATE runs on
a second connection so DDL does not implicit-commit the snapshot.
`INSERT INTO dest SELECT … FROM src` on the snapshot connection.
`load_method=insert_select_mysql_same_instance`. Python does not format
TSV. Cross-host (or `DATAFLOW_MYSQL_MYSQL_INSERT_SELECT=0`) uses SELECT →
FIFO TSV → STRICT `LOAD DATA LOCAL INFILE` (proven live at 4 rows with
tab/newline/backslash/NULL; this host has one MySQL on 3306, so the named
1M is INSERT SELECT).

Reproduce: `BENCH_SRC=bench_1m BENCH_DEST=bench_mysql_clone python scripts/bench_mysql_to_mysql_million.py`

| Item | Value |
|------|-------|
| Source | MySQL **3306** (`bench_1m`) |
| Destination | MySQL **3306** `bench_mysql_clone` |
| Rows | **1,000,000** |
| Elapsed | **6.430 s** |
| rows/s | **155,514** |
| dest `COUNT(*)` | **1,000,000** |
| `rejected_rows` | **0** |
| `copy_split` | `insert_select` |
| PK partitions | 249999 + 250000 + 250000 + 250001 (each dest COUNT matched) |
| Spot-check | `EMP0000001` / `EMP0500000` / `EMP1000000` cells equal |
| Artifact | `/opt/cursor/artifacts/mysql_mysql_1000000_proof.json` |

~26× the 170.3 s row path. Dest COUNT + per-range COUNT held. Pytest:
`test_mysql_mysql_copy` + `test_mysql_pg_copy` + `test_mysql_load_data` +
`test_million_row_proof` **29 passed / 0 failed**.

### Named 1M fixture — PostgreSQL→PostgreSQL binary COPY (2026-09-02)

Same-engine identity. Binary `COPY … TO STDOUT` into `COPY … FROM STDIN`
on `full_refresh_append` (previously overwrite-only, so append stayed on
the row path — SCALE_MATRIX 406 rows/s at 20k). Dest missing/empty is
CREATE + COPY. Occupied dest declines to the row path. Same-table COPY
is refused. Proof is dest `COUNT(*)` plus the mapped-column engine
checksum (same digest on both sides of one snapshot). Elapsed includes
that checksum.

Reproduce: `BENCH_DEST=bench_pg_clone python scripts/bench_pg_to_pg_million.py`

| Item | Value |
|------|-------|
| Source | PostgreSQL **5432** (`bench_emp_1000000`) |
| Destination | PostgreSQL **5432** `bench_pg_clone` |
| Rows | **1,000,000** |
| Elapsed | **7.205 s** |
| rows/s | **138,784** |
| dest `COUNT(*)` | **1,000,000** |
| `rejected_rows` | **0** |
| `load_method` | `copy_binary_server_to_server` |
| Engine checksum | source = dest (`577077833021269069196831`) |
| Spot-check | `EMP0000001` / `EMP0500000` / `EMP1000000` cells equal |
| Artifact | `/opt/cursor/artifacts/pg_pg_1000000_proof.json` |

~342× the SCALE_MATRIX 406 rows/s overwrite row path (20k). Do not quote
the module's 553k rows/s COPY-only figure — this 7.205 s includes the
digest.

Occupied dest with a mapped single PK now skips complete ranges and
DELETE+reloads partial ones (parity with MySQL). Live 8_000-row integer
PK: delete one key in the third range, resume `replace_destination=False`
→ 3 skip + 1 reload, dest COUNT 8_000.
Pytest: `test_pk_resume_skips_complete_and_reloads_partial`. Occupied
dest without a mapped PK still declines to the row path.

Named 1M skip-all (dest already complete, `BENCH_KEEP_DEST=1`):

| Item | Value |
|------|-------|
| dest `COUNT(*)` | **1,000,000** |
| `partitions_skipped` | **4 / 4** |
| `action` | skip, skip, skip, skip |
| Elapsed | **0.762 s** (range COUNTs only — not a copy-speed figure) |
| Artifact | `/opt/cursor/artifacts/pg_pg_1000000_resume_skip_proof.json` |

Do not quote 1.3M rows/s from skip-all. Pytest: `test_copy_fast_path` +
`test_pg_pg_copy` + copy-path suite **59 passed / 0 failed**.

### Named 1M fixture — SQL Server→SQL Server INSERT SELECT (2026-09-02)

Same-engine identity on **SQL Server 2022 :1433**. Same-instance
`INSERT INTO dest WITH (TABLOCK) SELECT … FROM src WITH (HOLDLOCK, TABLOCK)`.
`ALLOW_SNAPSHOT_ISOLATION` is **OFF** on `dataflow`; the path reads
`sys.databases` and does **not** `ALTER DATABASE`. Cross-host declines
to the row path (no BCP yet). Proof is dest `COUNT(*)` plus per-PK-range
dest COUNT. Python does not format a row.

This named fixture is **2 columns** (`id BIGINT` PK, `label NVARCHAR(32)`),
not the 10-col employee table used for PG↔MySQL. Do not compare the
0.766 s figure to those 10-col COPY times as the same workload.

Reproduce: `BENCH_SRC=bench_ss_1000000 BENCH_DEST=bench_ss_clone python scripts/bench_sqlserver_sqlserver_million.py`

| Item | Value |
|------|-------|
| Source | SQL Server **1433** (`bench_ss_1000000`) |
| Destination | SQL Server **1433** `bench_ss_clone` |
| Rows | **1,000,000** |
| Elapsed | **0.766 s** |
| dest `COUNT(*)` | **1,000,000** |
| `rejected_rows` | **0** |
| `load_method` | `insert_select_sqlserver_same_instance` |
| `sqlserver_isolation` | `holdlock` |
| `copy_split` | `insert_select` |
| PK partitions | 250000 × 4 (each dest COUNT matched) |
| Artifact | `/opt/cursor/artifacts/sqlserver_sqlserver_1000000_proof.json` |

Do not quote 1.3M rows/s as a 10-col SLA. Same-instance INSERT SELECT
moves the engine's own pages; it is not cross-host BCP.

Occupied dest with a mapped single PK skips complete ranges and
DELETE+reloads partial ones. Live 8_000-row integer PK: delete one key
in the third range, resume `replace_destination=False` → 3 skip + 1
reload, dest COUNT 8_000.
Pytest: `test_live_sqlserver_resume_skips_complete_range`. Occupied dest
without a mapped PK declines to the row path.

Named 1M skip-all (dest already complete, `BENCH_KEEP_DEST=1`):

| Item | Value |
|------|-------|
| dest `COUNT(*)` | **1,000,000** |
| `partitions_skipped` | **4 / 4** |
| `action` | skip, skip, skip, skip |
| Elapsed | **0.393 s** (range COUNTs only — not a copy-speed figure) |
| Artifact | `/opt/cursor/artifacts/sqlserver_sqlserver_1000000_resume_skip_proof.json` |

Do not quote 2.5M rows/s from skip-all. Pytest: `test_sqlserver_sqlserver_copy`
**7 passed / 0 failed**; with `test_mysql_mysql_copy` + `test_copy_fast_path`
+ `test_pg_pg_copy` **42 passed / 0 failed**.

### Named 1M fixture — Oracle→Oracle INSERT APPEND (2026-09-02)

Same-engine identity on **Oracle 21c XE :1521** (`XEPDB1`). Same-instance
`LOCK TABLE src IN SHARE MODE` then `INSERT /*+ APPEND */ INTO dest
SELECT … FROM src` when dest is empty. Range resume stays conventional
INSERT SELECT (APPEND is empty-dest only). Cross-host declines to the
row path (no Data Pump / DB link yet). Proof is dest `COUNT(*)` plus
per-PK-range dest COUNT. Python does not format a row.

This named fixture is **2 columns** (`ID NUMBER` PK, `LABEL VARCHAR2(32)`),
not the 10-col employee table. Do not compare 16.312 s to PG↔MySQL 10-col
COPY times as the same workload. Oracle XE here is slower than SQL Server
INSERT SELECT on this host — that is a measured engine difference, not a
claim that the algorithm is worse.

Reproduce: `BENCH_SRC=BENCH_ORA_1000000 BENCH_DEST=BENCH_ORA_CLONE python scripts/bench_oracle_oracle_million.py`

| Item | Value |
|------|-------|
| Source | Oracle **1521** `XEPDB1` (`BENCH_ORA_1000000`) |
| Destination | Oracle **1521** `BENCH_ORA_CLONE` |
| Rows | **1,000,000** |
| Elapsed | **16.312 s** |
| dest `COUNT(*)` | **1,000,000** |
| `rejected_rows` | **0** |
| `load_method` | `insert_select_oracle_same_instance` |
| `copy_split` | `insert_select_append` |
| `oracle_lock` | `share` |
| PK partitions | 250000 × 4 (each dest COUNT matched) |
| Artifact | `/opt/cursor/artifacts/oracle_oracle_1000000_proof.json` |

Occupied dest with a mapped single PK skips complete ranges and
DELETE+reloads partial ones. Live 8_000-row integer PK: delete one key
in the third range, resume `replace_destination=False` → 3 skip + 1
reload, dest COUNT 8_000.
Pytest: `test_live_oracle_resume_skips_complete_range`. Occupied dest
without a mapped PK declines to the row path.

Named 1M skip-all (dest already complete, `BENCH_KEEP_DEST=1`):

| Item | Value |
|------|-------|
| dest `COUNT(*)` | **1,000,000** |
| `partitions_skipped` | **4 / 4** |
| `action` | skip, skip, skip, skip |
| Elapsed | **0.155 s** (range COUNTs only — not a copy-speed figure) |
| Artifact | `/opt/cursor/artifacts/oracle_oracle_1000000_resume_skip_proof.json` |

Do not quote 6.4M rows/s from skip-all. Pytest: `test_oracle_oracle_copy`
**7 passed / 0 failed**.

### Named 1M fixture — PostgreSQL→SQL Server COPY text + fast_executemany (2026-09-02)

Cross-engine identity on the **same 10-col employee fixture** as PG→MySQL
(`bench_emp_1000000`). PostgreSQL `COPY (SELECT …) TO STDOUT` text is
decoded (whole-field `\N` is NULL; empty string stays empty string) and
bound with serial pyodbc `fast_executemany`. Dest `COUNT(*)` plus per-PK-range
dest COUNT is the proof.

This is **not** BCP and **not** `BULK INSERT FORMAT='CSV'`. This host has
no client `bcp`; Linux SQL Server rejects `CODEPAGE`; CSV bulk collapses
quoted empty string `""` to NULL. Live 3-row fixture: NULL / `''` / `'x'`
land as NULL / empty string / `x` (`test_live_pg_sqlserver_empty_string_is_not_null`).

Empty dest COPYs the table once (`copy_split=serial`) — no parallel INSERT
into a PK dest. Occupied dest with a mapped single PK skips complete ranges
and DELETE+reloads partial ones.

Reproduce: `BENCH_DEST=bench_ss_from_pg python scripts/bench_pg_to_sqlserver_million.py`

| Item | Value |
|------|-------|
| Source | PostgreSQL **5432** (`bench_emp_1000000`, 10 columns, PK `employee_id`) |
| Destination | SQL Server **1433**, create-new `bench_ss_from_pg` |
| Rows | **1,000,000** |
| Elapsed | **13.335 s** |
| dest `COUNT(*)` | **1,000,000** |
| `rejected_rows` | **0** |
| `load_method` | `copy_text_pg_to_sqlserver_fast_executemany` |
| `copy_split` | `serial` |
| `copy_workers` | **1** |
| PK partitions | 249999 + 250000 + 250000 + 250001 (each dest COUNT matched) |
| Artifact | `/opt/cursor/artifacts/pg_sqlserver_1000000_proof.json` |

Slower than PG→MySQL FIFO+ctid **3.480 s** on the same 10-col fixture
because this path decodes COPY text in Python and INSERTs — LOAD DATA /
BCP are not available without silent empty-string loss. Quality held:
dest COUNT 1,000,000, empty string ≠ NULL.

Occupied dest with a mapped single PK skips complete ranges and
DELETE+reloads partial ones. Live 8_000-row integer PK: delete one key
in the third range, resume `replace_destination=False` → 3 skip + 1
reload, dest COUNT 8_000.
Pytest: `test_live_pg_sqlserver_resume_skips_complete_range`. Occupied dest
without a mapped PK declines to the row path.

Named 1M skip-all (dest already complete, `BENCH_KEEP_DEST=1`):

| Item | Value |
|------|-------|
| dest `COUNT(*)` | **1,000,000** |
| `partitions_skipped` | **4 / 4** |
| `action` | skip, skip, skip, skip |
| Elapsed | **0.862 s** (range COUNTs only — not a copy-speed figure) |
| Artifact | `/opt/cursor/artifacts/pg_sqlserver_1000000_resume_skip_proof.json` |

Do not quote 1.16M rows/s from skip-all. Pytest: `test_pg_sqlserver_copy`
**7 passed / 0 failed**; with SQL Server / MySQL / Oracle copy tests
**26 passed / 0 failed**.

### Named 1M fixture — SQL Server→PostgreSQL SELECT + COPY FROM STDIN (2026-09-02)

Reverse of PG→SQL Server on the **same 10-col employee data** now sitting
on SQL Server (`bench_ss_from_pg`). HOLDLOCK `SELECT` is encoded as
PostgreSQL COPY text (`fast_copy_text_value`, empty string stays empty)
and loaded with `COPY FROM STDIN` on one thread. Dest `COUNT(*)` plus
per-PK-range dest COUNT is the proof. SNAPSHOT isolation is **off** on
this database; the path never `ALTER DATABASE`.

Reproduce: `BENCH_SRC=bench_ss_from_pg BENCH_DEST=bench_pg_from_ss python scripts/bench_sqlserver_to_pg_million.py`

| Item | Value |
|------|-------|
| Source | SQL Server **1433** (`bench_ss_from_pg`, 10 columns, PK `employee_id`) |
| Destination | PostgreSQL **5432**, create-new `bench_pg_from_ss` |
| Rows | **1,000,000** |
| Elapsed | **5.681 s** |
| dest `COUNT(*)` | **1,000,000** |
| `rejected_rows` | **0** |
| `load_method` | `select_sqlserver_copy_from_stdin_pg` |
| `copy_split` | `serial` |
| `sqlserver_isolation` | `holdlock` |
| PK partitions | 249999 + 250000 + 250000 + 250001 (each dest COUNT matched) |
| Artifact | `/opt/cursor/artifacts/sqlserver_pg_1000000_proof.json` |

Faster than PG→SQL Server **13.335 s** on the same 10-col population
because PostgreSQL `COPY FROM STDIN` is native bulk; the reverse path
binds decoded COPY text with `fast_executemany`. Comparable to
MySQL→PG **7.605 s**. Quality held: dest COUNT 1,000,000, empty string ≠ NULL.

Occupied dest with a mapped single PK skips complete ranges and
DELETE+reloads partial ones. Live 8_000-row integer PK: delete one key
in the third range, resume `replace_destination=False` → 3 skip + 1
reload, dest COUNT 8_000.
Pytest: `test_live_sqlserver_pg_resume_skips_complete_range`. Occupied dest
without a mapped PK declines to the row path.

Named 1M skip-all (dest already complete, `BENCH_KEEP_DEST=1`):

| Item | Value |
|------|-------|
| dest `COUNT(*)` | **1,000,000** |
| `partitions_skipped` | **4 / 4** |
| `action` | skip, skip, skip, skip |
| Elapsed | **1.330 s** (range COUNTs only — not a copy-speed figure) |
| Artifact | `/opt/cursor/artifacts/sqlserver_pg_1000000_resume_skip_proof.json` |

Do not quote 752k rows/s from skip-all. Pytest: `test_sqlserver_pg_copy`
**7 passed / 0 failed**; with PG→SQL Server / SQL Server / MySQL / Oracle
copy tests **33 passed / 0 failed**.

### Named 1M fixture — PostgreSQL→Oracle COPY text + executemany (2026-09-02)

Cross-engine identity on the **same 10-col employee fixture** as PG→MySQL
(`bench_emp_1000000`). PostgreSQL `COPY (SELECT …) TO STDOUT` text is
decoded and bound with serial `oracledb.executemany`. Dest `COUNT(*)`
plus per-PK-range dest COUNT is the proof.

This is **not** SQL*Loader and **not** Data Pump. This host has no client
`sqlldr` / `impdp`. Oracle VARCHAR2 stores `''` as NULL (engine law).
Live 3-row fixture: PG NULL / `''` / `'x'` land as Oracle NULL / NULL /
`x`, and `empty_string_as_null_cells >= 1`
(`test_live_pg_oracle_empty_string_becomes_null`). Rows are not dropped.

Empty dest COPYs the table once (`copy_split=serial`). Occupied dest with
a mapped single PK skips complete ranges and DELETE+reloads partial ones.

Reproduce: `BENCH_DEST=BENCH_PG_ORA python scripts/bench_pg_to_oracle_million.py`

| Item | Value |
|------|-------|
| Source | PostgreSQL **5432** (`bench_emp_1000000`, 10 columns, PK `employee_id`) |
| Destination | Oracle **1521** `XEPDB1`, create-new `BENCH_PG_ORA` |
| Rows | **1,000,000** |
| Elapsed | **53.181 s** |
| dest `COUNT(*)` | **1,000,000** |
| `rejected_rows` | **0** |
| `empty_string_as_null_cells` | **0** (this fixture has no empty strings) |
| `load_method` | `copy_text_pg_to_oracle_executemany` |
| `copy_split` | `serial` |
| `copy_workers` | **1** |
| PK partitions | 249999 + 250000 + 250000 + 250001 (each dest COUNT matched) |
| Artifact | `/opt/cursor/artifacts/pg_oracle_1000000_proof.json` |

Slower than PG→SQL Server **13.335 s** on the same 10-col fixture because
this path is array INSERT, not native bulk (`sqlldr` is absent). Quality
held: dest COUNT 1,000,000. Empty string is NULL on VARCHAR2 and is
surfaced, not hidden.

Occupied dest with a mapped single PK skips complete ranges and
DELETE+reloads partial ones. Live 8_000-row integer PK: delete one key
in the third range, resume `replace_destination=False` → 3 skip + 1
reload, dest COUNT 8_000.
Pytest: `test_live_pg_oracle_resume_skips_complete_range`. Occupied dest
without a mapped PK declines to the row path.

Named 1M skip-all (dest already complete, `BENCH_KEEP_DEST=1`):

| Item | Value |
|------|-------|
| dest `COUNT(*)` | **1,000,000** |
| `partitions_skipped` | **4 / 4** |
| `action` | skip, skip, skip, skip |
| Elapsed | **0.577 s** (range COUNTs only — not a copy-speed figure) |
| Artifact | `/opt/cursor/artifacts/pg_oracle_1000000_resume_skip_proof.json` |

Do not quote 1.73M rows/s from skip-all. Pytest: `test_pg_oracle_copy`
**7 passed / 0 failed**.

### Named 1M fixture — Oracle→PostgreSQL SELECT + COPY FROM STDIN (2026-09-02)

Reverse of PG→Oracle on the **same 10-col employee data** now sitting on
Oracle (`BENCH_PG_ORA`). `LOCK TABLE src IN SHARE MODE` `SELECT` is
encoded as PostgreSQL COPY text and loaded with `COPY FROM STDIN` on one
thread. Dest `COUNT(*)` plus per-PK-range dest COUNT is the proof.

Oracle VARCHAR2 empty string is already NULL at the source, so COPY
emits `\\N`. That is engine law, not a row drop
(`test_live_oracle_pg_varchar2_empty_is_null`).

Reproduce: `BENCH_SRC=BENCH_PG_ORA BENCH_DEST=bench_pg_from_ora python scripts/bench_oracle_to_pg_million.py`

| Item | Value |
|------|-------|
| Source | Oracle **1521** `XEPDB1` (`BENCH_PG_ORA`, 10 columns, PK `employee_id`) |
| Destination | PostgreSQL **5432**, create-new `bench_pg_from_ora` |
| Rows | **1,000,000** |
| Elapsed | **4.278 s** |
| dest `COUNT(*)` | **1,000,000** |
| `rejected_rows` | **0** |
| `load_method` | `select_oracle_copy_from_stdin_pg` |
| `copy_split` | `serial` |
| `oracle_lock` | `share` |
| PK partitions | 249999 + 250000 + 250000 + 250001 (each dest COUNT matched) |
| Artifact | `/opt/cursor/artifacts/oracle_pg_1000000_proof.json` |

Faster than PG→Oracle **53.181 s** on the same 10-col population because
PostgreSQL `COPY FROM STDIN` is native bulk. Comparable to SQL Server→PG
**5.681 s** and MySQL→PG **7.605 s**. Quality held: dest COUNT 1,000,000.

Occupied dest with a mapped single PK skips complete ranges and
DELETE+reloads partial ones. Live 8_000-row integer PK: delete one key
in the third range, resume `replace_destination=False` → 3 skip + 1
reload, dest COUNT 8_000.
Pytest: `test_live_oracle_pg_resume_skips_complete_range`. Occupied dest
without a mapped PK declines to the row path.

Named 1M skip-all (dest already complete, `BENCH_KEEP_DEST=1`):

| Item | Value |
|------|-------|
| dest `COUNT(*)` | **1,000,000** |
| `partitions_skipped` | **4 / 4** |
| `action` | skip, skip, skip, skip |
| Elapsed | **1.041 s** (range COUNTs only — not a copy-speed figure) |
| Artifact | `/opt/cursor/artifacts/oracle_pg_1000000_resume_skip_proof.json` |

Do not quote 960k rows/s from skip-all. Pytest: `test_oracle_pg_copy`
**7 passed / 0 failed**; combined identity-copy matrix
(`test_pg_oracle_copy` + `test_oracle_pg_copy` + SQL Server / MySQL /
Oracle / PG copy tests) **52 passed / 0 failed**.

### Named 1M fixture — MySQL→SQL Server SELECT + fast_executemany (2026-09-02)

Cross-engine identity on the **same 10-col employee fixture** now sitting
on MySQL (`bench_1m`). InnoDB `START TRANSACTION WITH CONSISTENT SNAPSHOT`
`SELECT` is bound with serial pyodbc `fast_executemany`. Dest `COUNT(*)`
plus per-PK-range dest COUNT is the proof.

This is **not** BCP and **not** `BULK INSERT FORMAT='CSV'`. Live 3-row
fixture: NULL / `''` / `'x'` land as NULL / empty string / `x`.

Empty dest SELECTs the table once (`copy_split=serial`). Occupied dest
with a mapped single PK skips complete ranges and DELETE+reloads partial
ones.

Reproduce: `BENCH_SRC=bench_1m BENCH_DEST=bench_ss_from_mysql python scripts/bench_mysql_to_sqlserver_million.py`

| Item | Value |
|------|-------|
| Source | MySQL **3306** (`bench_1m`, 10 columns, PK `employee_id`) |
| Destination | SQL Server **1433**, create-new `bench_ss_from_mysql` |
| Rows | **1,000,000** |
| Elapsed | **15.517 s** |
| dest `COUNT(*)` | **1,000,000** |
| `rejected_rows` | **0** |
| `load_method` | `select_mysql_fast_executemany_sqlserver` |
| `copy_split` | `serial` |
| `copy_workers` | **1** |
| PK partitions | 249999 + 250000 + 250000 + 250001 (each dest COUNT matched) |
| Artifact | `/opt/cursor/artifacts/mysql_sqlserver_1000000_proof.json` |

Comparable to PG→SQL Server **13.335 s** on the same 10-col population
(same `fast_executemany` writer). Quality held: dest COUNT 1,000,000,
empty string ≠ NULL.

Occupied dest with a mapped single PK skips complete ranges and
DELETE+reloads partial ones. Live 8_000-row integer PK: delete one key
in the third range, resume `replace_destination=False` → 3 skip + 1
reload, dest COUNT 8_000.
Pytest: `test_live_mysql_sqlserver_resume_skips_complete_range`. Occupied
dest without a mapped PK declines to the row path.

Named 1M skip-all (dest already complete, `BENCH_KEEP_DEST=1`):

| Item | Value |
|------|-------|
| dest `COUNT(*)` | **1,000,000** |
| `partitions_skipped` | **4 / 4** |
| `action` | skip, skip, skip, skip |
| Elapsed | **0.920 s** (range COUNTs only — not a copy-speed figure) |
| Artifact | `/opt/cursor/artifacts/mysql_sqlserver_1000000_resume_skip_proof.json` |

Do not quote 1.09M rows/s from skip-all. Pytest: `test_mysql_sqlserver_copy`
**6 passed / 0 failed**.

### Named 1M fixture — SQL Server→MySQL SELECT + STRICT LOAD DATA (2026-09-02)

Reverse of MySQL→SQL Server on the **same 10-col employee data** now
sitting on SQL Server (`bench_ss_from_mysql`). HOLDLOCK `SELECT` is
encoded as LOAD DATA TSV on one thread into a tempfile, then STRICT
`LOAD DATA LOCAL INFILE`. Dest `COUNT(*)` plus per-PK-range dest COUNT
is the proof. A FIFO + pyodbc pump is **not** used (deadlock class
measured on SQL Server→PG).

Live 3-row fixture: NULL / `''` / `'x'` land as NULL / empty string / `x`.

Reproduce: `BENCH_SRC=bench_ss_from_mysql BENCH_DEST=bench_mysql_from_ss python scripts/bench_sqlserver_to_mysql_million.py`

| Item | Value |
|------|-------|
| Source | SQL Server **1433** (`bench_ss_from_mysql`, 10 columns, PK `employee_id`) |
| Destination | MySQL **3306**, create-new `bench_mysql_from_ss` |
| Rows | **1,000,000** |
| Elapsed | **10.339 s** |
| dest `COUNT(*)` | **1,000,000** |
| `rejected_rows` | **0** |
| `load_method` | `select_sqlserver_load_data_mysql` |
| `copy_split` | `serial` |
| PK partitions | 249999 + 250000 + 250000 + 250001 (each dest COUNT matched) |
| Artifact | `/opt/cursor/artifacts/sqlserver_mysql_1000000_proof.json` |

Faster than MySQL→SQL Server **15.517 s** because MySQL LOAD DATA is
native bulk; the reverse path binds with `fast_executemany`. Slower than
PG→MySQL FIFO **3.480 s** because this path materializes a tempfile
instead of overlapping a FIFO (pyodbc FIFO deadlocks). Quality held:
dest COUNT 1,000,000, empty string ≠ NULL.

Occupied dest with a mapped single PK skips complete ranges and
DELETE+reloads partial ones. Live 8_000-row integer PK: delete one key
in the third range, resume `replace_destination=False` → 3 skip + 1
reload, dest COUNT 8_000.
Pytest: `test_live_sqlserver_mysql_resume_skips_complete_range`. Occupied
dest without a mapped PK declines to the row path.

Named 1M skip-all (dest already complete, `BENCH_KEEP_DEST=1`):

| Item | Value |
|------|-------|
| dest `COUNT(*)` | **1,000,000** |
| `partitions_skipped` | **4 / 4** |
| `action` | skip, skip, skip, skip |
| Elapsed | **0.898 s** (range COUNTs only — not a copy-speed figure) |
| Artifact | `/opt/cursor/artifacts/sqlserver_mysql_1000000_resume_skip_proof.json` |

Do not quote 1.11M rows/s from skip-all. Pytest: `test_sqlserver_mysql_copy`
**7 passed / 0 failed**.

### Named 1M fixture — MySQL→Oracle SELECT + executemany (2026-09-02)

MySQL has no `COPY TO STDOUT`. One InnoDB `START TRANSACTION WITH
CONSISTENT SNAPSHOT` streams `SELECT` (SSCursor) into
`oracledb.executemany` (`:1,:2` binds, 50k batches). Not `sqlldr` /
Data Pump. Dest `COUNT(*)` plus per-PK-range dest COUNT is the proof.

Oracle VARCHAR2 stores `''` as NULL (engine law). Empty-string cells
are bound as NULL and counted in `empty_string_as_null_cells`. Live
3-row fixture: NULL / `''` / `'x'` land as NULL / NULL / `x` with
`empty_string_as_null_cells >= 1`. Do not claim VARCHAR2 cell identity
for empty string. This 1M employee fixture has no empty strings
(`empty_string_as_null_cells=0`).

Reproduce: `BENCH_SRC=bench_1m BENCH_DEST=BENCH_MY_ORA python scripts/bench_mysql_to_oracle_million.py`

| Item | Value |
|------|-------|
| Source | MySQL **3306** (`bench_1m`, 10 columns, PK `employee_id`) |
| Destination | Oracle **1521/XEPDB1**, create-new `BENCH_MY_ORA` |
| Rows | **1,000,000** |
| Elapsed | **51.974 s** |
| dest `COUNT(*)` | **1,000,000** |
| `rejected_rows` | **0** |
| `load_method` | `select_mysql_executemany_oracle` |
| `copy_split` | `serial` |
| PK partitions | 249999 + 250000 + 250000 + 250001 (each dest COUNT matched) |
| `empty_string_as_null_cells` | **0** |
| Artifact | `/opt/cursor/artifacts/mysql_oracle_1000000_proof.json` |

Matches PG→Oracle **53.181 s** on the same bind path (executemany, not
INSERT SELECT). Slower than MySQL→SQL Server **15.517 s** because
SQL Server `fast_executemany` + TABLOCK is cheaper than Oracle array
INSERT VALUES. Quality held: dest COUNT 1,000,000, empty-string law
surfaced.

Occupied dest with a mapped single PK skips complete ranges and
DELETE+reloads partial ones. Live 8_000-row integer PK: delete one key
in the third range, resume `replace_destination=False` → 3 skip + 1
reload, dest COUNT 8_000.
Pytest: `test_live_mysql_oracle_resume_skips_complete_range`. Occupied
dest without a mapped PK declines to the row path.

Named 1M skip-all (dest already complete, `BENCH_KEEP_DEST=1`):

| Item | Value |
|------|-------|
| dest `COUNT(*)` | **1,000,000** |
| `partitions_skipped` | **4 / 4** |
| `action` | skip, skip, skip, skip |
| Elapsed | **0.634 s** (range COUNTs only — not a copy-speed figure) |
| Artifact | `/opt/cursor/artifacts/mysql_oracle_1000000_resume_skip_proof.json` |

Do not quote 1.58M rows/s from skip-all. Pytest: `test_mysql_oracle_copy`
**7 passed / 0 failed**.

### Named 1M fixture — Oracle→MySQL SELECT + STRICT LOAD DATA (2026-09-02)

Reverse of MySQL→Oracle on the **same 10-col employee data** now
sitting on Oracle (`BENCH_MY_ORA`). SHARE-lock `SELECT` is encoded as
LOAD DATA TSV on one thread into a tempfile, then STRICT
`LOAD DATA LOCAL INFILE`. Dest `COUNT(*)` plus per-PK-range dest COUNT
is the proof. A FIFO + oracledb pump is **not** used (same deadlock
class as pyodbc FIFO).

Oracle `''` is already NULL at source, so LOAD DATA emits `\N`. Live
3-row fixture: NULL / `''` / `'x'` land as NULL / NULL / `x` with
`varchar2_empty_stored_as_null=true`. That is engine law, not a row drop.

Reproduce: `BENCH_SRC=BENCH_MY_ORA BENCH_DEST=bench_mysql_from_ora python scripts/bench_oracle_to_mysql_million.py`

| Item | Value |
|------|-------|
| Source | Oracle **1521/XEPDB1** (`BENCH_MY_ORA`, 10 columns, PK `employee_id`) |
| Destination | MySQL **3306**, create-new `bench_mysql_from_ora` |
| Rows | **1,000,000** |
| Elapsed | **11.154 s** |
| dest `COUNT(*)` | **1,000,000** |
| `rejected_rows` | **0** |
| `load_method` | `select_oracle_load_data_mysql` |
| `copy_split` | `serial` |
| PK partitions | 249999 + 250000 + 250000 + 250001 (each dest COUNT matched) |
| Artifact | `/opt/cursor/artifacts/oracle_mysql_1000000_proof.json` |

Faster than MySQL→Oracle **51.974 s** because MySQL LOAD DATA is native
bulk; the reverse path binds with `executemany`. Close to SQL
Server→MySQL **10.339 s** (same tempfile LOAD DATA pattern). Quality
held: dest COUNT 1,000,000.

Occupied dest with a mapped single PK skips complete ranges and
DELETE+reloads partial ones. Live 8_000-row integer PK: delete one key
in the third range, resume `replace_destination=False` → 3 skip + 1
reload, dest COUNT 8_000.
Pytest: `test_live_oracle_mysql_resume_skips_complete_range`. Occupied
dest without a mapped PK declines to the row path.

Named 1M skip-all (dest already complete, `BENCH_KEEP_DEST=1`):

| Item | Value |
|------|-------|
| dest `COUNT(*)` | **1,000,000** |
| `partitions_skipped` | **4 / 4** |
| `action` | skip, skip, skip, skip |
| Elapsed | **0.763 s** (range COUNTs only — not a copy-speed figure) |
| Artifact | `/opt/cursor/artifacts/oracle_mysql_1000000_resume_skip_proof.json` |

Do not quote 1.31M rows/s from skip-all. Pytest: `test_oracle_mysql_copy`
**7 passed / 0 failed**; combined identity-copy matrix **79 passed / 0 failed**.

### Named 1M fixture — SQL Server→Oracle SELECT + executemany (2026-09-02)

SQL Server has no `COPY TO STDOUT` and this host has no client `bcp`.
One HOLDLOCK (or SNAPSHOT) `SELECT` is bound with `oracledb.executemany`
(`:1,:2` binds, 50k batches). Not `sqlldr` / Data Pump / BCP. Dest
`COUNT(*)` plus per-PK-range dest COUNT is the proof.

Oracle VARCHAR2 stores `''` as NULL (engine law). Empty-string cells
from NVARCHAR are bound as NULL and counted in
`empty_string_as_null_cells`. Live 3-row fixture: NULL / `''` / `'x'`
land as NULL / NULL / `x` with `empty_string_as_null_cells >= 1`. Do
not claim VARCHAR2 cell identity for empty string. This 1M employee
fixture has no empty strings (`empty_string_as_null_cells=0`).

Reproduce: `BENCH_SRC=bench_ss_from_mysql BENCH_DEST=BENCH_SS_ORA python scripts/bench_sqlserver_to_oracle_million.py`

| Item | Value |
|------|-------|
| Source | SQL Server **1433** (`bench_ss_from_mysql`, 10 columns, PK `employee_id`) |
| Destination | Oracle **1521/XEPDB1**, create-new `BENCH_SS_ORA` |
| Rows | **1,000,000** |
| Elapsed | **44.732 s** |
| dest `COUNT(*)` | **1,000,000** |
| `rejected_rows` | **0** |
| `load_method` | `select_sqlserver_executemany_oracle` |
| `copy_split` | `serial` |
| PK partitions | 249999 + 250000 + 250000 + 250001 (each dest COUNT matched) |
| `empty_string_as_null_cells` | **0** |
| Artifact | `/opt/cursor/artifacts/sqlserver_oracle_1000000_proof.json` |

Faster than MySQL→Oracle **51.974 s** and PG→Oracle **53.181 s** on the
same bind path (pyodbc fetch into executemany vs MySQL SSCursor / PG
COPY-text decode). Quality held: dest COUNT 1,000,000, empty-string law
surfaced.

Occupied dest with a mapped single PK skips complete ranges and
DELETE+reloads partial ones. Live 8_000-row integer PK: delete one key
in the third range, resume `replace_destination=False` → 3 skip + 1
reload, dest COUNT 8_000.
Pytest: `test_live_sqlserver_oracle_resume_skips_complete_range`. Occupied
dest without a mapped PK declines to the row path.

Named 1M skip-all (dest already complete, `BENCH_KEEP_DEST=1`):

| Item | Value |
|------|-------|
| dest `COUNT(*)` | **1,000,000** |
| `partitions_skipped` | **4 / 4** |
| `action` | skip, skip, skip, skip |
| Elapsed | **0.634 s** (range COUNTs only — not a copy-speed figure) |
| Artifact | `/opt/cursor/artifacts/sqlserver_oracle_1000000_resume_skip_proof.json` |

Do not quote 1.58M rows/s from skip-all. Pytest: `test_sqlserver_oracle_copy`
**7 passed / 0 failed**.

### Named 1M fixture — Oracle→SQL Server SELECT + fast_executemany (2026-09-02)

Reverse of SQL Server→Oracle on the **same 10-col employee data** now
sitting on Oracle (`BENCH_SS_ORA`). SHARE-lock `SELECT` is bound with
pyodbc `fast_executemany` (`INSERT … WITH (TABLOCK)`). Dest `COUNT(*)`
plus per-PK-range dest COUNT is the proof. Not BCP / CSV `BULK INSERT`
(quoted empty string collapses to NULL on Linux SQL Server). Dest
connection is `_close_ss`'d so TABLOCK is not pooled.

Oracle `''` is already NULL at source, so SQL Server NVARCHAR stores
NULL. Live 3-row fixture: NULL / `''` / `'x'` land as NULL / NULL / `x`
with `varchar2_empty_stored_as_null=true`. That is engine law, not a
row drop.

Reproduce: `BENCH_SRC=BENCH_SS_ORA BENCH_DEST=bench_ss_from_ora python scripts/bench_oracle_to_sqlserver_million.py`

| Item | Value |
|------|-------|
| Source | Oracle **1521/XEPDB1** (`BENCH_SS_ORA`, 10 columns, PK `employee_id`) |
| Destination | SQL Server **1433**, create-new `bench_ss_from_ora` |
| Rows | **1,000,000** |
| Elapsed | **11.94 s** |
| dest `COUNT(*)` | **1,000,000** |
| `rejected_rows` | **0** |
| `load_method` | `select_oracle_fast_executemany_sqlserver` |
| `copy_split` | `serial` |
| PK partitions | 249999 + 250000 + 250000 + 250001 (each dest COUNT matched) |
| Artifact | `/opt/cursor/artifacts/oracle_sqlserver_1000000_proof.json` |

Faster than SQL Server→Oracle **44.732 s** because SQL Server
`fast_executemany` + TABLOCK is cheaper than Oracle array INSERT
VALUES. Close to MySQL→SQL Server **15.517 s** on the same bind path.
Quality held: dest COUNT 1,000,000.

Occupied dest with a mapped single PK skips complete ranges and
DELETE+reloads partial ones. Live 8_000-row integer PK: delete one key
in the third range, resume `replace_destination=False` → 3 skip + 1
reload, dest COUNT 8_000.
Pytest: `test_live_oracle_sqlserver_resume_skips_complete_range`. Occupied
dest without a mapped PK declines to the row path.

Named 1M skip-all (dest already complete, `BENCH_KEEP_DEST=1`):

| Item | Value |
|------|-------|
| dest `COUNT(*)` | **1,000,000** |
| `partitions_skipped` | **4 / 4** |
| `action` | skip, skip, skip, skip |
| Elapsed | **0.704 s** (range COUNTs only — not a copy-speed figure) |
| Artifact | `/opt/cursor/artifacts/oracle_sqlserver_1000000_resume_skip_proof.json` |

Do not quote 1.42M rows/s from skip-all. Pytest: `test_oracle_sqlserver_copy`
**6 passed / 0 failed**; combined identity-copy matrix **92 passed / 0 failed**.

### Named 1M fixture — PostgreSQL→Iceberg COPY CSV + snapshot (2026-09-03)

SQL cartesian among PostgreSQL / MySQL / SQL Server / Oracle is closed.
Next live engine on this host is Iceberg REST (`127.0.0.1:8181`) with a
**local** warehouse (`file:///tmp/iceberg-rest-wh`). Identity bulk is
`COPY (SELECT …) TO STDOUT` CSV into one Arrow table and one catalog
snapshot commit. Python never builds dict rows. Dest COUNT is Parquet
file footers via `destination_row_count` / `iceberg_mor`, never
`scan().count()`. Empty dest is CoW snapshot append — **not**
`MERGE INTO`. Glue / Nessie are not on this host (Planned).

Do not mix this 1.344 s with PG→MySQL 3.480 s: Iceberg dest is a local
file warehouse, not a network SQL engine.

Reproduce: `BENCH_SRC=bench_emp_1000000 BENCH_DEST=bench_pg_iceberg python scripts/bench_pg_to_iceberg_million.py`

| Item | Value |
|------|-------|
| Source | PostgreSQL **5432** (`bench_emp_1000000`, 10 columns, PK `employee_id`) |
| Destination | Iceberg REST **8181**, create-new `default.bench_pg_iceberg` |
| Warehouse | `file:///tmp/iceberg-rest-wh` (local files, not S3) |
| Rows | **1,000,000** |
| Elapsed | **1.344 s** |
| dest COUNT (file footers) | **1,000,000** |
| `rejected_rows` | **0** |
| `load_method` | `copy_csv_pg_to_iceberg_snapshot` |
| `copy_split` | `serial` |
| `iceberg_write` | `append` |
| `shard_mode` | `table` |
| Artifact | `/opt/cursor/artifacts/pg_iceberg_1000000_proof.json` |

Live 3-row NULL / `''` / `'x'` on Iceberg string lands as NULL / `''` /
`x` — empty string is preserved (unlike Oracle VARCHAR2). Occupied dest
whose footer COUNT already equals the source snapshot is skip-complete.
Occupied dest with a different COUNT declines (leftover MERGE stays on
the row path). Iceberg catalog commits are snapshot-isolated, so this
COPY is serial.

Named 1M skip-all (dest already complete, `BENCH_KEEP_DEST=1`):

| Item | Value |
|------|-------|
| dest COUNT (file footers) | **1,000,000** |
| `partitions_skipped` | **1 / 1** |
| `copy_split` | skip |
| `iceberg_write` | skip |
| Elapsed | **0.648 s** (COUNTs only — not a copy-speed figure) |
| Artifact | `/opt/cursor/artifacts/pg_iceberg_1000000_resume_skip_proof.json` |

Do not quote 1.54M rows/s from skip-all. Pytest: `test_pg_iceberg_copy`
**10 passed / 0 failed**.

### Named 1M fixture — Iceberg→PostgreSQL snapshot Parquet + COPY FROM STDIN (2026-09-03)

Reverse of PostgreSQL→Iceberg on the **same 10-col employee data** now
sitting on Iceberg REST (`bench_pg_iceberg`). Current-snapshot Parquet
files are encoded as COPY text into `COPY FROM STDIN`. Source COUNT is
file footers, never `scan().count()`. Payload is never
`scan().to_arrow()`. Dest proof is PostgreSQL `COUNT(*)`.

Warehouse is still **local files**, not S3. Do not mix 2.898 s with
network SQL engine times.

Reproduce: `BENCH_SRC=bench_pg_iceberg BENCH_DEST=bench_pg_from_iceberg python scripts/bench_iceberg_to_pg_million.py`

| Item | Value |
|------|-------|
| Source | Iceberg REST **8181** (`default.bench_pg_iceberg`, 10 columns) |
| Destination | PostgreSQL **5432**, create-new `bench_pg_from_iceberg` |
| Warehouse | `file:///tmp/iceberg-rest-wh` (local files, not S3) |
| Rows | **1,000,000** |
| Elapsed | **2.898 s** |
| dest `COUNT(*)` | **1,000,000** |
| Iceberg source COUNT (file footers) | **1,000,000** |
| `rejected_rows` | **0** |
| `load_method` | `iceberg_parquet_copy_from_stdin_pg` |
| `copy_split` | `serial` |
| `iceberg_read` | `snapshot_parquet` |
| Artifact | `/opt/cursor/artifacts/iceberg_pg_1000000_proof.json` |

Live 3-row NULL / `''` / `'x'` round-trips as NULL / `''` / `x`. Occupied
dest whose `COUNT(*)` already equals the source footer COUNT is
skip-complete. Occupied dest with a different COUNT declines. MoR
snapshots (delete files) decline.

Named 1M skip-all (dest already complete, `BENCH_KEEP_DEST=1`):

| Item | Value |
|------|-------|
| dest `COUNT(*)` | **1,000,000** |
| `partitions_skipped` | **1 / 1** |
| `copy_split` | skip |
| `iceberg_read` | skip |
| Elapsed | **0.052 s** (COUNTs only — not a copy-speed figure) |
| Artifact | `/opt/cursor/artifacts/iceberg_pg_1000000_resume_skip_proof.json` |

Do not quote 19.3M rows/s from skip-all. Pytest: `test_iceberg_pg_copy`
**9 passed / 0 failed**.

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
- Copy-path proof is dest COUNT vs the source snapshot, not a second
  checksum reread. SQL engines use dest `COUNT(*)`. Iceberg dest COUNT is
  file footers, never `scan().count()`. Empty Iceberg dest is CoW snapshot
  append, not MERGE INTO. The 1.344 s PG→Iceberg figure is a **local**
  warehouse, not S3/Glue.
- Unfit cells on SQL COPY paths fail closed (STRICT + rollback). Quarantine
  remains on the row path.
- Profiled runs (`BENCH_PROFILE=1`) are ~2× slower by construction and are
  diagnostic only — never quote them as throughput.
