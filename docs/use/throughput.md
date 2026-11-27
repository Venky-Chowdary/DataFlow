# Throughput

How fast a database-to-database transfer moves rows, what makes it slow or
fast, and how to measure it yourself. Every number on this page comes from
the benchmark harness below, with the date, the commit and the machine. A
number without those is not a claim.

## Two paths

A database-to-database run takes one of two paths. The run summary names it
in `load_methods`.

| Path | When | What moves the rows | Rough speed |
|---|---|---|---|
| Server-to-server copy (`copy_binary_server_to_server`, `copy_text_load_data_*`, …) | Identity routes: every column is a plain carry onto the same type, append / overwrite / upsert / identity incremental, no source filter, no foreign-key guard, no risk contract on a column, no resume | `COPY` / `LOAD DATA` between the engines; rows never enter Python | the copy step: about 60,000 rows/s locally (PostgreSQL → PostgreSQL, 100k rows in 1.7 s), plus Gate-8 |
| Streaming row path (`insert`) | Everything else: transforms, cross-engine type changes, foreign-key orphan checks, signed risk contracts, source filters, resumed runs | Python reads a page, converts every cell, quarantines unfit cells, binds and inserts, and fingerprints every cell for Gate-8 | 450–550 rows/s per process on the mixed table, Gate-8 included (table below) |

Both paths keep the same guarantees: NULL and `''` stay distinct, held rows
are quarantined with a reason, the row accounting balances, and Gate-8
verifies the destination against an independent source re-read.

A PostgreSQL → PostgreSQL table with a `jsonb` column now takes the binary
copy when the destination column is `jsonb` too or does not exist yet
(`jsonb` → `jsonb` is a plain carry; the copy moves the stored datum). Into an
existing `text` or `json` column the cast decides the stored text, so that
route stays on the row path in every mode. The incremental and upsert copies
also re-check every live destination column type (exact names: `"Doc"` and
`"doc"` are two columns) inside their merge transaction, under a lock, and
fall back to the row path if a column changed after the route was chosen.
Before, the `json` cast the mapper stamps on JSON columns sent every such
table down the row path. A `json` (text) column still takes the row path.

## Measured numbers

Harness: `apps/api/benchmarks/db_throughput.py`. Table: 11 mixed columns
(BIGINT key, INTEGER, NUMERIC(18,4), DOUBLE, TIMESTAMP, DATE, BOOLEAN,
VARCHAR, TEXT with NULL / `''` / tab-newline-backslash, JSON, non-ASCII
text), deterministic, NULLs in every nullable column. Mode
`full_refresh_overwrite`, validation `strict`. Each run also re-reads both
tables in key order and compares every cell (`independent_check`); every run
below passed it.

Local machine: 4 vCPU, PostgreSQL 16, MariaDB 10.11, Python 3.13, 2026-11-21.

| Route, 100k rows | Path | Before (70cb4669c) rows/s | After rows/s | Verification s before → after | Peak RSS before → after |
|---|---|---|---|---|---|
| PostgreSQL → PostgreSQL | row path (`--row-path`) | 244 | 524 | 231 → 100 | 581 → 536 MB |
| PostgreSQL → PostgreSQL | default: binary copy after this change (was the row path) | 244 | 1,768 | 231 → 52 | 581 → 516 MB |
| PostgreSQL → MariaDB | row path | 236 | 446 | 227 → 125 | 603 → 549 MB |
| MariaDB → PostgreSQL | row path | 271 | 550 | 236 → 94 | 572 → 530 MB |

These seven runs ran one after another on one machine (load average 0.7 at
the start, up to 5–10 later from other work on it). The phases of the
PostgreSQL → PostgreSQL row-path run, before → after: transform and write
171 s → 82 s, in-stream checksum 54 s → 30 s, Gate-8 reconciliation 177 s →
71 s. On the copy route the copy itself took 1.7 s for 100k rows; the rest of
the 57 s was Gate-8, almost all of it the in-memory ladder described next.

CI (`throughput-smoke`, GitHub-hosted `ubuntu-latest`, MySQL 8.0, Python
3.12, 200k rows, run 44923502709): PostgreSQL → PostgreSQL 3,276 rows/s on the
default binary copy and 558 on the forced row path, PostgreSQL → MySQL 953,
MySQL → PostgreSQL 642; every route passed the independent cell check; peak
RSS 850–910 MB (at 200k the ladder below holds both populations in memory).
A second job on a later commit ran every route at about half those rates on a
slower runner (run 44936017037), so the smoke scales its floors by a CPU
calibration of the runner (`benchmarks/throughput_smoke.py`).

At 1M rows (same machine, 2026-11-21, under heavy load from other work —
load average 2–18 during these runs):

| Route, 1M rows | Before rows/s | After rows/s | Wall | Verification s | Peak RSS |
|---|---|---|---|---|---|
| PostgreSQL → MariaDB, row path | 275 | 623 | 60.6 → 26.8 min | 1,038 → 591 | 484 → 480 MB |
| PostgreSQL → PostgreSQL, default path | not run at 1M (was the row path: 244 at 100k) | 27,820 (binary copy) | 36 s | 6 | 183 MB |

All three 1M runs passed the independent cell check on all 1M rows. Above 250k
rows the ladder does not run, so the PostgreSQL → PostgreSQL copy verifies by
the in-database digest and count alone and finishes in seconds.

At or below 250,000 rows Gate-8 also runs the in-memory verification ladder
(column aggregates L2 and cell localisation L4/L5); above it the ladder is
skipped and says so (`coverage_limit`), so per-row cost at 1M rows is lower
than at 100k.

## Why the row path is slow, and what changed

Profiling (py-spy, cProfile) on the mixed table showed the row path is CPU
bound in Python, not in the database: the INSERTs were under 1% of samples.
Each cell went through the generic converters three to five times — the
write (transform, unfit-value quarantine checks, bind), the write-pass
fingerprint, the Gate-8 source re-read and destination read-back
fingerprints, and the ladder's aggregates — and each of those calls walked the
whole generic dispatch (sentinels, carried kinds, locale pins, bind routing,
carrier quantization) to arrive, for an ordinary cell, at `str(int)` or an ISO
wall clock.

What changed, with no change to any result:

- **Per-column fast paths** (`apps/api/services/cell_fastpath.py`). A column's
  plan is compiled once from the same predicates the full path consults; an
  ordinary typed or plain-text cell of a planned column gets the full path's
  answer directly, anything else takes the full path. Each plan is
  self-checked against the full path on a probe set before use, and
  `tests/test_cell_fastpath_equivalence.py` compares both paths over
  generated corpora for every plan kind, engine, transform, locale pin and
  strict mode. `DATAFLOW_CELL_FASTPATH=0` turns them off.
- **Ladder aggregates** parse each cell's number once instead of five times.
- **ISO timestamps in the integrity audit** no longer try twenty `strptime`
  formats before matching.
- **Batch salvage** (`apps/api/connectors/batch_salvage.py`). When a write
  chunk fails with a row-level data error under a quarantine policy, the
  PostgreSQL and MySQL writers split the chunk and retry the halves as
  batches, reaching the per-row path only for the bad rows' neighbourhood
  (about `2k·log2(n/k)` attempts for `k` bad rows in `n`, instead of `n`).
  Rows land or are held out exactly as before, in the same order, each with
  its own error. SQL Server and the generic SQL writer still salvage row by
  row, and so does MySQL into a non-transactional or temporary table
  (MyISAM, Aria, MEMORY; judged once per table per run through the session's
  `SHOW CREATE TABLE`): a failed multi-row INSERT there keeps the rows before
  its bad row.
  On such a table a chunk with a refused row already fails the run today
  (the chunk INSERT wrote those rows before the per-row salvage retried
  them; measured on 70cb4669c too) — an open gap, not changed here. A cancel is polled before every attempt: nothing lands after the poll
  that sees it. On MySQL each salvage attempt commits, so up to one in-flight
  sub-batch (at most half a chunk) can land between the cancel request and
  that poll, where row-by-row landed at most one row; the cancel still reports
  exactly the rows that landed. PostgreSQL rolls the whole salvaged chunk back
  on a cancel, as before.

## Kill and resume

`apps/api/tests/test_kill_resume_live.py` SIGKILLs a running transfer after
the durable checkpoint shows committed batches, starts a new process that
resumes the same job, and checks every cell of the destination against the
source: PostgreSQL ↔ PostgreSQL, MariaDB ↔ MariaDB, PostgreSQL → MariaDB and
MariaDB → PostgreSQL, in `full_refresh_overwrite`, `incremental_deduped`,
`full_refresh_append` and `incremental_append`. No row is lost or duplicated
in any of the sixteen cases. In the append modes, where a rerun from the start
would duplicate, a destination trigger counts what the second process writes:
never more than the rows after the checkpoint it resumed from (rows that
landed after the last checkpoint save are re-applied through the write
ledger, not duplicated). The same test with the second process started
without `resume` fails all eight append cases.

## Run it yourself

From `apps/api` with `PYTHONPATH=.` and the `PG*` / `MYSQL_*` variables set:

```
python -m benchmarks.db_throughput --route pg-mysql --rows 1000000 --out result.json
python -m benchmarks.db_throughput --route pg-pg --rows 200000 --row-path --out row.json
```

The report holds rows/s over the whole engine call, the engine's phase
profile, verification seconds (Gate-8 reconciliation plus the in-stream
checksum), peak RSS of the process, and the independent check.

- CI: the `throughput-smoke` job in `.github/workflows/ci.yml` runs, on
  every PR, 100k rows on the row path (PostgreSQL → PostgreSQL and
  PostgreSQL → MySQL) and 20k rows on the PostgreSQL → PostgreSQL binary copy
  that carries `jsonb` (its load method is checked), against the floors in
  `apps/api/benchmarks/throughput_floors.json`, scaled to the runner by a CPU
  calibration (never below 75% of a floor). It is report-only — not in the
  Required PR gate — until it has five consecutive green runs. The other
  floors (copy at 200k, MySQL → PostgreSQL) are checked by the dispatch
  workflow's `check_floors` step.
- 10M / 100M: the manual **Throughput benchmark** workflow
  (`.github/workflows/throughput-benchmark.yml`). A hosted runner fits 10M
  rows of this table on disk; 100M needs a self-hosted runner, and on the row
  path needs more than the 6-hour job limit at the rates above.

## Not covered

- 1M rows was measured on two routes only (above); MariaDB → PostgreSQL at
  1M was not run. Nothing above 1M was run locally; the dispatch workflow is
  the place for 10M / 100M.
- SQL Server, Oracle, Snowflake and BigQuery routes are not in the harness.
- Parallel table workers: a multi-table job runs its tables in parallel where
  the engine already did; nothing here changed that.

## Verified against

- `apps/api/benchmarks/db_throughput.py`, `apps/api/benchmarks/throughput_smoke.py`,
  `apps/api/benchmarks/throughput_floors.json`
- `apps/api/services/cell_fastpath.py`, `apps/api/connectors/batch_salvage.py`,
  `apps/api/src/transfer/copy_route.py`
- `apps/api/tests/test_cell_fastpath_equivalence.py`, `apps/api/tests/test_batch_salvage.py`,
  `apps/api/tests/test_kill_resume_live.py`, `apps/api/tests/test_pg_jsonb_identity_copy.py`,
  `apps/api/tests/test_db_throughput_harness.py`
- `.github/workflows/ci.yml` (`throughput-smoke`), `.github/workflows/throughput-benchmark.yml`
