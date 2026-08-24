# Transfer Studio parity matrix — measured evidence

Harness: `apps/api/scripts/live_studio_parity_matrix.py`
Raw artifact: `/home/ubuntu/parity_full2.json`
Path exercised: the product's own Studio sequence per case — `/transfer/introspect`
→ Map (`/transfer/map`) → Validate (`/preflight`) → Run (`/transfer/execute`),
then an **independent** destination read for the row census.

The harness never declares the source shape itself: the source schema carried
into Map and Validate is the one `/transfer/introspect` returns, so a harness
cannot invent a type Studio never sees. Each case declares the contract it
expects, and a case whose measurement disagrees with its contract is reported as
`parity_break` — the class this matrix exists to catch: **Validate clears and Run
fails.**

Live services: PostgreSQL 5433, MySQL 3307, MongoDB 27017, local CSV/XLSX
fixtures, local file export. Snowflake / BigQuery / Redshift / Oracle /
SQL Server are **not** exercised here — an unreachable warehouse is reported as
skipped, never as green.

## Result — 60 cases

```
pass:                  58
blocked_consistently:   2
parity_break:           0
```

| Route | full_refresh_append | append (2nd run) | overwrite (2nd run) | incremental_append | incremental_deduped | mirror | scd2 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| postgresql→mysql | pass | blocked | pass | pass | pass | pass | pass |
| mysql→postgresql | pass | blocked | pass | pass | pass | pass | pass |
| postgresql→mongodb | pass | pass | pass | pass | pass | — | — |
| mongodb→postgresql | pass | pass | pass | pass | pass | pass | pass |
| mysql→mongodb | pass | pass | pass | pass | pass | — | — |
| csv→postgresql | pass | pass | pass | pass | pass | pass | pass |
| csv→mysql | pass | pass | pass | pass | pass | pass | pass |
| excel→postgresql | pass | pass | pass | pass | pass | pass | pass |
| mongodb→file_export | pass | pass | pass | pass | — | — | — |
| postgresql→file_export | pass | pass | pass | pass | — | — | — |

`—` = the mode is not offered for that destination class and is therefore not
run (mirror/SCD2 need a SQL destination that can hold history versions).

### The two blocked cases are the correct verdict, not a failure

`postgresql→mysql` and `mysql→postgresql`, `full_refresh_append` run a second
time into a destination that already holds those keys and **enforces** them:

```
Destination already stores these keys: 5 key value(s) in this batch are already
at rest in the destination on id, which enforces uniqueness — a
full_refresh_append insert aborts on the first one
```

Blocked at Validate, before any partial write. The equivalent MongoDB / file /
CSV destinations are not blocked because nothing there enforces the key, and the
declared contract for those is append duplication, which the census confirms.

## Defects this matrix found and fixed

### 1. MongoDB incremental read compared a datetime watermark to string cells

`mongodb→postgresql` `incremental_deduped` cleared Validate and then failed Run
with `Source table 'sp_src' has no columns or is empty` — a 200-row collection.
BSON orders values **by type first, then value**, so a `$gt` on a `datetime`
watermark can never match a field whose cells hold ISO strings: the page came
back with no rows and, because a schemaless source names its columns from the
documents it returns, no headers either.

Fixed in `connectors/mongodb_reader.py`: the watermark is aligned to the BSON
family the collection **actually stores** in the cursor field
(`stored_cursor_bson_kind`), a cursor field that mixes families is refused rather
than silently half-read, and `src/transfer/stream.py` no longer reads a zero-row
incremental page as an empty source when the schema is already known.

### 2. SCD2 read one scope in the buffered path and another in the streaming path

`csv→postgresql`, `csv→mysql` and `excel→postgresql` SCD2 cleared Validate and
failed Run on their own row-count proof:

```
Row count mismatch: source 1, rejected 0, skipped 0, removed by transform 0,
filtered out 0, expected target 1 vs target 200
```

The SCD2 write itself was correct — the destination held 201 rows, two versions
for the updated key, 200 current. The buffered path narrowed the read to the
cursor delta (1 changed row) and then reconciled that one row against the
destination's whole 200-row **current** population, while the streaming SQL path
snapshots the entire source into staging and therefore reconciled correctly.
Two read scopes for one declared mode.

Fixed in `services/batch_incremental.py`: SCD2 compares a whole source snapshot
against the destination's current versions — unchanged rows produce no new
version, so a full read is idempotent and the current census is exactly what
Gate-8 can prove. The cursor still advances (so cursor identity/reset refusals
still apply); it just no longer narrows the read. Regression:
`apps/api/tests/test_batch_incremental.py::test_scd2_reads_the_whole_snapshot_while_the_cursor_still_advances`.

## Not proven here

* Snowflake / BigQuery / Redshift / Oracle / SQL Server routes (no reachable
  instance in this environment).
* CDC across these routes (measured separately; default remains at-least-once).
* Per-cell fidelity beyond the mapped-projection digest for the file-export
  destinations.
* Throughput: only PostgreSQL→MySQL `full_refresh_append` at 1M rows is measured
  (`docs/THROUGHPUT_1M_EVIDENCE.md`). These cases are 200-row correctness cases.
