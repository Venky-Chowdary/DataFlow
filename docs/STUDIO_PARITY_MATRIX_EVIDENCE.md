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

## Existence must not depend on where a name sorts (fixed)

Browser testing found a live source table refused as absent. Measured against
the local PostgreSQL fixture (`dataflow`, schema `public`, 354 tables):

```
POST /transfer/introspect  table="public.vt_src"  →  table_exists=false, columns=[]
                                                    "not found"
```

The object listing is bounded (`listing first 200`), and that listing was also
what normalised `public.vt_src` to `vt_src`. Past the cap, the catalog was asked
for a table whose name literally contained a dot and answered no rows — so a
readable table became "not found", with Continue disabled, purely because of
where it sorted. A second defect hid behind it: the bounded sample SELECT was
handed the qualified string too and read `public.public.vt_src`.

Fixed with one owner for qualified names — `services.schema_introspect.split_object_namespace()`
— applied at the introspect entry, so catalog lookup, sample read and every
downstream step receive namespace and object apart. MySQL is namespaced by
database, not schema; MongoDB collection names may contain dots and are not
split. Measured after the fix:

| request | table_exists | columns | sample rows |
| --- | --- | --- | --- |
| pg `public.vt_src` | true | 5 | 6 |
| pg `vt_src` | true | 5 | 6 |
| pg `public.nope_x` | false | 0 | 0 |
| mysql `dataflow.bench_1m_proof` | true | 10 | 100 |
| mysql `bench_1m_proof` | true | 10 | 100 |
| mysql `dataflow.nope_y` | false | 0 | 0 |

The 60-case matrix was re-run after the change: **58 pass, 2 blocked
consistently, 0 parity breaks** — unchanged (`/home/ubuntu/parity_after_ns.json`).

### Why 354 tables existed at all

32 of them were our own leaked `_df_mirrorkeys_*` mirror key-staging tables — so
the product's own scratch was consuming the bounded page that pushed a real
table out of the listing. Three fixes:

* Staging names are stamped (`_df_mirrorkeys_<epoch>_<rand>`), and a mirror run
  sweeps stamped orphans older than 6h plus pre-stamp names under a bounded lock
  wait (2s), so a table a concurrent or older-build run holds is skipped, never
  waited on. A sweep failure never fails the mirror.
* An all-digit legacy suffix (`_df_mirrorkeys_255577532241`) previously parsed as
  an epoch stamp dated year 10069 — an orphan that could never age out. A stamp
  is only read as a clock between 2020-01-01 and now.
* Internal scratch prefixes are hidden from operator-facing object listings.
* A staging table that cannot be dropped is now logged as a warning instead of
  swallowed.

Regression: `apps/api/tests/test_qualified_object_existence.py` (12 cases).
