# Transformation layer audit — Pipelines → Transforms

Audited at the same bar as the transfer engine: every claim below is a live case
run through `TransformRunner` against a real engine, with the destination read
back cell by cell afterwards. Row counts were never accepted as proof, because
the defects this audit found all reconcile perfectly on counts.

- Harness: `repro/transform_live.py`
- After the fixes: `repro/transform_live_results.json` — **33 cases, 33 ok**
- Before the fixes: `repro/transform_live_base_results.json` — **33 cases, 18 not ok**
- Engines: PostgreSQL 16 (5433), MySQL 8 (3307), SQL Server 2022 (1433)
- Unit coverage: `apps/api/tests/test_transform_layer_wave96.py::TestIncrementalColumnAlignment`

Oracle is excluded by design, not by omission: it has no
`CREATE TABLE IF NOT EXISTS`, so an incremental model cannot be seeded
idempotently and the runner refuses it at build time rather than emitting SQL
that works on day one and fails on day two.

## Defect 1 — incremental loads bound columns by position (silent corruption)

`INSERT INTO mart SELECT * FROM (<model body>)` writes by ordinal. That is only
correct while the model's SELECT order equals the target's column order. The
target of an incremental model outlives the model definition, so the two diverge
routinely:

| Case | Before | After |
| --- | --- | --- |
| Target pre-created as `(id, city, region)`, model selects `id, region, city` | `city='EMEA'`, `region='Berlin'` — run **success** | values in their own columns |
| Target `(id, tenant, city)`, model omits `tenant` | PG shifted every value one column left (`tenant='Berlin'`); MySQL/SQL Server failed with a count mismatch | `tenant` left to the target, rest aligned |
| `delete+insert` onto a reordered target | swapped, run **success** | aligned |
| Model and target differ only in column case | swapped | aligned case-insensitively |

Nothing in the product could have caught this: the row count matched, the
checksum was computed over the same swapped rows, and the data tests were on the
columns that still lined up. It corrupts by writing plausible values into the
wrong column, which is worse than failing.

**Fix.** The executed load now names both column lists, matched by name:

```sql
INSERT INTO "mart" ("id", "region", "city")
SELECT "id", "region", "city" FROM (<model body>) AS _df_new
```

Both sides are read for real — the model's columns from a zero-row execution of
its own body, the target's from the destination catalog — and the resolved
mapping is reported per model as `column_alignment`. Anything that cannot be
matched stops the load *before it writes*, naming the column:

- a column the model produces that the target does not have;
- a target column that is `NOT NULL`, has no default, no identity and no
  computed expression, and that the model does not produce;
- a declared `unique_key` missing from the model's output or from the target
  (the `DELETE` would match nothing, so `delete+insert` would duplicate).

Fail-closed did not become fail-always: a `NOT NULL` column with a default, an
identity or a computed expression still loads, proven as its own case on all
three engines.

## Defect 2 — the first run of an append model loaded every row twice

The seed was `CREATE TABLE IF NOT EXISTS mart AS <body>`, which materialized the
rows, and the `INSERT` that followed wrote the same batch again. Only the very
first run of a brand-new model was affected, so every later run looked correct
and the duplication survived as history. Measured before the fix: 4 source rows
became 8 on PostgreSQL and MySQL.

**Fix.** The seed creates the shape only (`WHERE 1 = 0`) on every dialect, which
is what the SQL Server `SELECT … INTO` path already did, so exactly one
statement loads rows per run. Measured after: 4 after the first run, 8 after the
second (append is at-least-once by design and says so).

## Confirmed correct, and now covered

- `table` materialization rebuilds and owns its column order.
- `delete+insert` is idempotent on the unique key across re-runs.
- `append` is at-least-once and declares it in the reported strategy
  (`append (at-least-once; re-runs duplicate rows)`), rather than implying
  idempotency it does not have.
- A refusal writes zero rows — asserted, not assumed, in every refusal case.
- An undefined `ref()` is a plan-time failure, so nothing runs; the dead
  "warnings" branch that suggested otherwise was removed.
- Downstream models are skipped, naming the failed upstream, rather than built
  on a stale or absent relation.
- A data test naming a column the model does not produce fails instead of
  reporting green.
- Unsupported dialect/materialization combinations are refused at build time.

## Not proven yet

- Snowflake, BigQuery, Databricks, Trino, Vertica, ClickHouse and Redshift are
  in the dialect capability table but have no live run here — no credentials.
  Their statements are unit-covered only.
- Transform runs have no row ledger of their own: `rows_affected` is the last
  driver rowcount, not a `read/written/quarantined` account, and there is no
  quarantine path for rows a transform rejects. A transform failure leaves the
  landed data intact and is surfaced, but it is not replayable per row.
- Type fidelity *through* a transform (precision, timezone, JSON) is inherited
  from the destination's own CTAS inference and has not been matrixed.
- Concurrent runs of the same project against one target are not fenced.
