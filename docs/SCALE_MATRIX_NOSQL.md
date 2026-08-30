# Track C — NoSQL, analytics and emulator engines: measured scale matrix

Every number in this document was produced by
`apps/api/tests/scale/nosql_matrix.py` against live engines on the desktop lab
fleet (`docker compose --profile amd64-sql up -d`), running the product path
`src.transfer.engine.UniversalTransferEngine.execute_tracked`. No mocked
writer, no bypass, no extrapolation. Where a number is not yet measured this
document says so instead of estimating it.

## How to re-run

```bash
cd apps/api
DATAFLOW_SCALE_NOSQL=1 PYTHONPATH=. .venv/bin/python -m tests.scale.nosql_matrix \
    --rows 100000 --out /tmp/track_c.json
# narrower: --engines mongodb,redis,dynamodb,duckdb,bigquery_emulator
#           --modes full_refresh_overwrite,upsert
```

Without `DATAFLOW_SCALE_NOSQL=1` the harness exits immediately, so CI without
the fleet skips instead of failing. Every engine that is not reachable is
recorded as `skip` with the driver's own error string.

## What a cell proves

Each cell runs the same sync mode **twice** and then measures the destination
with that store's own client — never the writer's acknowledgement:

| Evidence | How it is obtained |
| --- | --- |
| `dest_count` | independent driver count (`COUNT(*)`, `SCAN`+`TYPE`, `Scan`/`Select`, `count_documents`) compared to `sync_mode_probe.expected_rows_scaled` for the mode **and the destination's addressing** |
| `dest_checksum` | additive checksum over the mapped projection re-read from the destination, compared to the fixture checksum × the number of copies the mode should have landed |
| `temporal` | per-row naive/zoned landing verdicts, reported separately because an instant-only carrier (BSON date) cannot hold a zoneless wall clock |
| row accounting | engine `rejected` / `quarantined` / `coerced_null` / `skipped` so a short landing says where the rows went |

A cell is `pass` only when the transfer succeeded **and** the independent count
equals the expected count **and** the independent checksum matches. Anything
else is `fail` with the measured numbers, or `skip (exact reason)`.

### Key-addressed classification

`full_refresh_append` lands `2N` in a row-addressed store and `N` in a
key-addressed one when the same keys are rewritten. The harness takes the
classification from the canonical owner, `services.primary_key`
(`KEY_ADDRESSED_DESTS`), not from a local list:

| Store | Addressing | Why |
| --- | --- | --- |
| Redis | key-addressed | the write target *is* the key (`HSET <prefix>:<id>`); a second write to the same id replaces the hash |
| DynamoDB | key-addressed | `PutItem` on the same partition key replaces the item |
| MongoDB | key-addressed | writes are `_id`-addressed upserts, not blind inserts |
| Elasticsearch | key-addressed | `_id`-addressed index requests |
| PostgreSQL / MySQL / DuckDB / BigQuery | row-addressed | `INSERT` appends a new row for the same key, so append must double |

## Fixture

`apps/api/tests/scale/nosql_fixture.py` is one deterministic generator used for
every route, so a checksum is comparable across engines. It carries nested
documents, scalar arrays, arrays of objects, heterogeneous field types across
documents, missing and extra fields, deep nesting, UUID/ObjectId-shaped and
unicode keys, `Decimal128`-class decimals, integers beyond `2^53`
(`9007199254740994`), and both naive and UTC-zoned timestamps. Flattening is
declared explicitly (`flatten_top_level_keys`) and the naive timestamp is only
allowed to land on an instant carrier when the Map step declares
`assume_timezone`, so nothing is silently reinterpreted.

## Results

### 200-row shakeout (complete, base revision `3e44f94e`)

Artifact: `/tmp/track_c_200.json` — **121 cells: 73 pass, 24 fail, 24 skip.**
This is the run that found the defects listed below; it is recorded because it
is the last *complete* sweep, not because it is the current state. The failures
it recorded were then root-caused and fixed, and the affected cells were re-run
individually (see "Defects found and fixed").

| Route | overwrite | append | incremental | upsert |
| --- | --- | --- | --- | --- |
| postgresql→mongodb | fail | pass | pass | pass |
| mongodb→postgresql | pass | pass | pass | pass |
| mysql→mongodb | pass | pass | pass | pass |
| mongodb→mysql | pass | pass | pass | pass |
| mongodb→mongodb | pass | pass | pass | pass |
| postgresql→redis | fail | pass | pass | pass |
| redis→postgresql | pass | pass | pass | pass |
| mysql→redis | pass | pass | pass | pass |
| redis→mysql | fail | fail | fail | fail |
| redis→redis | pass | pass | pass | pass |
| postgresql→dynamodb | fail | pass | pass | pass |
| dynamodb→postgresql | pass | pass | pass | pass |
| mysql→dynamodb | pass | pass | pass | pass |
| dynamodb→mysql | fail | fail | fail | fail |
| dynamodb→dynamodb | pass | pass | pass | pass |
| postgresql→duckdb | fail | pass | pass | pass |
| duckdb→postgresql | pass | pass | pass | pass |
| mysql→duckdb | pass | pass | pass | pass |
| duckdb→mysql | pass | pass | pass | pass |
| duckdb→duckdb | pass | pass | pass | pass |
| postgresql→bigquery_emulator | fail | fail | fail | fail |
| mysql→bigquery_emulator | fail | fail | fail | fail |
| postgresql→{mongodb,redis,dynamodb}→postgresql | fail | — | — | — |
| postgresql→duckdb→postgresql | pass | — | — | — |
| postgresql/mysql→elasticsearch | skip | skip | skip | skip |
| postgresql/mysql→clickhouse | skip | skip | skip | skip |
| postgresql/mysql→iceberg | skip | skip | skip | skip |

### After the fixes (individually re-run, 200 rows)

Artifact: `/tmp/track_c_fix.json` — **53 cells: 44 pass, 9 fail** (the nine were
the BigQuery-emulator cells, root-caused afterwards, see D6/D7). Re-measured
after-numbers for the specific defect cells are quoted inline below.

### 100,000 rows

**Not yet measured.** The full-fleet 200-row sweep on the fixed revision and
the 100K sweep were still running when this document was committed. This
section will carry the measured 100K numbers — count, checksum, elapsed,
rows/sec per cell — and until it does, no 100K claim exists for this track.

## Skips, with the exact reason

| Engine | Status | Reason (verbatim from the probe) |
| --- | --- | --- |
| Elasticsearch | skip | `elasticsearch unavailable: HTTP 400 on _has_privileges: {"error":"no handler found for uri [/_security/user/_has_privileges] and method [POST]"}` — the container that starts on this box does not expose the security API the connector's privilege preflight requires, so index privileges cannot be established and the connector fails closed |
| ClickHouse | skip | `clickhouse unavailable: connector status=live for driver=generic_sql — the engine refuses a production transfer on a non-certified connector` — capability is not `TRANSFER_READY`; the refusal is the product behaving correctly, not an environment failure |
| Iceberg | skip | `iceberg unavailable: no Iceberg REST catalog in docker-compose.yml (MinIO warehouse only) and no DATAFLOW_ICEBERG_REST_URI configured` |
| BigQuery (hosted) | skip | no credentials on this box — only the emulator (`localhost:9050`) was exercised |

## Defects found and fixed

Each entry names the **one canonical owner** that changed. No `*_v2` module was
added, no writer rejection was weakened, no test was edited to go green.

**D1 — key-addressed append reported silent data loss.**
`postgresql→redis` `full_refresh_append` failed with a row-conservation
violation because accounting compared a destination *count delta* against rows
written. In a keyspace keyed by id the second run rewrites the same keys, so
the delta is zero while nothing was lost. Owner:
`services/row_conservation.py` (with the key census in
`services/dest_precount.py`). After: `dest_count=200`, expected 200, checksum
match.

**D2 — whole-keyspace sources replayed the entire population on
`incremental_append`.** Redis and DynamoDB have no server-side cursor
predicate, so the reader returned every key and the second run landed `2N` into
a row-addressed destination. Owner: `services/sync_cursor.py` — the cursor is
now applied client-side for whole-keyspace sources. After:
`redis→postgresql` and `dynamodb→postgresql` `incremental_append` land `N`.

**D3 — exact large integers were canonicalized through IEEE float.**
`dynamodb→postgresql` reconciliation reported `9007199254740994` as
`9007199254740990`; direct inspection proved the exact value was in the store,
so this was a false loss report in the comparator. Owner:
`services/reconciliation.py` — integer-valued `Decimal` stays exact.

**D4 — Redis integers wider than `INT32` were rejected on write.**
`9007199254740994` was graded against `INTEGER` because the writer did not
thread its own dialect into integer wire coercion. Owner:
`connectors/redis_writer.py` (dialect passed to the canonical coercion).

**D5 — Redis/DynamoDB as *source* produced false decimal-width failures into
SQL.** Profiled source types were not authoritative on the schemaless side, so
the mapping graded a proven-width decimal against an invented narrow carrier.
Owner: `services/data_profiler.py` (profiler precedence).

**D6 — BigQuery quarantined every temporal value on the first run.** The
temporal quarantine ran with an unknown dialect and graded BigQuery as
timezone-less, so zoned values were quarantined; then the materializer stripped
the zone before BigQuery's JSON formatter. Owners:
`connectors/bigquery_writer.py` (concrete `dest_db`) and
`connectors/sql_temporal.py` (a bare `TIMESTAMP` that is an *instant* carrier
on this dialect refuses a naive wall clock instead of inventing UTC, and keeps
aware values aware).

**D7 — every second run into a BigQuery table this product had just created
failed closed.** Destination introspection translated BigQuery's physical
carriers into DataFlow's neutral vocabulary (`DATETIME` → `TIMESTAMP_NTZ`,
`BIGNUMERIC(24,6)` → bare `BIGNUMERIC`), and the second run then graded those
foreign spellings against BigQuery's own rules — reporting
`Lossy / fidelity collapse` and `narrow_type — sync paused for review` on a
table that had accepted the identical population minutes earlier. Two owners:

* `services/schema_introspect.py` keeps the catalog's physical carrier
  (`_bq_field_physical`) alongside the logical translation, and
  `src/transfer/adapters_introspect.py` restores it for the destination role;
* `services/type_system.py` gains `decimal_capacity_is_equal_or_wider`, and
  `services/schema_drift.py` uses it so a *wider* fixed-point sink is not drift
  — the invented shape (bare `BIGNUMERIC` is `76,38`) stays an
  operator-visible invention/fidelity chip, which is a different verdict from
  "this loses digits".

After D6+D7 a two-run BigQuery-emulator probe passes both runs (previously run
1 passed and run 2 blocked); the full BigQuery cell sweep on the fixed revision
is part of the pending measurement above.

**D8 — MongoDB repeated-run DDL identity divergence.** Run 2 re-derived
mapping carriers from sampled destination values and diverged from the stamped
approval (`DDL identity mismatch`). Owner: the Decision Kernel type path
(`services/decision_kernel/`), after which a 200-row MongoDB sweep reported
`21 pass, 0 fail, 0 skip`.

**D9 — `mongosh` replica-set bootstrap never ran.** The compose init used `#`
comments inside JavaScript, so `mongosh` raised `SyntaxError` and `rs0` was
never initiated. Fixed in `docker-compose.yml`.

## Still unproven

* 100K-row measurements for every route (running; see above).
* BigQuery-emulator cells beyond the two-run identity probe.
* Elasticsearch/OpenSearch, ClickHouse and Iceberg remain skips for the reasons
  quoted above — none of them is a green.
* MongoDB contract persistence logs a suppressed
  `bson.errors.InvalidDocument: cannot encode object: Decimal('1')` when the
  mapping contract document is stored; the store falls back, so no transfer
  result depends on it, but it is not fixed.
