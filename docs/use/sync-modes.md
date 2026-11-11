# Sync modes

A sync mode decides two things: how much of the source is read, and what
happens to rows already in the destination. This page lists the modes the
engine acts on, what each guarantees, and where each stops.

**Always state the sync mode.** The defaults differ by entry point: the engine
treats a missing mode as append, while the JSON route
`POST /api/v1/transfer/execute` defaults to overwrite.

## The modes

| Mode | Reads | Writes | Needs a key | Needs a cursor column |
|---|---|---|---|---|
| `full_refresh_overwrite` | Everything | Replaces the destination table's contents | no | no |
| `full_refresh_append` | Everything | Adds rows; existing rows stay | no | no |
| `upsert` | Everything | Inserts new keys, updates existing keys; rows only in the destination stay | yes | no |
| `mirror` | Everything | Upsert, plus deletes of rows the source no longer has | yes | no |
| `incremental_append` | Rows at or after the saved cursor | Adds rows | no | yes |
| `incremental_deduped` | Rows at or after the saved cursor | Upserts by key | yes | yes |
| `scd2` | Everything | Keeps every version of a row | yes | no |
| `cdc` | The source's change log | Applies inserts, updates and deletes by key | yes | yes (the log position) |
| `reverse_etl` | Everything | Writes to an application destination | see Not covered | |

Other spellings are accepted and mapped onto these. The ones worth knowing:

| You write | You get |
|---|---|
| `append`, `insert`, `full_append` | `full_refresh_append` |
| `overwrite`, `replace`, `truncate` | `full_refresh_overwrite` |
| `incremental` | `incremental_append` (not a dedupe) |
| `incremental_upsert`, `dedupe`, `deduped` | `incremental_deduped` |
| `merge` | `upsert` |
| `cdc_incremental` | `cdc` |
| `full_refresh_mirror` | `mirror` |
| `scd_2`, `slowly_changing_dimension` | `scd2` |

Nothing maps onto `mirror` unless it says mirror, because mirror deletes.

`POST /api/v1/transfer/execute` and the schedule routes reject an unknown mode
name. Where a name reaches the engine unvalidated, it is logged and does not
trigger incremental reads or upsert writes.

## What every mode has in common

- **Delivery is at-least-once.** A row may be written more than once after a
  retry or resume. Keyed modes absorb this because the second write updates
  the same row. Append modes can produce duplicates. Nothing in the product
  claims exactly-once delivery.
- **Rows are never dropped silently.** A row that cannot be written is
  quarantined on the job, and the job finishes as
  `completed_with_quarantine`, not `completed`.
- **Validate checks the mode against the route** before any write. A mode that
  needs a key or a cursor is refused when the stream does not name one.

## Full refresh

`full_refresh_overwrite` replaces the table. `full_refresh_append` adds to it.

Append and continuity: Gate-8 verifies the rows a run wrote, so rows from
earlier runs are outside its scope. To cover that gap, after each successful
run the engine stores a fingerprint of the whole destination table. Before the
next append writes anything, the destination is read again and must match.
A mismatch fails the run before any write. If the destination was changed on
purpose, accept the current state as the new baseline
(`POST /api/v1/transfer/continuity/rebaseline`).

## Upsert

Reads the whole source and writes by key. Rows that exist only in the
destination are left alone.

How the key is enforced depends on the destination:

- PostgreSQL and MySQL use their native upsert, which needs a unique index on
  the key. If the table has none, Validate discloses that an index will be
  created.
- SQL Server, Oracle, Snowflake, BigQuery and others match the key in a
  `MERGE` (or a keyed delete and insert) and create no index.
- Duplicate keys already in the destination block the run, because the match
  would be ambiguous.

For a file source, upsert re-reads the file and matches on the business key.
No cursor column is needed.

## Mirror

Mirror is upsert plus deletes: destination rows that are no longer in the
source are removed.

- By default the delete is **soft**. The row is kept, `_deleted` is set and
  `_deleted_at` is stamped. A stream can declare `delete_mode="hard"` to
  delete the row instead.
- A row that reappears in the source is reactivated.
- `_deleted` belongs to the destination. The writer never sets it on
  ordinary rows, so a deleted row cannot be quietly revived by a later write.
- Row counts in the destination do not fall after a soft delete. Count active
  rows by filtering on `_deleted`.

Mirror is destructive. Use it only when the source is complete at every run.
A partial source read would delete rows.

## Incremental

Incremental modes read rows at or after a saved cursor value, then store the
new high value.

- The read includes rows equal to the saved value, so a group of rows sharing
  one cursor value is read again in full. `incremental_append` can therefore
  write those rows twice. `incremental_deduped` handles this by key.
- Incremental reads **cannot see deletes** in the source. Use mirror or CDC
  when deletes matter.
- A row that was changed without its cursor value moving forward is missed.
- On the fast copy path between PostgreSQL, MySQL and SQLite, the cursor types
  treated as safe are date, naive datetime or timestamp, text and integer.
  Time-zone-aware timestamps, booleans, floats, JSON and time-of-day columns
  are treated as unsafe cursors there.
- If the destination table was dropped or changed outside the pipeline, the
  saved cursor no longer matches it. Clear the cursor so the next run reads
  from the start.
- The SDK-based SaaS sources declare that they do not read incrementally. See
  [../reference/connectors.md](../reference/connectors.md).

## SCD2

Keeps the full history of each key. When a non-key attribute changes, the
current version is closed (`valid_to` set, `is_current` false) and a new
current version is inserted. Running the same source snapshot again adds no
rows.

SCD2 is for SQL destinations. It is not exactly-once.

## CDC

CDC reads the source's change log instead of its tables, so it carries
inserts, updates and deletes.

- **Sources**: PostgreSQL (logical decoding), MySQL (binlog), MongoDB (change
  streams), Oracle (LogMiner) and SQL Server (CDC or change tracking). Validate
  refuses CDC for any other source.
- **Destinations**: not object stores or file exports. They have no log
  position to resume from.
- **Guarantee**: at-least-once, applied as keyed upserts. Where the
  destination keeps the log position on each row, a redelivered older change
  cannot overwrite a newer one. That protects row state on keyed
  destinations. It is not exactly-once delivery, and it does not apply to
  append-only destinations.
- **Source setup** is required: for example `wal_level=logical` and
  replication privileges on PostgreSQL, row-format binlog on MySQL.
- **If log capture cannot attach** and the server is configured to emit a log
  (slot quota reached, privilege missing), the run fails with the remedy. It
  does not fall back to polling, because polling would stop carrying deletes
  without saying so. If the server was never configured to emit a log, the
  run may fall back to query-based capture with the loss declared.
- **Schema changes** on the source can pause CDC for review. See
  [../operate/runbooks.md](../operate/runbooks.md).
- A MongoDB delete names only the document id. When the pipeline's key is a
  business key, the collection needs pre-images enabled, or the delete cannot
  be applied and the run stops.
- One job at a time reads a given replication slot, binlog position or
  capture instance.

## Append-only destinations

Some destinations can only add rows (Kafka topics are the main case). On
those, only append modes are accepted. Overwrite, upsert, deduped, mirror and
SCD2 are refused at Validate, because they would run as a plain append and
publish duplicates that cannot be removed.

## Not covered

- `reverse_etl`: the mode exists and is used for application destinations
  such as Salesforce and HubSpot. Its guarantees were not traced for this page.
- Throughput and how each mode behaves at large volume.
- Exactly which destinations store the log position per row.
- Transform steps, which are separate from the sync mode.

## Verified against

- `apps/api/services/sync_cursor.py` (canonical modes, aliases, default)
- `apps/api/src/routers/transfer_router.py` (`ExecuteTransferRequest` default)
- `apps/api/services/preflight_cursor_gate.py`, `apps/api/services/preflight_transfer_policy.py`
- `apps/api/services/execution_engine_contract.py`, `apps/api/services/job_status.py`
- `apps/api/services/append_continuity.py`
- `apps/api/connectors/merge_registry.py`
- `apps/api/services/mirror_engine.py`
- `apps/api/services/copy_incremental.py`
- `apps/api/services/scd2_engine.py`
- `apps/api/services/cdc_capability.py`, `apps/api/services/cdc_effectively_once.py`,
  `apps/api/services/cdc_lease.py`
- `apps/api/services/connector_capability_registry.py`
