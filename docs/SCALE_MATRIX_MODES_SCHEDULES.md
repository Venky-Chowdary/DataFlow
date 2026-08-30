# Track D — advanced sync modes and schedules at scale

Live-engine evidence for CDC, the keyed batch modes, and the scheduler. Every
destination number below comes from an **independent driver connection** doing
`COUNT(*)` (or the store's equivalent) plus a content checksum over the mapped
projection — never from the writer's acknowledgement. Transfers run through
`src.transfer.engine.UniversalTransferEngine.execute_tracked`; there is no bypass
path in the harness.

## Re-running

```bash
cd apps/api
DATAFLOW_SCALE_MODES=1 python -m tests.scale.run_matrix            # everything
DATAFLOW_SCALE_MODES=1 python -m tests.scale.run_matrix cdc        # one suite
DATAFLOW_SCALE_MODES=1 python -m tests.scale.run_matrix crash batch scheduler
```

Without `DATAFLOW_SCALE_MODES=1` the entry point prints a skip line and exits 0,
so CI without the docker fleet does not fail. Knobs:
`DATAFLOW_SCALE_ROWS` (default `100000`), `DATAFLOW_SCALE_CHANGE_ROWS`
(default `2000`), `DATAFLOW_SCALE_JOB_WAIT` (scheduler firing wait, default 900s).
Crash injection is scripted: the harness forks `tests/scale/crash_child.py`,
watches the destination, and `SIGKILL`s the child once it has observed rows —
nothing is killed by hand.

Fleet used: `docker compose --profile amd64-sql up -d` — PostgreSQL 16
(`wal_level=logical`), MySQL 8 (ROW binlog + GTID), MongoDB 7 replica set `rs0`,
Redis 7.

## CDC — 100,000 rows, then 2,000 changed rows

Snapshot is 100,000 rows; the DML window then applies 2,000 inserts, 1,000
updates and 1,000 hard deletes and is drained until two consecutive quiet
destination windows (`poll_windows_to_drain` is recorded per cell).

| Route | Capture | Snapshot→log handoff | INSERT/UPDATE/DELETE | Idle re-run (no dup) | Rows | Rows/s (snapshot) | Delete capture | Delivery |
|---|---|---|---|---|---|---|---|---|
| postgresql→postgresql | pgoutput logical slot | pass | pass | pass | 100000 → 101000 | 1667 | yes | at-least-once |
| postgresql→mysql | pgoutput logical slot | pass | pass | pass | 100000 → 101000 | 1674 | yes | at-least-once |
| mysql→postgresql | binlog ROW + GTID | pass | pass | pass | 100000 → 101000 | 1436–1674 | yes | at-least-once |
| mongodb→postgresql | change stream + pre-images | pass | pass (3K rerun) | pass (3K rerun) | 100000 snapshot | 202 | yes | at-least-once |
| postgresql→mongodb | pgoutput logical slot | pass (3K rerun) | pass (3K rerun) | pass (3K rerun) | 3000 → 4000 | 598 | yes | at-least-once |
| mysql→mysql | binlog ROW + GTID | pass (3K rerun) | pass (3K rerun) | pass (3K rerun) | 3000 → 4000 | 1076 | yes | at-least-once |

The three "3K rerun" routes are the routes whose defects were fixed last (see
below). They are proven end-to-end on the live engines at 3,000 rows; the 100K
re-measurement for those routes is still running at the time of writing and this
table will be updated with the measured numbers rather than assumed ones. Do not
read the 3K rows as a 100K claim.

Every passing cell records the persisted watermark
(`slot=…|phase=streaming|lsn=0/…` for PostgreSQL, `file:pos` + GTID for MySQL,
resume token for MongoDB), the lag fields the product exposes
(`cdc_lag_seconds`, `cdc_lag_basis`, `replication_lag_bytes`,
`cdc_heartbeat_age_sec`) and `watermark_advanced`.

### Semantics claim

Every route is claimed **at-least-once upsert**, including the ones where the
crash-resume destination count and checksum matched exactly. A matching count
after one crash is not proof of exactly-once: the same change can still be
redelivered, and the destination is only correct because the apply is an
idempotent keyed upsert. No route claims exactly-once.

## Crash injection mid-stream and resume — 20,000 rows

| Route | Result | Source | Destination (independent) | Rejected | Quarantined | Rows/s |
|---|---|---|---|---|---|---|
| postgresql→postgresql | pass | 20000 | 20000 | 0 | 0 | ~1527 |
| mysql→postgresql | pass | 20000 | 20000 | 0 | 0 | ~1436 |
| mongodb→postgresql | pass | 20000 | 20000 | 0 | 0 | ~202 |

The child is `SIGKILL`ed after the destination has taken ≥500 rows, so the kill
lands mid-stream, not between runs. Resume happens from the persisted cursor; no
loss and no double-apply were observed (no duplicate keys, checksums matched).
The 100K crash pass is not yet measured.

## Defects found and fixed (root cause, canonical owner)

1. **MongoDB hard deletes were silently dropped when the pipeline is keyed on a
   business key.** A change-stream delete event carries only
   `documentKey._id`; with `primary_key = "id"` the reader could not name the
   deleted row and dropped the event, leaving stale rows at the destination
   (measured: `deleted keys still at destination=1000`). Owner:
   `apps/api/connectors/mongodb_change_stream.py` — it now probes
   `changeStreamPreAndPostImages`, requests
   `fullDocumentBeforeChange=whenAvailable`, takes the business key from the
   pre-image, and when pre-images are off it **fails closed** with an actionable
   remediation (`services/cdc_capability.mongo_delete_key_refusal`) instead of
   discarding the delete. Silently degrading to a cursor poll — which can never
   see a hard delete — was not an option.
2. **MySQL→MySQL CDC deadlocked itself:
   `(1205, 'Lock wait timeout exceeded')`.** The binlog snapshot held
   `FLUSH TABLES WITH READ LOCK` for the whole dump, which freezes every write on
   the instance — including this pipeline's own destination when it lives on the
   same server. Owner: `apps/api/connectors/mysql_change_stream.py`, now
   Debezium-class *minimal* locking: hold the global lock only to open
   `START TRANSACTION WITH CONSISTENT SNAPSHOT` and read the binlog
   coordinates, release it, then dump inside that read view. Still gap-free,
   and writers are no longer blocked.
3. **PostgreSQL LSNs were mis-ordered by a MySQL destination.** A WAL value like
   `0/14F23958` was compared as a MySQL `file:pos`. Owner:
   `apps/api/connectors/lsn_guards.py` — one family classifier for WAL hi/lo,
   binlog `file:pos`, GTID sets, Oracle SCNs, MongoDB resume tokens, SQL Server
   LSNs, numeric versions and opaque values. Equal is not newer, and
   cross-family values are incomparable rather than coerced into a bogus order.
   The same edit removed SQL `%` wildcards that broke DB-API interpolation.
4. **MongoDB numeric precision was inferred from a sample, so real values were
   quarantined.** A 3,000-row probe landed 999 rows with 2,001 quarantined
   against an undersized decimal. Owner: BSON carrier evidence is now preserved
   before stringification and the BSON numeric type is authoritative
   (`services/data_profiler.py`, `services/schema_introspect.py`); the same
   probe then measured 3000/3000 with 0 rejected, 0 coerced-null, 0 skipped.
5. **A killed CDC consumer left a lease that blocked every later run.** Owner:
   `apps/api/services/cdc_lease.py` / `cdc_lease_store.py` — leases can be
   enumerated, and cleanup releases only the leases the route owns (an operator
   `force_release_lease` exists for the rest). Nothing releases an unrelated
   lease.
6. **Cross-workspace schedule read.** A user who belongs to two workspaces could
   read a sibling workspace's schedule while declaring a different workspace in
   `X-Workspace-Id`. Owner: `apps/api/services/workspace_access.py` —
   `assert_resource_workspace` now enforces the declared scope, and an
   unattributed resource is 404 under workspace isolation.

## Scheduler

Measured through the real FastAPI app (`TestClient` against the connector and
schedule routers) and the real runner (`services.schedule_runner`), with the
destination read back on an independent PostgreSQL connection:
`pass 10, fail 2` before the fixes below.

Passing: schedule creation + persisted state, interval next-run, ordinary cron
next-run, actual firing at the scheduled time, sync-mode preservation on the
scheduled run, proof artifact produced for the scheduled run, failure surfacing,
retry/backoff, no duplicate execution when two **separate processes** race the
same schedule, and refusal to overlap when the previous run is still in flight.

Failing at that point, both since changed and **not yet re-measured**:
- *DST boundary* — the independent `zoneinfo` expectation and the product
  disagreed around a spring-forward wall time that does not exist. The harness
  now checks the three cases separately (ordinary offset change, nonexistent
  spring-forward local time, repeated fall-back hour with `fold=0`) instead of
  asserting one number.
- *Workspace ownership* — the real defect in item 6 above; fixed in the guard.

## Not proven yet

- 100K re-measurement of `postgresql→mongodb`, `mysql→mysql` and the MongoDB DML
  window (running; 3K is what is measured today).
- Crash injection at 100K (20K is what is measured).
- The batch suite (`incremental_deduped` three-run idempotency, composite keys,
  late-arriving updates, NULL-in-key, SCD2 closure/no-churn, mirror delete
  propagation, reverse ETL) is committed and runnable but its 100K numbers are
  not in this document yet, so no claim is made for it here.
- Scheduler DST and workspace cells after the fixes.
- SQL Server: not exercised in this track.
