# Runbooks

Short procedures for the situations operators meet most often. Each one says
what the system does on its own, what you can do, and what it will not do.

Paths are under `/api/v1`. The permission each route needs is in brackets.

## Job states

| State | Meaning |
|---|---|
| `pending`, `queued`, `running`, `retrying` | A worker owns the job. |
| `paused` | Stopped by an operator. Never resumed automatically. |
| `completed` | Finished; data is in the destination. |
| `completed_with_quarantine` | Finished, and some rows were rejected or some values were set to NULL. Not a clean success. |
| `completed_with_warnings` | Finished but wrote nothing, for example because today's file has not arrived. |
| `failed`, `cancelled` | Finished without success. |

Final states do not change again. A cancelled job is never rewritten to
completed, even if a fast copy finishes after the cancel. Cancelling updates
the job's status; it does not promise that a statement already running on the
destination is interrupted.

## A job looks stuck

Two background checks run in the API process.

**Stall watchdog.** It watches the current step. When the step is quiet for
longer than its threshold the job is marked `stalled`, with `stall_seconds`
and `stall_step`. At three times the threshold the job is failed with the
finding `step_stalled`.

| Step | Quiet for |
|---|---|
| connect, create table, DDL | 120 s |
| preflight | 300 s |
| reconcile, verify, upload | 600 s |
| extract, load, write, anything else | 900 s |

Change one step with `DATAWRAP_JOB_STEP_STALL_<STEP>_SEC`, or the fallback with
`DATAWRAP_JOB_STEP_STALL_SEC`.

What to do:

1. Open the job and read the step it is on and for how long.
2. If it is waiting on the destination, check for a lock held by another
   session (see "Lock timeouts" below).
3. Cancel it: `POST /connectors/jobs/{job_id}/cancel` [connector.write].
4. After it reaches a final state, resume from the last committed checkpoint
   with `POST /connectors/jobs/{job_id}/resume`, or start again from zero with
   `.../retry`.

A job failed for a stall may still have a worker holding the destination
write. The failure message says so. Check the destination before re-running
into the same table.

## A worker died (orphaned jobs)

A job is an orphan when all of these are true: its state is one a worker owns;
no worker holds its lease; this process is not running it; no queue entry is
waiting for it; and its record has not been written for the grace period
(`DATAWRAP_JOB_ORPHAN_GRACE_SEC`, default the lease time of 60 s).

The orphan sweeper then does one of these:

- resumes it from its checkpoint, at most `DATAWRAP_JOB_ORPHAN_MAX_RECLAIMS`
  times (default 2);
- records it cancelled, if a cancel had been requested;
- records it failed, if there is nothing to resume from, the uploaded source
  file is gone, or the reclaims are used up.

Rows the dead worker already committed are not removed, and the message says
so. Two replicas sweeping at once cannot both settle the same job.

Workers have no graceful shutdown. Stopping a worker mid-run leaves its job to
this sweep. Expect a delay of at least the lease time before it moves.

To stop the API from re-queuing orphans at start, set
`DATAWRAP_API_ORPHAN_RESUME=0`.

## Resume is refused

Resume needs a durable progress marker. It is refused when there is none, when
the checkpoint is older than `DATAFLOW_RESUME_MAX_AGE_HOURS` (default 168), or
when the write mode has changed since the checkpoint. Use retry instead.
Delivery on resume is at-least-once: rows may be written again and are
reconciled by key where the route has one.

## Lock timeouts

- A write session waits at most `DATAWRAP_DEST_LOCK_WAIT_SECONDS` (default
  120, maximum 600) for a destination lock, then fails. Find and end the
  blocking session on the destination, then resume or retry.
- With `DATAWRAP_DEST_WRITE_LOCK=1`, only one job at a time writes to a keyed
  destination table. A second job waits up to
  `DATAWRAP_DEST_WRITE_LOCK_WAIT_SECONDS` (default 3600) and then fails as
  busy. This lock is off by default.
- CDC sources are protected by a lease so two jobs never read the same
  replication slot, binlog position or capture instance. A conflict fails the
  second job at once. See the next section.

## CDC is behind or will not start

Look first:

- `GET /ops/freshness` [job.read] shows lag per pipeline.
- `GET /ops/cdc-leases` [workspace.manage] shows who holds each source.
- `GET /ops/cdc-cursors` [workspace.manage] shows saved positions.
- `GET /ops/cdc-captures` [workspace.manage] lists the replication slots and
  publications Datawrap created on a PostgreSQL source.
- `GET /cdc/mapping-reviews` [job.read] lists tables whose schema changed
  while CDC was running.

Lag figures come from counters inside each process. On a deployment with
separate workers, read them from the job and the pipeline, not from the API's
`/metrics`.

Common causes and actions:

| Symptom | Action |
|---|---|
| "Lease conflict" on start | Another job holds the source. Cancel that job. If its process is gone, `POST /ops/cdc-leases/force-release`. This does not stop the old holder; it fails on its next renewal. |
| CDC paused for schema review | Check the mapping for the changed table, then `POST /cdc/mapping-reviews/{review_id}/acknowledge` [job.manage]. |
| Position is no longer available on the source (log was purged) | `POST /ops/cdc-cursors/clear` so the next run takes a fresh snapshot. |
| Replication slots left on a PostgreSQL source | `POST /ops/cdc-captures/cleanup`. Only objects your workspace recorded can be removed. |
| Log capture cannot attach (slot quota, missing privilege) | The run fails and names the remedy. It does not fall back to polling, because polling cannot see deletes. |

CDC delivery is at-least-once. Changes are applied as keyed upserts.

## Verification failed

After writing, the job compares source and destination (Gate-8; see
[../use/verification.md](../use/verification.md)). When it fails:

1. Open the job's Trust Report: `GET /jobs/{job_id}/parity-report` [job.read].
   It classifies each difference and proposes fixes.
2. Check what the failed run left in the destination. The job says which of
   these applied:
   - A full-refresh overwrite keeps the rows it wrote, so you can inspect
     them. The job is still failed and not verified. Re-running replaces the
     table. `DATAFLOW_DROP_ON_VALIDATION_FAILURE=1` restores the earlier
     behaviour of removing them.
   - An append into a destination that was empty before the run has its
     failed batch removed, returning the table to empty.
   - A destination that already held rows is left untouched.
   - CDC, SCD2 and mirror runs are never cleaned up this way.
3. Rejected rows are in the job's quarantine:
   `GET /connectors/jobs/{job_id}/quarantine`. Replay them after fixing the
   cause with `.../quarantine/replay`. `GET /ops/dlq` lists rejected rows
   across jobs.
4. Approve a proposed fix, or accept a difference with a note, in the Trust
   Report. See [../use/parity-studio.md](../use/parity-studio.md).

## Trust Report states

| Verdict | Meaning | Can it be signed? |
|---|---|---|
| `proven` | Every row was compared and none differs. | Yes |
| `explained` | Every row was compared; rows differ and every difference has a known cause. | Yes |
| `no_difference_found` | Values were compared on a sample only. Nothing unexplained was found. | Yes, when the sample was a random draw and the report states an upper bound. No, when the sample bounds nothing: run full verification first. |
| `unexplained` | At least one difference has no known cause. | No, until it is fixed or accepted |
| `unverifiable` | The job has not finished, nothing was compared, or only row counts were recorded. | No. There is nothing to sign. |

Sign-off is also blocked while a fix is being applied. A report that cannot be
loaded is not a verdict.

## Rolling back a migration

Datawrap does not undo committed rows in a destination. It offers three
things, and says which applies to a job:

| Strategy | What it is |
|---|---|
| Document only | A written plan for the operator. Nothing is executed. This is the default for writes into the primary table. |
| Discard staging | Drops the staging table `<table>_df_staging`. Never touches the primary table. Available only when the run wrote through staging. |
| Require warehouse restore | Points you to your database's own point-in-time recovery or time travel. Datawrap never runs it. |

`POST /transfer/{job_id}/rollback/execute` [job.manage] runs the discard
staging strategy only. To restore a primary table, use your database's own
backup or recovery.

## Reading an error reference

Many error responses end with `Reference: <id>`, and carry the same id in the
`X-Correlation-ID` response header. The message is deliberately general; the
detail is in the server log.

To find the cause:

1. Copy the reference id.
2. Search the API's logs for that id. The matching log event carries the
   event name, the HTTP status and the exception.

`DATAFLOW_EXPOSE_ERROR_DETAIL=1` adds a redacted copy of the exception text to
the response. Use it for debugging only.

Not every error has a reference. Some routes still return the raw error text.

## The API answers only /health

In production the API checks its settings at start. When something required
is missing it keeps `/health` up, reports `status: misconfigured`, and refuses
every other route. The missing settings are in the process log, each line
starting `[FATAL] Production config:` or naming a topology error. See
[configuration.md](configuration.md#what-production-requires).

## Not covered

- "Rehearsal" runs. No rehearsal feature was found in the service code at this
  commit, so its states are not described.
- Alert thresholds and dashboards.
- Recovery of the service's own database. See
  [backup-and-upgrade.md](backup-and-upgrade.md).

## Verified against

- `apps/api/services/job_status.py`, `apps/api/services/job_watchdog.py`,
  `apps/api/services/orphan_jobs.py`, `apps/api/services/lease_heartbeat.py`
- `apps/api/services/checkpoint_service.py` (resume rules)
- `apps/api/connectors/write_resilience.py`, `apps/api/services/destination_write_lock.py`
- `apps/api/services/cdc_lease.py`, `apps/api/services/cdc_capability.py`,
  `apps/api/services/cdc_capture_inventory.py`, `apps/api/src/routers/ops_router.py`
- `apps/api/services/batch_undo.py`, `apps/api/services/quarantine_dlq.py`
- `apps/api/services/parity_report.py` (verdicts), `apps/api/services/parity_signoff.py`
- `apps/api/services/migration_rollback.py`, `apps/api/services/recovery_honesty.py`
- `apps/api/services/safe_errors.py`
- `apps/api/src/main.py`, `apps/api/services/platform_config.py`
- `apps/api/tests/fixtures/rbac_route_permissions.json` (routes and permissions)
