# Quarantine DLQ Fail-Closed (Module 5)

## Promise

**Rejected rows are never lost silently.**

If a transfer quarantines / rejects rows, the control-plane DLQ **must** be
durable before the job can finish as `completed` or `completed_with_quarantine`.

## Before (P0)

`_persist_job_quarantine` was best-effort:

- Persist failure → `quarantine_dlq_error` + `quarantine_durable=False`
- Job still completed successfully
- Replay UI looked healthy but found nothing

## After

| Condition | Terminal behavior |
|-----------|-------------------|
| No rejected rows | OK (`quarantine_durable=True` vacuously) |
| Rejects + durable DLQ | OK (`completed_with_quarantine` as usual) |
| Rejects + DLQ persist failed | **Fail closed** — `QuarantineDlqLostError` → job `failed` |

Destination-side `{table}_df_quarantine` remains best-effort SQL convenience.
Control-plane Mongo/JSONL is the Migration Assurance authority for replay.

## Code SSOT

- Policy: `apps/api/services/quarantine_dlq.py`
  - `QuarantineDlqLostError`
  - `persist_job_quarantine_outcome`
  - `assert_quarantine_durable_or_raise`
- Engine: `apps/api/src/transfer/engine.py` → `_persist_job_quarantine`

## Guarantees

- Operators cannot get a green terminal status while rejected rows are undurable
- Replay has a durable trail when quarantine completed

## Non-guarantees

- Dest-table DLQ write failure alone does not fail the job if control-plane succeeded
- Job document `rejected_details` may still exist in memory even when DLQ failed
  (job is marked failed so Execute is not treated as success)
