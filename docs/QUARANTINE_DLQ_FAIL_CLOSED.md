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

## Crash / retry / rollback honesty (Module 5 / Phase 3)

| Event | Behavior |
|-------|----------|
| Batch/stream reject | Persist **new** rejected rows to control-plane DLQ **before** checkpoint advances |
| Buffered checkpoint | Same — delta persist; refuse continue if DLQ fails |
| Job finalize | Persist only the **unpersisted suffix** (no duplicate DLQ append) |
| Crash after primary write, before DLQ | Fail closed on resume/finalize if rejects undurable — **not** destination XA/2PC |
| `SKIP_ROW` | Audit skip (`disposition=skipped`); not replay-quarantine |
| `QUARANTINE_ROW` | Full holdout for replay (`quarantine_required=true`) |
| Rollback | Staging discard only when planned; quarantine rows are **not** auto-undone on primary |

Transactional quarantine bound to destination commit is **not** claimed.

## Guarantees

- Operators cannot get a green terminal status while rejected rows are undurable
- Replay has a durable trail when quarantine completed

## Non-guarantees

- Dest-table DLQ write failure alone does not fail the job if control-plane succeeded
- Job document `rejected_details` may still exist in memory even when DLQ failed
  (job is marked failed so Execute is not treated as success)
