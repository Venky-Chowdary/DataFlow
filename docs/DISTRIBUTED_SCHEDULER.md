# Distributed Transfer Scheduler (Phase F5)

## Problem (audit §1.3 D3)

A process-local `ThreadPoolExecutor` alone is not multi-replica safe: two API
instances can race the same Mongo job row. Leases mitigate; a durable **claim
queue** completes the contract.

## Modes (`DATAFLOW_SCHEDULER_MODE`)

| Mode | Behavior |
|------|----------|
| `local` | API thread pool + `worker_leases` only (single replica / demo) |
| `claim` | Enqueue → Mongo `transfer_job_queue` → claim under lease |
| `auto` (default) | `claim` when `requires_distributed_backend()`, else `local` |

Overrides: `DATAFLOW_WORKER_FLEET=1` forces claim; `=0` forces local.

## Ownership contract

```
enqueue:  transfer_job_queue.status = queued
claim:    queued → claimed + worker_leases.acquire (fence++)
run:      transfer_jobs → running (lease_fence on progress)
ack/done: queue → done|failed; lease released
stale:    claimed → queued (reclaim); expired lease steal bumps fence
```

## Who pulls?

* Dedicated worker: `python -m src.worker_main` (`WORKER_FLEET=1`)
* API claim loop (default when claim mode): `DATAFLOW_API_CLAIM_LOOP=1`  
  Set `API_CLAIM_LOOP=0` when only the Worker process should execute.

## Env cheat-sheet

| Variable | Default | Meaning |
|----------|---------|---------|
| `SCHEDULER_MODE` | `auto` | local / claim / auto |
| `WORKER_FLEET` | unset | Force claim/local |
| `API_CLAIM_LOOP` | `1` | API also claims |
| `TRANSFER_WORKERS` | `8` | Concurrent jobs per process |
| `WORKER_LEASE_TTL` | `60` | Lease heartbeat window (sec) |
| `WORKER_POLL` | `2` | Claim poll interval (sec) |

## Not Temporal (yet)

v1 safety = Mongo queue + leases + fences. Temporal/Celery/SQS remain a later
option for workflow history UX — not required for correctness.

## Scheduled run lifecycle (QA round 5)

Every scheduled attempt (cadence beat, retry, Run now) is one **run record** in
the schedule's `run_history`, written before any work starts
(`services/schedule_run_lifecycle.py`):

```
pending ──► running ──► completed | completed_with_* | failed | cancelled
   ├──────► failed   (refused or crashed before a job existed)
   └──────► skipped  (overlap, pending approval, append replay)
```

* Every transition is persisted on the record (`transitions`); a terminal
  record never changes again.
* Every terminal record has `reason` and `reason_code`.
* The displayed status (`last_status`, `last_status_reason`, `current_run`,
  `last_skip` on REST, MCP `get_schedule`/`list_schedules`, and the UI) is
  derived from the records. The stored `last_status` is a legacy mirror that no
  reader displays.
* Orphan sweep (every beat and at startup): an open record whose job's lease
  and heartbeat expired is failed `worker_lost` ("Worker lost: …") and the job
  document is settled the same way (conditional on the status read, so a job
  the job-orphan sweeper resumed first is left running). A `pending` record with
  no job older than `DATAFLOW_SCHEDULE_UNBOUND_CLAIM_GRACE_SEC` is failed the same way.
* Skipped beats are recorded with their reason; identical consecutive skips
  coalesce (`occurrences`, `last_seen_at`). A beat refused because another
  replica holds the fleet lock is not a skip — that replica runs it.
* Failures before a job exists consume the cadence slot (no hot loop), park
  after repeated identical failures, and notify the workspace.
* Backfill: records written before this existed are read through `run_view`
  (a `legacy-*` id and a reason derived from `error`).
