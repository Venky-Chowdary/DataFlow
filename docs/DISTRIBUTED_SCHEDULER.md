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
