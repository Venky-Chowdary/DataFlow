"""One scheduler worker process, for the duplicate-execution race.

Two threads in one interpreter cannot falsify the guard: ``mark_schedule_running``
holds an in-process lock, so the second caller queues behind the first no matter
what the store does. Two *processes* contend the way two replicas do — different
instance ids, no shared lock, only the store's conditional claim between them —
so the race is run here and the parent grades the printed outcome.

    python -m tests.scale.scheduler_worker <schedule_id> [barrier_epoch]
"""

from __future__ import annotations

import sys
import time


def main(argv: list[str]) -> int:
    schedule_id = argv[0]
    barrier = float(argv[1]) if len(argv) > 1 else 0.0
    while barrier and time.time() < barrier:
        time.sleep(0.005)
    from services import schedule_runner

    try:
        job_id = schedule_runner._run_schedule(schedule_id)
    except Exception as exc:  # noqa: BLE001 — the refusal is the measurement
        print(f"REFUSED {type(exc).__name__}: {exc}", flush=True)
        return 0
    print(f"DISPATCHED {job_id or ''}".strip(), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
