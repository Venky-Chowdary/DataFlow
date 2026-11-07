"""Child-process entry for the Proofs → Scale benchmark (R7).

``python -m benchmarks.local_bench_worker <rows>`` runs the CSV → SQLite
benchmark in its own process and writes JSON lines to stdout:

* ``{"type": "progress", "phase", "progress_pct", "rows_done", "elapsed_seconds"}``
  about twice a second, read from this process's in-memory job record;
* one ``{"type": "result", "report": {...}}`` at the end.

Its own process because: the hosted API must not have its environment
switched to the benchmark's in-memory store; ``ru_maxrss`` of a fresh process
is the benchmark's peak memory (in the long-lived API process the per-job
delta of a lifetime high-water mark reads 0 MB); and the request returns at
once instead of timing out behind a proxy.
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time


def _emit(payload: dict) -> None:
    sys.stdout.write(json.dumps(payload, default=str) + "\n")
    sys.stdout.flush()


def _peak_rss_bytes() -> int:
    try:
        import resource
    except ImportError:  # pragma: no cover - non-POSIX
        return 0
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(peak if sys.platform == "darwin" else peak * 1024)


def main(argv: list[str]) -> int:
    rows = int(argv[1])
    os.environ["DATAFLOW_JOB_STORE"] = "memory"
    os.environ["DATAFLOW_DISABLE_OBJECT_STORE"] = "1"
    t0 = time.monotonic()
    _emit({"type": "progress", "phase": "starting", "progress_pct": 0, "rows_done": 0,
           "elapsed_seconds": 0.0})
    import benchmarks.cloud_scale as bench
    from services.mongodb_service import get_mongodb_service

    startup = round(time.monotonic() - t0, 3)
    job_id = bench._new_job_id()
    done = threading.Event()

    def watch() -> None:
        store = get_mongodb_service()
        while not done.wait(0.5):
            try:
                job = store.get_job(job_id) or {}
            except Exception:  # noqa: BLE001
                job = {}
            _emit({
                "type": "progress",
                "phase": str(job.get("phase") or "transferring"),
                "progress_pct": float(job.get("progress_pct") or 0),
                "rows_done": int(job.get("records_processed") or 0),
                "elapsed_seconds": round(time.monotonic() - t0, 1),
            })

    watcher = threading.Thread(target=watch, daemon=True)
    watcher.start()
    try:
        report = bench.run_local_benchmark(rows, job_id=job_id)
    finally:
        done.set()
        watcher.join(timeout=2)
    peak = _peak_rss_bytes()
    report["peak_memory_bytes"] = peak
    report["peak_memory_mb"] = round(peak / (1024 * 1024), 1)
    report["peak_memory_basis"] = "peak resident memory of the benchmark process"
    report["startup_seconds"] = startup
    _emit({"type": "result", "report": report})
    return 0 if report.get("success") else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
