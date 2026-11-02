#!/usr/bin/env python3
"""Phase F6 — measured sqlite→sqlite microbench (named hardware, real numbers)."""

from __future__ import annotations

import json
import os
import platform
import sqlite3
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def _make_db(path: Path, rows: int) -> None:
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, payload TEXT)")
    conn.executemany(
        "INSERT INTO src VALUES (?, ?)",
        [(i, f"row-{i}") for i in range(rows)],
    )
    conn.commit()
    conn.close()


def _run_once(*, rows: int, parallel: int) -> dict:
    from services.checkpoint_service import CheckpointService
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    class _FakeMongo:
        def __init__(self) -> None:
            self.jobs: dict = {}

        def get_job(self, job_id: str):
            return self.jobs.get(job_id)

        def update_job_status(self, job_id: str, status: str, **kwargs) -> bool:
            self.jobs.setdefault(job_id, {})
            self.jobs[job_id].update(kwargs)
            self.jobs[job_id]["status"] = status
            return True

    os.environ["DATAFLOW_PARALLEL_WORKERS"] = str(parallel)
    os.environ["DATAWRAP_PARALLEL_WORKERS"] = str(parallel)

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        src = tmp_path / "src.db"
        dst = tmp_path / "dst.db"
        _make_db(src, rows)
        t0 = time.perf_counter()
        written, _ddl, summary, _cols = stream_database_transfer(
            EndpointConfig(
                kind="database", format="sqlite", database=str(src), table="src"
            ),
            EndpointConfig(
                kind="database", format="sqlite", database=str(dst), table="dst"
            ),
            [
                {"source": "id", "target": "id"},
                {"source": "payload", "target": "payload"},
            ],
            {"id": "integer", "payload": "string"},
            job_id="00000000000000000000bench",
            checkpoint_service=CheckpointService(_FakeMongo()),
            stream_contracts=[
                {"selected": True, "sync_mode": "full_refresh_overwrite", "primary_key": "id"}
            ],
        )
        elapsed = time.perf_counter() - t0
    return {
        "rows": rows,
        "rows_written": written,
        "parallel_workers": parallel,
        "elapsed_sec": round(elapsed, 4),
        "rows_per_sec": round(written / elapsed, 2) if elapsed > 0 else 0,
        "pagination_mode": summary.get("pagination_mode"),
        "checksum_mode": summary.get("checksum_mode"),
    }


def main() -> int:
    rows = int(os.environ.get("BENCH_ROWS", "20000"))
    results = []
    for parallel in (1, 4):
        results.append(_run_once(rows=rows, parallel=parallel))

    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "schema_version": 1,
        "hardware": {
            "platform": platform.platform(),
            "processor": platform.processor(),
            "python": sys.version.split()[0],
            "cpu_count": os.cpu_count(),
        },
        "route": "sqlite→sqlite",
        "runs": results,
        "honesty_note": (
            "Developer microbench only — not a warehouse SLA. "
            "Publish with named hardware; never invent rows/sec in marketing."
        ),
    }
    dest = _API_ROOT / "data" / "proofs" / "throughput_microbench.json"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(out, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(out, indent=2))
    print(f"Wrote {dest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
