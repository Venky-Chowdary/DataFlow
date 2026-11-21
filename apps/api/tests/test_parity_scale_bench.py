"""Parity at Scale 1M-row benchmark (opt-in): exact faults, bounded memory.

Runs ``scripts/bench_parity_scale_pg.py`` in its own process (so its peak RSS
is the scan's, not pytest's) when ``DATAFLOW_PARITY_SCALE_BENCH_ROWS`` is set —
the ``parity-scale-pg`` CI job sets it to 1,000,000. Asserts every injected
fault is found with its exact key in mode R and mode H, full coverage, and the
design's memory target (under 256 MB RSS per scan worker). A wall-clock ceiling
is asserted only when ``DATAFLOW_PARITY_SCALE_BENCH_MAX_SEC`` is set.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROWS = int(os.environ.get("DATAFLOW_PARITY_SCALE_BENCH_ROWS") or 0)
API = Path(__file__).resolve().parents[1]


@pytest.mark.skipif(not ROWS, reason="benchmark not requested: set DATAFLOW_PARITY_SCALE_BENCH_ROWS")
def test_parity_scale_benchmark_finds_every_fault_within_the_memory_target() -> None:
    args = [sys.executable, str(API / "scripts" / "bench_parity_scale_pg.py"), "--rows", str(ROWS)]
    if os.environ.get("DATAFLOW_PARITY_SCALE_BENCH_UNLOGGED"):
        args.append("--unlogged")
    proc = subprocess.run(args, cwd=API, capture_output=True, text=True, timeout=3600, check=False)
    assert proc.returncode == 0, proc.stdout[-4000:] + proc.stderr[-4000:]
    report = json.loads(proc.stdout)
    out = os.environ.get("DATAFLOW_PARITY_SCALE_BENCH_OUT")
    if out:
        Path(out).write_text(json.dumps(report, indent=1), encoding="utf-8")
    assert {r["mode"] for r in report["runs"]} == {"R", "H"}
    for run in report["runs"]:
        assert run["faults_found_exactly"], run
        assert run["complete"], run
    assert report["peak_rss_mb"] < 256, report
    ceiling = os.environ.get("DATAFLOW_PARITY_SCALE_BENCH_MAX_SEC")
    if ceiling:
        assert max(r["seconds"] for r in report["runs"]) <= float(ceiling), report
