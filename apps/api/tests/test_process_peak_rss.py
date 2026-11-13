"""A child process reports its own peak memory, not its parent's.

Linux carries the parent's resident set into a child's ``ru_maxrss`` across
fork/vfork + exec: the kernel records the old mm's high-water mark at exec.
So ``getrusage(RUSAGE_SELF).ru_maxrss`` in a fresh child reads at least the
parent's RSS at spawn time.

- The Proofs → Scale benchmark worker (``benchmarks.local_bench_worker``),
  spawned by the long-lived API process, reported the API's RSS as "peak
  resident memory of the benchmark process".
- test_g1h_shaped_memory_bounded_subprocess_pg measured its probe the same
  way: 314 MB standalone, but 1756 MB (local) and 2075 MB (PR 424 CI, api-tests
  4/6) after ~7000 tests had grown the pytest process.

``/proc/self/status`` VmHWM is the process's own high-water mark.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from services.process_peak_rss import own_peak_rss_bytes

API = Path(__file__).resolve().parents[1]

CHILD = (
    "import sys; sys.path.insert(0, {api!r});"
    "from {mod} import {fn}; print({fn}() // (1024 * 1024))"
)


def test_the_helper_reads_this_process_peak() -> None:
    assert own_peak_rss_bytes() > 10 * 1024 * 1024


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="the inheritance is Linux behaviour")
@pytest.mark.parametrize(
    ("mod", "fn"),
    [
        ("services.process_peak_rss", "own_peak_rss_bytes"),
        ("benchmarks.local_bench_worker", "_peak_rss_bytes"),
    ],
)
def test_a_child_spawned_from_a_large_parent_reports_its_own_peak(mod: str, fn: str) -> None:
    ballast = bytearray(400 * 1024 * 1024)
    for i in range(0, len(ballast), 4096):
        ballast[i] = 1
    out = subprocess.run(
        [sys.executable, "-c", CHILD.format(api=str(API), mod=mod, fn=fn)],
        capture_output=True, text=True, timeout=60, cwd=API,
    )
    del ballast
    assert out.returncode == 0, out.stderr
    peak_mb = int(out.stdout.strip())
    assert peak_mb < 200, f"{mod}.{fn} reported {peak_mb} MB: the parent's RSS, not the child's"
