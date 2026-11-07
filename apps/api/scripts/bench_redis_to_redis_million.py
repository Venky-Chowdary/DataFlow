"""Time a real Redis→Redis identity append through the production stream engine.

Seeds desktop-lab Redis `bench_redis_src` when missing. Fails closed unless
dest prefix COUNT equals the source COUNT with rejected_rows=0.
Empty dest is server-side Redis COPY, not GET+SET / DUMP+RESTORE / DBSIZE.
Same host+port+db+prefix declines. Dest `bench_redis_clone` is unique
(not reused from `bench_1m` / `bench_gcs_clone.jsonl` / `bench_adls_clone.jsonl`).

    cd apps/api && python scripts/bench_redis_to_redis_million.py

    BENCH_ROWS=1000000 BENCH_SRC=bench_redis_src BENCH_DEST=bench_redis_clone \\
      python scripts/bench_redis_to_redis_million.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from services.million_row_bench import run_redis_redis_volume  # noqa: E402


if __name__ == "__main__":
    rows = int(os.environ.get("BENCH_ROWS", "1000000"))
    src = os.environ.get("BENCH_SRC", "bench_redis_src")
    dest = os.environ.get("BENCH_DEST", "bench_redis_clone")
    proof = os.environ.get("BENCH_PROOF")
    if not proof:
        proof = str(Path("/opt/cursor/artifacts") / f"redis_redis_{rows}_proof.json")
    run_redis_redis_volume(
        rows=rows,
        dest_table=dest,
        source_table=src,
        sync_mode=os.environ.get("BENCH_SYNC", "full_refresh_append"),
        keep_dest=os.environ.get("BENCH_KEEP_DEST") == "1",
        fail_closed=True,
        proof_path=proof,
    )
