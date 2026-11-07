"""Time a real PostgreSQL→Iceberg identity append through the production stream engine.

Seeds the named 10-column employee fixture when missing. Fails closed unless
Iceberg dest COUNT (file footers, never scan().count()) equals the source
with rejected_rows=0. Empty dest is CoW snapshot append, not MERGE INTO.

    cd apps/api && python scripts/bench_pg_to_iceberg_million.py

    BENCH_ROWS=1000000 BENCH_DEST=bench_pg_iceberg \\
      python scripts/bench_pg_to_iceberg_million.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from services.million_row_bench import run_pg_iceberg_volume  # noqa: E402


if __name__ == "__main__":
    rows = int(os.environ.get("BENCH_ROWS", "1000000"))
    src = os.environ.get("BENCH_SRC")
    dest = os.environ.get("BENCH_DEST", "bench_pg_iceberg")
    proof = os.environ.get("BENCH_PROOF")
    if not proof:
        proof = str(Path("/opt/cursor/artifacts") / f"pg_iceberg_{rows}_proof.json")
    run_pg_iceberg_volume(
        rows=rows,
        dest_table=dest,
        source_table=src,
        sync_mode=os.environ.get("BENCH_SYNC", "full_refresh_append"),
        keep_dest=os.environ.get("BENCH_KEEP_DEST") == "1",
        fail_closed=True,
        proof_path=proof,
    )
