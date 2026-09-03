"""Time a real MySQL→Iceberg identity append through the production stream engine.

Seeds the named 10-column employee fixture on MySQL (`bench_1m`) when
missing. Fails closed unless Iceberg dest COUNT (file footers, never
scan().count()) equals the source with rejected_rows=0. Empty dest is
CoW snapshot append, not MERGE INTO. MySQL has no COPY TO STDOUT — one
consistent-snapshot SELECT is encoded as CSV.

    cd apps/api && python scripts/bench_mysql_to_iceberg_million.py

    BENCH_ROWS=1000000 BENCH_SRC=bench_1m BENCH_DEST=bench_mysql_iceberg \\
      python scripts/bench_mysql_to_iceberg_million.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from services.million_row_bench import run_mysql_iceberg_volume  # noqa: E402


if __name__ == "__main__":
    rows = int(os.environ.get("BENCH_ROWS", "1000000"))
    src = os.environ.get("BENCH_SRC", "bench_1m")
    dest = os.environ.get("BENCH_DEST", "bench_mysql_iceberg")
    proof = os.environ.get("BENCH_PROOF")
    if not proof:
        proof = str(Path("/opt/cursor/artifacts") / f"mysql_iceberg_{rows}_proof.json")
    run_mysql_iceberg_volume(
        rows=rows,
        dest_table=dest,
        source_table=src,
        sync_mode=os.environ.get("BENCH_SYNC", "full_refresh_append"),
        keep_dest=os.environ.get("BENCH_KEEP_DEST") == "1",
        fail_closed=True,
        proof_path=proof,
    )
