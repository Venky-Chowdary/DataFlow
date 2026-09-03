"""Time a real Iceberg→Iceberg identity append through the production stream engine.

Uses Iceberg REST table `bench_mysql_iceberg` when present; otherwise seeds
it from MySQL `bench_1m`. Fails closed unless dest footer COUNT equals the
source footer COUNT with rejected_rows=0. Source COUNT is never
scan().count(). Payload is never scan().to_arrow() / MERGE INTO. Empty dest
is CoW snapshot append. Same table declines. Dest `bench_ice_clone` is
unique (not reused from `bench_ice_mongo` / `bench_pg_iceberg`).

    cd apps/api && python scripts/bench_iceberg_to_iceberg_million.py

    BENCH_ROWS=1000000 BENCH_SRC=bench_mysql_iceberg BENCH_DEST=bench_ice_clone \\
      python scripts/bench_iceberg_to_iceberg_million.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from services.million_row_bench import run_iceberg_iceberg_volume  # noqa: E402


if __name__ == "__main__":
    rows = int(os.environ.get("BENCH_ROWS", "1000000"))
    src = os.environ.get("BENCH_SRC", "bench_mysql_iceberg")
    dest = os.environ.get("BENCH_DEST", "bench_ice_clone")
    proof = os.environ.get("BENCH_PROOF")
    if not proof:
        proof = str(Path("/opt/cursor/artifacts") / f"iceberg_iceberg_{rows}_proof.json")
    run_iceberg_iceberg_volume(
        rows=rows,
        dest_table=dest,
        source_table=src,
        sync_mode=os.environ.get("BENCH_SYNC", "full_refresh_append"),
        keep_dest=os.environ.get("BENCH_KEEP_DEST") == "1",
        fail_closed=True,
        proof_path=proof,
    )
