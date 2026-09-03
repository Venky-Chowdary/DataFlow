"""Time a real Oracle→Iceberg identity append through the production stream engine.

Seeds the named 10-column employee fixture on Oracle (`BENCH_MY_ORA`)
when missing (from MySQL `bench_1m`). Fails closed unless Iceberg dest
COUNT (file footers, never scan().count()) equals the source with
rejected_rows=0. Empty dest is CoW snapshot append, not MERGE INTO.

    cd apps/api && python scripts/bench_oracle_to_iceberg_million.py

    BENCH_ROWS=1000000 BENCH_SRC=BENCH_MY_ORA BENCH_DEST=bench_ora_iceberg \\
      python scripts/bench_oracle_to_iceberg_million.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from services.million_row_bench import run_oracle_iceberg_volume  # noqa: E402


if __name__ == "__main__":
    rows = int(os.environ.get("BENCH_ROWS", "1000000"))
    src = os.environ.get("BENCH_SRC", "BENCH_MY_ORA")
    dest = os.environ.get("BENCH_DEST", "bench_ora_iceberg")
    proof = os.environ.get("BENCH_PROOF")
    if not proof:
        proof = str(Path("/opt/cursor/artifacts") / f"oracle_iceberg_{rows}_proof.json")
    run_oracle_iceberg_volume(
        rows=rows,
        dest_table=dest,
        source_table=src,
        sync_mode=os.environ.get("BENCH_SYNC", "full_refresh_append"),
        keep_dest=os.environ.get("BENCH_KEEP_DEST") == "1",
        fail_closed=True,
        proof_path=proof,
    )
