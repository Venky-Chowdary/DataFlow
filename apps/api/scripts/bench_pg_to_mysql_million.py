"""Time a real PostgreSQL→MySQL append through the production stream engine.

Discovers the reachable local pair (5432/3306 first, then 5433/3307). Uses the
memory job store when Mongo 27017 is down. Always prints destination COUNT(*)
and fails closed on a clean-fixture mismatch.

    cd apps/api && python scripts/bench_pg_to_mysql_million.py

    BENCH_ROWS=1000000 BENCH_DEST=bench_1m \\
      python scripts/bench_pg_to_mysql_million.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from services.million_row_bench import run_pg_mysql_volume  # noqa: E402
from services.million_row_proof import skip_reason_if_unreachable  # noqa: E402


if __name__ == "__main__":
    skip = skip_reason_if_unreachable()
    if skip:
        print(skip)
        sys.exit(2)
    rows = int(os.environ.get("BENCH_ROWS", "1000000"))
    dest = os.environ.get("BENCH_DEST", "bench_dest")
    proof = os.environ.get("BENCH_PROOF")
    if not proof:
        proof = str(Path("/opt/cursor/artifacts") / f"pg_mysql_{rows}_proof.json")
    run_pg_mysql_volume(
        rows=rows,
        dest_table=dest,
        sync_mode=os.environ.get("BENCH_SYNC", "full_refresh_append"),
        keep_dest=os.environ.get("BENCH_KEEP_DEST") == "1",
        fail_closed=True,
        proof_path=proof,
    )
