"""Time a real SQL Server→PostgreSQL identity append through the production stream engine.

Uses the 10-column employee fixture already on SQL Server (`bench_ss_from_pg`)
when present; otherwise seeds it from PostgreSQL. Fails closed unless
destination COUNT(*) equals the source with rejected_rows=0.

    cd apps/api && python scripts/bench_sqlserver_to_pg_million.py

    BENCH_ROWS=1000000 BENCH_SRC=bench_ss_from_pg BENCH_DEST=bench_pg_from_ss \\
      python scripts/bench_sqlserver_to_pg_million.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from services.million_row_bench import run_sqlserver_pg_volume  # noqa: E402


if __name__ == "__main__":
    rows = int(os.environ.get("BENCH_ROWS", "1000000"))
    src = os.environ.get("BENCH_SRC", "bench_ss_from_pg")
    dest = os.environ.get("BENCH_DEST", "bench_pg_from_ss")
    proof = os.environ.get("BENCH_PROOF")
    if not proof:
        proof = str(Path("/opt/cursor/artifacts") / f"sqlserver_pg_{rows}_proof.json")
    run_sqlserver_pg_volume(
        rows=rows,
        dest_table=dest,
        source_table=src,
        sync_mode=os.environ.get("BENCH_SYNC", "full_refresh_append"),
        keep_dest=os.environ.get("BENCH_KEEP_DEST") == "1",
        fail_closed=True,
        proof_path=proof,
    )
