"""Time a real MySQL→MySQL identity append through the production stream engine.

Requires the named MySQL fixture from the PG→MySQL bench (default ``bench_1m``).
Fails closed unless destination COUNT(*) equals the source with rejected_rows=0.

    cd apps/api && python scripts/bench_mysql_to_mysql_million.py

    BENCH_ROWS=1000000 BENCH_SRC=bench_1m BENCH_DEST=bench_mysql_clone \\
      python scripts/bench_mysql_to_mysql_million.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from services.million_row_bench import run_mysql_mysql_volume  # noqa: E402
from services.million_row_proof import skip_reason_if_unreachable  # noqa: E402


if __name__ == "__main__":
    skip = skip_reason_if_unreachable()
    if skip:
        print(skip)
        sys.exit(2)
    rows = int(os.environ.get("BENCH_ROWS", "1000000"))
    src = os.environ.get("BENCH_SRC", "bench_1m")
    dest = os.environ.get("BENCH_DEST", "bench_mysql_clone")
    proof = os.environ.get("BENCH_PROOF")
    if not proof:
        proof = str(Path("/opt/cursor/artifacts") / f"mysql_mysql_{rows}_proof.json")
    run_mysql_mysql_volume(
        rows=rows,
        source_table=src,
        dest_table=dest,
        sync_mode=os.environ.get("BENCH_SYNC", "full_refresh_append"),
        keep_dest=os.environ.get("BENCH_KEEP_DEST") == "1",
        fail_closed=True,
        proof_path=proof,
    )
