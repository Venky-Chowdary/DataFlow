"""Time a real Oracle→Oracle identity append through the production stream engine.

Seeds a named NUMBER PK fixture when missing. Fails closed unless destination
COUNT(*) equals the source with rejected_rows=0.

    cd apps/api && python scripts/bench_oracle_oracle_million.py

    BENCH_ROWS=1000000 BENCH_SRC=BENCH_ORA_1000000 BENCH_DEST=BENCH_ORA_CLONE \\
      python scripts/bench_oracle_oracle_million.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from services.million_row_bench import run_oracle_oracle_volume  # noqa: E402


if __name__ == "__main__":
    rows = int(os.environ.get("BENCH_ROWS", "1000000"))
    src = os.environ.get("BENCH_SRC", f"BENCH_ORA_{rows}")
    dest = os.environ.get("BENCH_DEST", "BENCH_ORA_CLONE")
    proof = os.environ.get("BENCH_PROOF")
    if not proof:
        proof = str(Path("/opt/cursor/artifacts") / f"oracle_oracle_{rows}_proof.json")
    run_oracle_oracle_volume(
        rows=rows,
        source_table=src,
        dest_table=dest,
        sync_mode=os.environ.get("BENCH_SYNC", "full_refresh_append"),
        keep_dest=os.environ.get("BENCH_KEEP_DEST") == "1",
        fail_closed=True,
        proof_path=proof,
    )
