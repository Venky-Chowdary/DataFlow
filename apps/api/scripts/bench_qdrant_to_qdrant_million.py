"""Time a real Qdrant→Qdrant identity append through the production stream engine.

Seeds `bench_qdrant_src` when missing. Fails closed unless dest
`points_count` equals the source count with rejected_rows=0. Empty dest is
scroll+upsert of raw id/vector/payload, not vectorize / re-embed /
snapshot restore. Same collection declines. Dest `bench_qdrant_clone` is
unique (not reused from other bench clones).

    cd apps/api && python scripts/bench_qdrant_to_qdrant_million.py

    BENCH_ROWS=1000000 BENCH_SRC=bench_qdrant_src BENCH_DEST=bench_qdrant_clone \\
      python scripts/bench_qdrant_to_qdrant_million.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from services.million_row_bench import run_qdrant_qdrant_volume  # noqa: E402


if __name__ == "__main__":
    rows = int(os.environ.get("BENCH_ROWS", "1000000"))
    src = os.environ.get("BENCH_SRC", "bench_qdrant_src")
    dest = os.environ.get("BENCH_DEST", "bench_qdrant_clone")
    proof = os.environ.get("BENCH_PROOF")
    if not proof:
        proof = str(Path("/opt/cursor/artifacts") / f"qdrant_qdrant_{rows}_proof.json")
    run_qdrant_qdrant_volume(
        rows=rows,
        dest_table=dest,
        source_table=src,
        sync_mode=os.environ.get("BENCH_SYNC", "full_refresh_append"),
        keep_dest=os.environ.get("BENCH_KEEP_DEST") == "1",
        fail_closed=True,
        proof_path=proof,
    )
