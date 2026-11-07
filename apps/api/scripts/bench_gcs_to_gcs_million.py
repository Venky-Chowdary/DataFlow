"""Time a real GCS→GCS identity append through the production stream engine.

Seeds fake-gcs `bench_gcs_src.jsonl` when missing. Fails closed unless
dest artifact COUNT equals the source COUNT with rejected_rows=0.
Empty dest is server-side copy_blob / rewrite, not GET+PUT / `gsutil cp`.
Same endpoint+bucket+object declines. Dest `bench_gcs_clone.jsonl` is
unique (not reused from `bench_s3_clone.csv` / `bench_1m`).

    cd apps/api && python scripts/bench_gcs_to_gcs_million.py

    BENCH_ROWS=1000000 BENCH_SRC=bench_gcs_src.jsonl BENCH_DEST=bench_gcs_clone.jsonl \\
      python scripts/bench_gcs_to_gcs_million.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from services.million_row_bench import run_gcs_gcs_volume  # noqa: E402


if __name__ == "__main__":
    rows = int(os.environ.get("BENCH_ROWS", "1000000"))
    src = os.environ.get("BENCH_SRC", "bench_gcs_src.jsonl")
    dest = os.environ.get("BENCH_DEST", "bench_gcs_clone.jsonl")
    proof = os.environ.get("BENCH_PROOF")
    if not proof:
        proof = str(Path("/opt/cursor/artifacts") / f"gcs_gcs_{rows}_proof.json")
    run_gcs_gcs_volume(
        rows=rows,
        dest_table=dest,
        source_table=src,
        dest_bucket=os.environ.get("BENCH_GCS_BUCKET"),
        source_bucket=os.environ.get("BENCH_GCS_SRC_BUCKET"),
        sync_mode=os.environ.get("BENCH_SYNC", "full_refresh_append"),
        keep_dest=os.environ.get("BENCH_KEEP_DEST") == "1",
        fail_closed=True,
        proof_path=proof,
    )
