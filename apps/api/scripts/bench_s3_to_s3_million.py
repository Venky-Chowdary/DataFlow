"""Time a real S3→S3 identity append through the production stream engine.

Seeds MinIO `bench_pg_s3.csv` from PostgreSQL when missing. Fails closed
unless dest artifact COUNT equals the source COUNT with rejected_rows=0.
Empty dest is server-side CopyObject, not GET+PUT / `aws s3 cp` /
`aws s3 sync`. Same endpoint+bucket+key declines. Dest `bench_s3_clone.csv`
is unique (not reused from `bench_pg_s3.csv` / `bench_1m`).

    cd apps/api && python scripts/bench_s3_to_s3_million.py

    BENCH_ROWS=1000000 BENCH_SRC=bench_pg_s3.csv BENCH_DEST=bench_s3_clone.csv \\
      python scripts/bench_s3_to_s3_million.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from services.million_row_bench import run_s3_s3_volume  # noqa: E402


if __name__ == "__main__":
    rows = int(os.environ.get("BENCH_ROWS", "1000000"))
    src = os.environ.get("BENCH_SRC", "bench_pg_s3.csv")
    dest = os.environ.get("BENCH_DEST", "bench_s3_clone.csv")
    proof = os.environ.get("BENCH_PROOF")
    if not proof:
        proof = str(Path("/opt/cursor/artifacts") / f"s3_s3_{rows}_proof.json")
    run_s3_s3_volume(
        rows=rows,
        dest_table=dest,
        source_table=src,
        dest_bucket=os.environ.get("BENCH_S3_BUCKET"),
        source_bucket=os.environ.get("BENCH_S3_SRC_BUCKET"),
        sync_mode=os.environ.get("BENCH_SYNC", "full_refresh_append"),
        keep_dest=os.environ.get("BENCH_KEEP_DEST") == "1",
        fail_closed=True,
        proof_path=proof,
    )
