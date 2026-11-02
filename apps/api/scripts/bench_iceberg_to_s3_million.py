"""Time a real Iceberg→S3 identity append through the production stream engine.

Seeds Iceberg `bench_s3_iceberg` from S3 when missing. Fails closed
unless dest artifact COUNT equals the source footer COUNT with
rejected_rows=0. Empty dest is snapshot Parquet + upload, not
`MERGE INTO` / `aws s3 cp`. Dest key must be `.csv` / `.tsv`. Dest
`bench_s3_from_iceberg.csv` is unique (not reused from `bench_1m` /
`bench_sqlite_s3.csv` / `bench_s3_clone.csv`). Nested list/map/struct
decline. Iceberg times are local warehouse (`file:///tmp/iceberg-rest-wh`),
not S3/Glue.

    cd apps/api && python scripts/bench_iceberg_to_s3_million.py

    BENCH_ROWS=1000000 BENCH_SRC=bench_s3_iceberg BENCH_DEST=bench_s3_from_iceberg.csv \\
      python scripts/bench_iceberg_to_s3_million.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from services.million_row_bench import run_iceberg_s3_volume  # noqa: E402


if __name__ == "__main__":
    rows = int(os.environ.get("BENCH_ROWS", "1000000"))
    src = os.environ.get("BENCH_SRC", "bench_s3_iceberg")
    dest = os.environ.get("BENCH_DEST", "bench_s3_from_iceberg.csv")
    proof = os.environ.get("BENCH_PROOF")
    if not proof:
        proof = str(Path("/opt/cursor/artifacts") / f"iceberg_s3_{rows}_proof.json")
    run_iceberg_s3_volume(
        rows=rows,
        dest_table=dest,
        source_table=src,
        dest_bucket=os.environ.get("BENCH_S3_BUCKET"),
        sync_mode=os.environ.get("BENCH_SYNC", "full_refresh_append"),
        keep_dest=os.environ.get("BENCH_KEEP_DEST") == "1",
        fail_closed=True,
        proof_path=proof,
    )
