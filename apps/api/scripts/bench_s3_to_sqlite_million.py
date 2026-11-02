"""Time a real S3→SQLite identity append through the production stream engine.

Seeds MinIO `bench_sqlite_s3.csv` from SQLite when missing. Fails closed
unless dest `COUNT(*)` equals the source artifact COUNT with
rejected_rows=0. Empty dest is executemany insert, not sqlite3 `.import`
/ `aws s3 cp`. JSON/JSONL/Parquet decline. Dest `bench_s3_from_sqlite`
is unique (not reused from `bench_1m` / `bench_pg_sqlite`).

    cd apps/api && python scripts/bench_s3_to_sqlite_million.py

    BENCH_ROWS=1000000 BENCH_SRC=bench_sqlite_s3.csv BENCH_DEST=bench_s3_from_sqlite \\
      python scripts/bench_s3_to_sqlite_million.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from services.million_row_bench import run_s3_sqlite_volume  # noqa: E402


if __name__ == "__main__":
    rows = int(os.environ.get("BENCH_ROWS", "1000000"))
    src = os.environ.get("BENCH_SRC", "bench_sqlite_s3.csv")
    dest = os.environ.get("BENCH_DEST", "bench_s3_from_sqlite")
    proof = os.environ.get("BENCH_PROOF")
    if not proof:
        proof = str(Path("/opt/cursor/artifacts") / f"s3_sqlite_{rows}_proof.json")
    run_s3_sqlite_volume(
        rows=rows,
        dest_table=dest,
        source_table=src,
        source_bucket=os.environ.get("BENCH_S3_BUCKET"),
        dest_database=os.environ.get("BENCH_SQLITE_DEST_DB"),
        sync_mode=os.environ.get("BENCH_SYNC", "full_refresh_append"),
        keep_dest=os.environ.get("BENCH_KEEP_DEST") == "1",
        fail_closed=True,
        proof_path=proof,
    )
