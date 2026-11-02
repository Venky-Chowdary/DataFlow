"""Time a real SQLite→S3 identity append through the production stream engine.

Seeds `/tmp/dataflow-sqlite-bench/src.db` table `bench_pg_sqlite` from
PostgreSQL when missing (hire_date is TEXT after that hop). Fails closed
unless S3 dest artifact COUNT equals the source COUNT with
rejected_rows=0. Empty dest is SELECT CSV + upload, not `.dump` /
`aws s3 cp`. Dest key must be `.csv` / `.tsv`. Dest `bench_sqlite_s3.csv`
is unique (not reused from `bench_1m` / `bench_pg_s3.csv` /
`bench_mysql_s3.csv`).

    cd apps/api && python scripts/bench_sqlite_to_s3_million.py

    BENCH_ROWS=1000000 BENCH_SRC=bench_pg_sqlite BENCH_DEST=bench_sqlite_s3.csv \\
      python scripts/bench_sqlite_to_s3_million.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from services.million_row_bench import run_sqlite_s3_volume  # noqa: E402


if __name__ == "__main__":
    rows = int(os.environ.get("BENCH_ROWS", "1000000"))
    src = os.environ.get("BENCH_SRC", "bench_pg_sqlite")
    dest = os.environ.get("BENCH_DEST", "bench_sqlite_s3.csv")
    proof = os.environ.get("BENCH_PROOF")
    if not proof:
        proof = str(Path("/opt/cursor/artifacts") / f"sqlite_s3_{rows}_proof.json")
    run_sqlite_s3_volume(
        rows=rows,
        dest_table=dest,
        source_table=src,
        source_database=os.environ.get("BENCH_SQLITE_SRC_DB"),
        dest_bucket=os.environ.get("BENCH_S3_BUCKET"),
        sync_mode=os.environ.get("BENCH_SYNC", "full_refresh_append"),
        keep_dest=os.environ.get("BENCH_KEEP_DEST") == "1",
        fail_closed=True,
        proof_path=proof,
    )
