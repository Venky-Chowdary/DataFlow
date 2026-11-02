"""Time a real SQL Server→S3 identity append through the production stream engine.

Seeds SQL Server `bench_sqlite_sqlserver` from SQLite when missing. Fails
closed unless dest artifact COUNT equals the source snapshot COUNT with
rejected_rows=0. Empty dest is HOLDLOCK SELECT CSV + upload, not BCP /
`aws s3 cp`. Dest key must be `.csv` / `.tsv`. Dest `bench_sqlserver_s3.csv`
is unique (not reused from `bench_1m` / `bench_pg_s3.csv` /
`bench_mysql_s3.csv` / `bench_sqlite_s3.csv`).

    cd apps/api && python scripts/bench_sqlserver_to_s3_million.py

    BENCH_ROWS=1000000 BENCH_SRC=bench_sqlite_sqlserver BENCH_DEST=bench_sqlserver_s3.csv \\
      python scripts/bench_sqlserver_to_s3_million.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from services.million_row_bench import run_sqlserver_s3_volume  # noqa: E402


if __name__ == "__main__":
    rows = int(os.environ.get("BENCH_ROWS", "1000000"))
    src = os.environ.get("BENCH_SRC", "bench_sqlite_sqlserver")
    dest = os.environ.get("BENCH_DEST", "bench_sqlserver_s3.csv")
    proof = os.environ.get("BENCH_PROOF")
    if not proof:
        proof = str(Path("/opt/cursor/artifacts") / f"sqlserver_s3_{rows}_proof.json")
    run_sqlserver_s3_volume(
        rows=rows,
        dest_table=dest,
        source_table=src,
        dest_bucket=os.environ.get("BENCH_S3_BUCKET"),
        sync_mode=os.environ.get("BENCH_SYNC", "full_refresh_append"),
        keep_dest=os.environ.get("BENCH_KEEP_DEST") == "1",
        fail_closed=True,
        proof_path=proof,
    )
