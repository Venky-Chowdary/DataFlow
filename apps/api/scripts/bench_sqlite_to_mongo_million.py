"""Time a real SQLite→MongoDB identity append through the production stream engine.

Seeds `/tmp/dataflow-sqlite-bench/src.db` table `bench_pg_sqlite` from
PostgreSQL when missing (hire_date is TEXT after that hop). Fails closed
unless dest count_documents({}) equals the source COUNT with
rejected_rows=0. Empty dest is SELECT fetchmany + insert_many, not
`.dump` / `mongoimport`. Dest `bench_sqlite_mongo` is unique (not reused
from `bench_1m` / `bench_pg_mongo` / `bench_mysql_mongo`).

    cd apps/api && python scripts/bench_sqlite_to_mongo_million.py

    BENCH_ROWS=1000000 BENCH_SRC=bench_pg_sqlite BENCH_DEST=bench_sqlite_mongo \\
      python scripts/bench_sqlite_to_mongo_million.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from services.million_row_bench import run_sqlite_mongo_volume  # noqa: E402


if __name__ == "__main__":
    rows = int(os.environ.get("BENCH_ROWS", "1000000"))
    src = os.environ.get("BENCH_SRC", "bench_pg_sqlite")
    dest = os.environ.get("BENCH_DEST", "bench_sqlite_mongo")
    proof = os.environ.get("BENCH_PROOF")
    if not proof:
        proof = str(Path("/opt/cursor/artifacts") / f"sqlite_mongo_{rows}_proof.json")
    run_sqlite_mongo_volume(
        rows=rows,
        dest_table=dest,
        source_table=src,
        source_database=os.environ.get("BENCH_SQLITE_SRC_DB"),
        sync_mode=os.environ.get("BENCH_SYNC", "full_refresh_append"),
        keep_dest=os.environ.get("BENCH_KEEP_DEST") == "1",
        fail_closed=True,
        proof_path=proof,
    )
