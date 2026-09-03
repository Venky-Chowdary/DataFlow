"""Time a real SQLite→Iceberg identity append through the production stream engine.

Seeds `/tmp/dataflow-sqlite-bench/src.db` table `bench_pg_sqlite` from
PostgreSQL when missing (hire_date is TEXT after that hop). Fails closed
unless Iceberg dest COUNT (file footers, never scan().count()) equals
the source COUNT with rejected_rows=0. Empty dest is SELECT CSV + CoW
snapshot append, not `.dump` / `MERGE INTO`. Dest `bench_sqlite_iceberg`
is unique (not reused from `bench_1m` / `bench_pg_iceberg` /
`bench_mysql_ice`). Iceberg times are local warehouse
(`file:///tmp/iceberg-rest-wh`), not S3/Glue.

    cd apps/api && python scripts/bench_sqlite_to_iceberg_million.py

    BENCH_ROWS=1000000 BENCH_SRC=bench_pg_sqlite BENCH_DEST=bench_sqlite_iceberg \\
      python scripts/bench_sqlite_to_iceberg_million.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from services.million_row_bench import run_sqlite_iceberg_volume  # noqa: E402


if __name__ == "__main__":
    rows = int(os.environ.get("BENCH_ROWS", "1000000"))
    src = os.environ.get("BENCH_SRC", "bench_pg_sqlite")
    dest = os.environ.get("BENCH_DEST", "bench_sqlite_iceberg")
    proof = os.environ.get("BENCH_PROOF")
    if not proof:
        proof = str(Path("/opt/cursor/artifacts") / f"sqlite_iceberg_{rows}_proof.json")
    run_sqlite_iceberg_volume(
        rows=rows,
        dest_table=dest,
        source_table=src,
        source_database=os.environ.get("BENCH_SQLITE_SRC_DB"),
        sync_mode=os.environ.get("BENCH_SYNC", "full_refresh_append"),
        keep_dest=os.environ.get("BENCH_KEEP_DEST") == "1",
        fail_closed=True,
        proof_path=proof,
    )
