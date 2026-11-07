"""Time a real Iceberg→SQLite identity append through the production stream engine.

Seeds Iceberg `bench_sqlite_iceberg` from SQLite when missing. Fails
closed unless dest COUNT(*) equals the source footer COUNT with
rejected_rows=0. Empty dest is snapshot Parquet + executemany, not
`.import` / `MERGE INTO`. Dest `bench_sqlite_from_iceberg` is unique
(not reused from `bench_1m` / `bench_pg_sqlite` / `bench_ice_mongo`).
DATE lands as SQLite TEXT (no DATE affinity). Iceberg times are local
warehouse (`file:///tmp/iceberg-rest-wh`), not S3/Glue.

    cd apps/api && python scripts/bench_iceberg_to_sqlite_million.py

    BENCH_ROWS=1000000 BENCH_SRC=bench_sqlite_iceberg BENCH_DEST=bench_sqlite_from_iceberg \\
      python scripts/bench_iceberg_to_sqlite_million.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from services.million_row_bench import run_iceberg_sqlite_volume  # noqa: E402


if __name__ == "__main__":
    rows = int(os.environ.get("BENCH_ROWS", "1000000"))
    src = os.environ.get("BENCH_SRC", "bench_sqlite_iceberg")
    dest = os.environ.get("BENCH_DEST", "bench_sqlite_from_iceberg")
    proof = os.environ.get("BENCH_PROOF")
    if not proof:
        proof = str(Path("/opt/cursor/artifacts") / f"iceberg_sqlite_{rows}_proof.json")
    run_iceberg_sqlite_volume(
        rows=rows,
        dest_table=dest,
        source_table=src,
        dest_database=os.environ.get("BENCH_SQLITE_DEST_DB"),
        sync_mode=os.environ.get("BENCH_SYNC", "full_refresh_append"),
        keep_dest=os.environ.get("BENCH_KEEP_DEST") == "1",
        fail_closed=True,
        proof_path=proof,
    )
