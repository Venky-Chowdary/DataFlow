"""Time a real MySQL→SQLite identity append through the production stream engine.

Seeds MySQL `bench_sqlite_mysql` from SQLite when missing. Fails closed
unless dest COUNT(*) equals the source snapshot COUNT(*) with
rejected_rows=0. Empty dest is snapshot SELECT + executemany, not
mysqldump / sqlite3 `.import`. Dest `bench_sqlite_from_mysql` is unique
(not reused from `bench_1m` / `bench_pg_sqlite`). DATE lands as SQLite
TEXT (no DATE affinity).

    cd apps/api && python scripts/bench_mysql_to_sqlite_million.py

    BENCH_ROWS=1000000 BENCH_SRC=bench_sqlite_mysql BENCH_DEST=bench_sqlite_from_mysql \\
      python scripts/bench_mysql_to_sqlite_million.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from services.million_row_bench import run_mysql_sqlite_volume  # noqa: E402


if __name__ == "__main__":
    rows = int(os.environ.get("BENCH_ROWS", "1000000"))
    src = os.environ.get("BENCH_SRC", "bench_sqlite_mysql")
    dest = os.environ.get("BENCH_DEST", "bench_sqlite_from_mysql")
    proof = os.environ.get("BENCH_PROOF")
    if not proof:
        proof = str(Path("/opt/cursor/artifacts") / f"mysql_sqlite_{rows}_proof.json")
    run_mysql_sqlite_volume(
        rows=rows,
        dest_table=dest,
        source_table=src,
        dest_database=os.environ.get("BENCH_SQLITE_DEST_DB"),
        sync_mode=os.environ.get("BENCH_SYNC", "full_refresh_append"),
        keep_dest=os.environ.get("BENCH_KEEP_DEST") == "1",
        fail_closed=True,
        proof_path=proof,
    )
