"""Time a real SQLite→Oracle identity append through the production stream engine.

Seeds `/tmp/dataflow-sqlite-bench/src.db` table `bench_pg_sqlite` from
PostgreSQL when missing (hire_date is TEXT after that hop). Fails closed
unless dest COUNT(*) equals the source COUNT with rejected_rows=0. Empty
dest is SELECT + executemany, not sqlldr / Data Pump / `.dump`. Dest
`bench_sqlite_oracle` is unique (not reused from `bench_1m` /
`bench_pg_oracle` / `bench_mysql_ora`). VARCHAR2 stores `''` as NULL
(engine law, counted in empty_string_as_null_cells).

    cd apps/api && python scripts/bench_sqlite_to_oracle_million.py

    BENCH_ROWS=1000000 BENCH_SRC=bench_pg_sqlite BENCH_DEST=bench_sqlite_oracle \\
      python scripts/bench_sqlite_to_oracle_million.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from services.million_row_bench import run_sqlite_oracle_volume  # noqa: E402


if __name__ == "__main__":
    rows = int(os.environ.get("BENCH_ROWS", "1000000"))
    src = os.environ.get("BENCH_SRC", "bench_pg_sqlite")
    dest = os.environ.get("BENCH_DEST", "bench_sqlite_oracle")
    proof = os.environ.get("BENCH_PROOF")
    if not proof:
        proof = str(Path("/opt/cursor/artifacts") / f"sqlite_oracle_{rows}_proof.json")
    run_sqlite_oracle_volume(
        rows=rows,
        dest_table=dest,
        source_table=src,
        source_database=os.environ.get("BENCH_SQLITE_SRC_DB"),
        sync_mode=os.environ.get("BENCH_SYNC", "full_refresh_append"),
        keep_dest=os.environ.get("BENCH_KEEP_DEST") == "1",
        fail_closed=True,
        proof_path=proof,
    )
