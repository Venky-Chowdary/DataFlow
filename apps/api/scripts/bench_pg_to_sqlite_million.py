"""Time a real PostgreSQL→SQLite identity append through the production stream engine.

Seeds PostgreSQL `bench_emp_*` when missing. Fails closed unless SQLite dest
`COUNT(*)` equals the source snapshot COUNT with rejected_rows=0. DATE
lands as TEXT (SQLite has no DATE affinity). Empty dest is executemany
insert, not `.import`. Dest `bench_pg_sqlite` is unique (not reused from
`bench_1m` / `bench_pg_mongo`).

    cd apps/api && python scripts/bench_pg_to_sqlite_million.py

    BENCH_ROWS=1000000 BENCH_SRC=bench_emp_1000000 BENCH_DEST=bench_pg_sqlite \\
      python scripts/bench_pg_to_sqlite_million.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from services.million_row_bench import run_pg_sqlite_volume  # noqa: E402


if __name__ == "__main__":
    rows = int(os.environ.get("BENCH_ROWS", "1000000"))
    src = os.environ.get("BENCH_SRC", f"bench_emp_{rows}")
    dest = os.environ.get("BENCH_DEST", "bench_pg_sqlite")
    proof = os.environ.get("BENCH_PROOF")
    if not proof:
        proof = str(Path("/opt/cursor/artifacts") / f"pg_sqlite_{rows}_proof.json")
    run_pg_sqlite_volume(
        rows=rows,
        dest_table=dest,
        source_table=src,
        dest_database=os.environ.get("BENCH_SQLITE_DEST_DB"),
        sync_mode=os.environ.get("BENCH_SYNC", "full_refresh_append"),
        keep_dest=os.environ.get("BENCH_KEEP_DEST") == "1",
        fail_closed=True,
        proof_path=proof,
    )
