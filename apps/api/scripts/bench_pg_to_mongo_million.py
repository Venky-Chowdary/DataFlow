"""Time a real PostgreSQL→MongoDB identity append through the production stream engine.

Seeds the named 10-column employee fixture on PostgreSQL
(`bench_emp_1000000`) when missing. Fails closed unless Mongo dest
count_documents({}) equals the source with rejected_rows=0. Empty dest
is insert_many, not upsert. Not mongoimport. _id is not invented.

    cd apps/api && python scripts/bench_pg_to_mongo_million.py

    BENCH_ROWS=1000000 BENCH_SRC=bench_emp_1000000 BENCH_DEST=bench_pg_mongo \\
      python scripts/bench_pg_to_mongo_million.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from services.million_row_bench import run_pg_mongo_volume  # noqa: E402


if __name__ == "__main__":
    rows = int(os.environ.get("BENCH_ROWS", "1000000"))
    src = os.environ.get("BENCH_SRC", f"bench_emp_{rows}")
    dest = os.environ.get("BENCH_DEST", "bench_pg_mongo")
    proof = os.environ.get("BENCH_PROOF")
    if not proof:
        proof = str(Path("/opt/cursor/artifacts") / f"pg_mongo_{rows}_proof.json")
    run_pg_mongo_volume(
        rows=rows,
        dest_table=dest,
        source_table=src,
        sync_mode=os.environ.get("BENCH_SYNC", "full_refresh_append"),
        keep_dest=os.environ.get("BENCH_KEEP_DEST") == "1",
        fail_closed=True,
        proof_path=proof,
    )
