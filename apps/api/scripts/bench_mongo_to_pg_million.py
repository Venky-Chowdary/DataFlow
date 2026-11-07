"""Time a real MongoDB→PostgreSQL identity append through the production stream engine.

Uses Mongo collection `bench_pg_mongo` when present; otherwise seeds it
from PostgreSQL `bench_emp_1000000`. Fails closed unless dest COUNT(*)
equals the Mongo snapshot count_documents with rejected_rows=0. Source
COUNT is never estimatedDocumentCount. Not mongoexport.

    cd apps/api && python scripts/bench_mongo_to_pg_million.py

    BENCH_ROWS=1000000 BENCH_SRC=bench_pg_mongo BENCH_DEST=bench_pg_from_mongo \\
      python scripts/bench_mongo_to_pg_million.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from services.million_row_bench import run_mongo_pg_volume  # noqa: E402


if __name__ == "__main__":
    rows = int(os.environ.get("BENCH_ROWS", "1000000"))
    src = os.environ.get("BENCH_SRC", "bench_pg_mongo")
    dest = os.environ.get("BENCH_DEST", "bench_pg_from_mongo")
    proof = os.environ.get("BENCH_PROOF")
    if not proof:
        proof = str(Path("/opt/cursor/artifacts") / f"mongo_pg_{rows}_proof.json")
    run_mongo_pg_volume(
        rows=rows,
        dest_table=dest,
        source_table=src,
        sync_mode=os.environ.get("BENCH_SYNC", "full_refresh_append"),
        keep_dest=os.environ.get("BENCH_KEEP_DEST") == "1",
        fail_closed=True,
        proof_path=proof,
    )
