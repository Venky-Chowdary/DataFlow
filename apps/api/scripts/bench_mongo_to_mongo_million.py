"""Time a real MongoDB→MongoDB identity append through the production stream engine.

Uses Mongo collection `bench_mysql_mongo` when present; otherwise seeds it
from MySQL `bench_1m`. Fails closed unless dest count_documents equals the
source snapshot count_documents with rejected_rows=0. Source COUNT is
never estimatedDocumentCount. Not mongoexport / mongoimport / $out.
Empty dest is insert_many. Same collection is refused.

    cd apps/api && python scripts/bench_mongo_to_mongo_million.py

    BENCH_ROWS=1000000 BENCH_SRC=bench_mysql_mongo BENCH_DEST=bench_mongo_mongo \\
      python scripts/bench_mongo_to_mongo_million.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from services.million_row_bench import run_mongo_mongo_volume  # noqa: E402


if __name__ == "__main__":
    rows = int(os.environ.get("BENCH_ROWS", "1000000"))
    src = os.environ.get("BENCH_SRC", "bench_mysql_mongo")
    dest = os.environ.get("BENCH_DEST", "bench_mongo_mongo")
    proof = os.environ.get("BENCH_PROOF")
    if not proof:
        proof = str(Path("/opt/cursor/artifacts") / f"mongo_mongo_{rows}_proof.json")
    run_mongo_mongo_volume(
        rows=rows,
        dest_table=dest,
        source_table=src,
        sync_mode=os.environ.get("BENCH_SYNC", "full_refresh_append"),
        keep_dest=os.environ.get("BENCH_KEEP_DEST") == "1",
        fail_closed=True,
        proof_path=proof,
    )
