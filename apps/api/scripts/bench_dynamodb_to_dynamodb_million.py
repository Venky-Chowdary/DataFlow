"""Time a real DynamoDB→DynamoDB identity append through the production stream.

Seeds DynamoDB Local `bench_dynamodb_src` when missing. Fails closed unless
dest Scan COUNT equals the source COUNT with rejected_rows=0. Dest COUNT
is never DescribeTable.ItemCount / ListTables length / write ack. Empty
dest is BatchWriteItem of raw AttributeValues, not export-table /
ImportTable / PutItem one-by-one. Same endpoint+table declines. Dest
`bench_dynamodb_clone` is unique (not reused from `bench_1m` /
`bench_mongo_mongo`). DynamoDB Local is not a customer-tenant
PRODUCTION_SKU.

    cd apps/api && python scripts/bench_dynamodb_to_dynamodb_million.py

    BENCH_ROWS=1000000 BENCH_SRC=bench_dynamodb_src BENCH_DEST=bench_dynamodb_clone \\
      python scripts/bench_dynamodb_to_dynamodb_million.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from services.million_row_bench import run_dynamodb_dynamodb_volume  # noqa: E402


if __name__ == "__main__":
    rows = int(os.environ.get("BENCH_ROWS", "1000000"))
    src = os.environ.get("BENCH_SRC", "bench_dynamodb_src")
    dest = os.environ.get("BENCH_DEST", "bench_dynamodb_clone")
    proof = os.environ.get("BENCH_PROOF")
    if not proof:
        proof = str(Path("/opt/cursor/artifacts") / f"dynamodb_dynamodb_{rows}_proof.json")
    run_dynamodb_dynamodb_volume(
        rows=rows,
        dest_table=dest,
        source_table=src,
        sync_mode=os.environ.get("BENCH_SYNC", "full_refresh_append"),
        keep_dest=os.environ.get("BENCH_KEEP_DEST") == "1",
        fail_closed=True,
        proof_path=proof,
    )
