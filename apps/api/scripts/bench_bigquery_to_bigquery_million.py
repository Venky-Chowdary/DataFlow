"""Time a real BigQuery→BigQuery identity append through the production stream engine.

Seeds goccy `bench_bq_src` when missing. Fails closed unless dest
COUNT(*) equals the source COUNT with rejected_rows=0.
Empty dest is CTAS / INSERT SELECT of mapped columns, not
insert_rows_json / CLONE / leftover MERGE. Same
project+dataset+table declines. Dest `bench_bq_clone` is unique
(not reused from `bench_1m` / `bench_adls_clone.jsonl` /
`bench_snowflake_clone`). goccy is not a customer-tenant
PRODUCTION_SKU. Dest COUNT is COUNT(*), never Table.num_rows.

    cd apps/api && python scripts/bench_bigquery_to_bigquery_million.py

    BENCH_ROWS=1000000 BENCH_SRC=bench_bq_src BENCH_DEST=bench_bq_clone \\
      python scripts/bench_bigquery_to_bigquery_million.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from services.million_row_bench import run_bigquery_bigquery_volume  # noqa: E402


if __name__ == "__main__":
    rows = int(os.environ.get("BENCH_ROWS", "1000000"))
    src = os.environ.get("BENCH_SRC", "bench_bq_src")
    dest = os.environ.get("BENCH_DEST", "bench_bq_clone")
    proof = os.environ.get("BENCH_PROOF")
    if not proof:
        proof = str(Path("/opt/cursor/artifacts") / f"bigquery_bigquery_{rows}_proof.json")
    run_bigquery_bigquery_volume(
        rows=rows,
        dest_table=dest,
        source_table=src,
        sync_mode=os.environ.get("BENCH_SYNC", "full_refresh_append"),
        keep_dest=os.environ.get("BENCH_KEEP_DEST") == "1",
        fail_closed=True,
        proof_path=proof,
    )
