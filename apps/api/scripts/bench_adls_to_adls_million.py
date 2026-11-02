"""Time a real ADLS→ADLS identity append through the production stream engine.

Seeds Azurite `bench_adls_src.jsonl` when missing. Fails closed unless
dest artifact COUNT equals the source COUNT with rejected_rows=0.
Empty dest is server-side start_copy_from_url, not GET+PUT / `azcopy`.
Same endpoint+container+blob declines. Dest `bench_adls_clone.jsonl` is
unique (not reused from `bench_gcs_clone.jsonl` / `bench_s3_clone.csv` /
`bench_1m`). Azurite is not a customer-tenant PRODUCTION_SKU.

    cd apps/api && python scripts/bench_adls_to_adls_million.py

    BENCH_ROWS=1000000 BENCH_SRC=bench_adls_src.jsonl BENCH_DEST=bench_adls_clone.jsonl \\
      python scripts/bench_adls_to_adls_million.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from services.million_row_bench import run_adls_adls_volume  # noqa: E402


if __name__ == "__main__":
    rows = int(os.environ.get("BENCH_ROWS", "1000000"))
    src = os.environ.get("BENCH_SRC", "bench_adls_src.jsonl")
    dest = os.environ.get("BENCH_DEST", "bench_adls_clone.jsonl")
    proof = os.environ.get("BENCH_PROOF")
    if not proof:
        proof = str(Path("/opt/cursor/artifacts") / f"adls_adls_{rows}_proof.json")
    run_adls_adls_volume(
        rows=rows,
        dest_table=dest,
        source_table=src,
        dest_container=os.environ.get("BENCH_ADLS_CONTAINER"),
        source_container=os.environ.get("BENCH_ADLS_SRC_CONTAINER"),
        sync_mode=os.environ.get("BENCH_SYNC", "full_refresh_append"),
        keep_dest=os.environ.get("BENCH_KEEP_DEST") == "1",
        fail_closed=True,
        proof_path=proof,
    )
