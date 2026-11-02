"""Time a real DuckDB→DuckDB identity append through the production stream engine.

Seeds `bench_duckdb_src` when missing. Fails closed unless dest
`COUNT(*)` equals the source COUNT with rejected_rows=0. Empty dest is
`ATTACH … (READ_ONLY)` + `INSERT SELECT`, not `EXPORT DATABASE` /
`read_parquet` staging / a pandas round trip. Dest DDL is the source
catalog's types and keys, not widened mapping stamps. Same file + table
declines. `:memory:` and MotherDuck decline. Dest `bench_duckdb_clone` is
unique (not reused from `bench_1m` / `bench_kafka_clone` /
`bench_redis_clone`).

    cd apps/api && python scripts/bench_duckdb_to_duckdb_million.py

    BENCH_ROWS=1000000 BENCH_SRC=bench_duckdb_src BENCH_DEST=bench_duckdb_clone \\
      python scripts/bench_duckdb_to_duckdb_million.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from services.million_row_bench import run_duckdb_duckdb_volume  # noqa: E402


if __name__ == "__main__":
    rows = int(os.environ.get("BENCH_ROWS", "1000000"))
    src = os.environ.get("BENCH_SRC", "bench_duckdb_src")
    dest = os.environ.get("BENCH_DEST", "bench_duckdb_clone")
    proof = os.environ.get("BENCH_PROOF")
    if not proof:
        proof = str(Path("/opt/cursor/artifacts") / f"duckdb_duckdb_{rows}_proof.json")
    run_duckdb_duckdb_volume(
        rows=rows,
        dest_table=dest,
        source_table=src,
        source_path=os.environ.get("BENCH_DUCKDB_SRC_PATH"),
        dest_path=os.environ.get("BENCH_DUCKDB_DEST_PATH"),
        sync_mode=os.environ.get("BENCH_SYNC", "full_refresh_append"),
        keep_dest=os.environ.get("BENCH_KEEP_DEST") == "1",
        fail_closed=True,
        proof_path=proof,
    )
