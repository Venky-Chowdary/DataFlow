"""Time a real Elasticsearch→Elasticsearch identity append through the stream engine.

Seeds desktop-lab Elasticsearch `bench_es_src` when missing. Fails closed
unless dest `_count` equals the source COUNT with rejected_rows=0.
Empty dest is cluster `_reindex`, not scroll+bulk / helpers.reindex /
`_cat/indices` docs.count. Same host+port+index declines. Dest
`bench_es_clone` is unique (not reused from `bench_1m` /
`bench_redis_clone` / `bench_gcs_clone.jsonl`).

    cd apps/api && python scripts/bench_elasticsearch_to_elasticsearch_million.py

    BENCH_ROWS=1000000 BENCH_SRC=bench_es_src BENCH_DEST=bench_es_clone \\
      python scripts/bench_elasticsearch_to_elasticsearch_million.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from services.million_row_bench import run_elasticsearch_elasticsearch_volume  # noqa: E402


if __name__ == "__main__":
    rows = int(os.environ.get("BENCH_ROWS", "1000000"))
    src = os.environ.get("BENCH_SRC", "bench_es_src")
    dest = os.environ.get("BENCH_DEST", "bench_es_clone")
    proof = os.environ.get("BENCH_PROOF")
    if not proof:
        proof = str(
            Path("/opt/cursor/artifacts") / f"elasticsearch_elasticsearch_{rows}_proof.json"
        )
    run_elasticsearch_elasticsearch_volume(
        rows=rows,
        dest_table=dest,
        source_table=src,
        sync_mode=os.environ.get("BENCH_SYNC", "full_refresh_append"),
        keep_dest=os.environ.get("BENCH_KEEP_DEST") == "1",
        fail_closed=True,
        proof_path=proof,
    )
