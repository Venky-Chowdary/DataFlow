"""Time a real Kafka→Kafka identity append through the production stream engine.

Seeds desktop-lab Kafka `bench_kafka_src` when missing. Fails closed unless
dest watermark COUNT equals the source COUNT with rejected_rows=0.
Empty dest is consume+produce of raw bytes, not JSON decode /
`kafka_json_payload` / MirrorMaker 2 / Cluster Linking. Same
bootstrap+topic declines. Dest `bench_kafka_clone` is unique (not reused
from `bench_1m` / `bench_redis_clone` / `bench_es_clone`). Desktop-lab
Kafka on :9092 is not a customer-tenant PRODUCTION_SKU. Named 1M is not
invented on this host.

    cd apps/api && python scripts/bench_kafka_to_kafka_million.py

    BENCH_ROWS=1000000 BENCH_SRC=bench_kafka_src BENCH_DEST=bench_kafka_clone \\
      python scripts/bench_kafka_to_kafka_million.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from services.million_row_bench import run_kafka_kafka_volume  # noqa: E402


if __name__ == "__main__":
    rows = int(os.environ.get("BENCH_ROWS", "1000000"))
    src = os.environ.get("BENCH_SRC", "bench_kafka_src")
    dest = os.environ.get("BENCH_DEST", "bench_kafka_clone")
    proof = os.environ.get("BENCH_PROOF")
    if not proof:
        proof = str(Path("/opt/cursor/artifacts") / f"kafka_kafka_{rows}_proof.json")
    run_kafka_kafka_volume(
        rows=rows,
        dest_table=dest,
        source_table=src,
        sync_mode=os.environ.get("BENCH_SYNC", "full_refresh_append"),
        keep_dest=os.environ.get("BENCH_KEEP_DEST") == "1",
        fail_closed=True,
        proof_path=proof,
    )
