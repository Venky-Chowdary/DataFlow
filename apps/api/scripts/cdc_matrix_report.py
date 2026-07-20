#!/usr/bin/env python3
"""Run the CDC-focused pytest matrix and write an honest pass/fail/skip proof.

Reports counts only — never invents “100% CDC” or Airbyte parity claims.
SQL Server / Oracle live ITs remain env-gated skips until those services exist.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROOF_DIR = ROOT / "data" / "proofs"
PROOF_DIR.mkdir(parents=True, exist_ok=True)

CDC_TARGETS = [
    "tests/test_debezium_parity.py",
    "tests/test_cdc_transfer.py",
    "tests/test_cdc_engine.py",
    "tests/test_cdc_snapshot_lsn_handoff.py",
    "tests/test_cdc_snapshot_window.py",
    "tests/test_cdc_incremental_snapshot.py",
    "tests/test_cdc_lsn_stamp.py",
    "tests/test_cdc_schema_history.py",
    "tests/test_cdc_multistream_mappings.py",
    "tests/test_cdc_multi_table_reader.py",
    "tests/test_cdc_shared_ack_chaos.py",
    "tests/test_cdc_shared_reader_integration.py",
    "tests/test_cdc_effectively_once.py",
    "tests/test_cdc_distributed_lease.py",
    "tests/test_cdc_toast_and_txn_buffer.py",
    "tests/test_postgresql_change_stream.py",
    "tests/test_cdc_postgres_logical_integration.py",
    "tests/test_cdc_mysql_binlog_integration.py",
    "tests/test_cdc_mysql_mid_snapshot.py",
    "tests/test_cdc_mongodb_change_stream_integration.py",
    "tests/test_cdc_sqlserver_ct_integration.py",
    "tests/test_cdc_sqlserver_native_integration.py",
    "tests/test_sqlserver_native_cdc.py",
    "tests/test_sqlserver_oracle_cdc.py",
    "tests/test_cdc_oracle_integration.py",
]


def main() -> int:
    targets = [t for t in CDC_TARGETS if (ROOT / t).is_file()]
    missing = [t for t in CDC_TARGETS if t not in targets]
    cmd = [
        sys.executable,
        "-m",
        "pytest",
        *targets,
        "-q",
        "--tb=line",
    ]
    env = os.environ.copy()
    env.setdefault("DATAFLOW_JOB_STORE", "memory")
    proc = subprocess.run(
        cmd,
        cwd=str(ROOT),
        env=env,
        capture_output=True,
        text=True,
    )
    out = (proc.stdout or "") + (proc.stderr or "")
    import re

    passed = failed = skipped = 0
    summary_line = ""
    for line in reversed(out.splitlines()):
        if re.search(r"\d+\s+(passed|failed|skipped)", line):
            summary_line = line.strip()
            break
    for kind in ("failed", "passed", "skipped"):
        mm = re.search(rf"(\d+)\s+{kind}", summary_line)
        if not mm:
            continue
        n = int(mm.group(1))
        if kind == "failed":
            failed = n
        elif kind == "passed":
            passed = n
        else:
            skipped = n

    proof = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "suite": "cdc_matrix",
        "targets": targets,
        "missing_targets": missing,
        "exit_code": proc.returncode,
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "summary_line": summary_line,
        "honesty": {
            "delivery_default": "at-least-once upsert",
            "exactly_once_claimed": False,
            "effectively_once_pk_sinks": True,
            "airbyte_parity_claimed": False,
            "notes": [
                "PG/MySQL/Mongo live ITs run when services are up (CI enables PG wal_level=logical).",
                "SQL Server CDC CI proves Change Tracking *and* native CDC (capture + LSN).",
                "Oracle live IT is env-gated (DATAFLOW_ORACLE_ENABLE=1); optional cdc-oracle CI job.",
                "Leases: Redis multi-node (fail-closed) or file single-host; fencing generation on steal.",
                "PG TOAST merge + typed txn buffer overflow (no silent drop/wipe).",
                "Shared multi-table live IT + ack-barrier chaos are in matrix.",
                "_df_lsn effectively-once PK-sink proofs are not platform exactly-once.",
            ],
        },
    }
    path = PROOF_DIR / "cdc_matrix_report.json"
    path.write_text(json.dumps(proof, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(proof, indent=2))
    if proc.returncode != 0 and failed == 0:
        # Collection errors etc. — still fail the gate
        print(out[-4000:], file=sys.stderr)
    elif failed:
        print(out[-4000:], file=sys.stderr)
    return 0 if proc.returncode == 0 else proc.returncode


if __name__ == "__main__":
    raise SystemExit(main())
