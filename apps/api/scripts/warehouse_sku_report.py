#!/usr/bin/env python3
"""Run warehouse SKU proof pytest slice and write an honest pass/fail/skip artifact.

Phase B9 — CI must always produce this JSON (never silently omit when earlier
gates fail). Skip reasons are recorded when optional services/creds are absent.
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

# Keep in sync with .github/workflows/ci.yml warehouse SKU step.
WAREHOUSE_TARGETS = [
    "tests/test_production_sku_matrix.py",
    "tests/test_execute_tracked_csv_to_bigquery.py",
    "tests/test_execute_tracked_csv_to_bigquery_upsert.py",
    "tests/test_execute_tracked_csv_to_snowflake_upsert.py",
]


def parse_pytest_summary(output: str) -> tuple[int, int, int, str]:
    """Parse pytest -q summary into (passed, failed, skipped, summary_line)."""
    import re

    passed = failed = skipped = errors = 0
    summary_line = ""
    for line in reversed((output or "").splitlines()):
        if re.search(r"\d+\s+(passed|failed|skipped|error)", line):
            summary_line = line.strip()
            break
    for kind, attr in (
        ("failed", "failed"),
        ("passed", "passed"),
        ("skipped", "skipped"),
        ("error", "errors"),
        ("errors", "errors"),
    ):
        mm = re.search(rf"(\d+)\s+{kind}\b", summary_line)
        if not mm:
            continue
        n = int(mm.group(1))
        if attr == "failed":
            failed = n
        elif attr == "passed":
            passed = n
        elif attr == "skipped":
            skipped = n
        else:
            errors = n
    failed += errors
    return passed, failed, skipped, summary_line


def _skip_reasons() -> list[str]:
    reasons: list[str] = []
    # SaaS reverse-ETL is intentionally out of this proof unless creds exist.
    if not (os.environ.get("SALESFORCE_ACCESS_TOKEN") or "").strip():
        reasons.append("Salesforce reverse-ETL skipped — no SALESFORCE_ACCESS_TOKEN")
    if not (os.environ.get("HUBSPOT_ACCESS_TOKEN") or "").strip():
        reasons.append("HubSpot reverse-ETL skipped — no HUBSPOT_ACCESS_TOKEN")
    # fakesnow / BQ emulator are expected in CI; note when absent for local runs.
    try:
        import fakesnow  # noqa: F401
    except Exception:
        reasons.append("fakesnow not installed — Snowflake SKU tests may skip/fail")
    return reasons


def main() -> int:
    targets = [t for t in WAREHOUSE_TARGETS if (ROOT / t).is_file()]
    missing = [t for t in WAREHOUSE_TARGETS if not (ROOT / t).is_file()]
    cmd = [
        sys.executable,
        "-m",
        "pytest",
        *targets,
        "-k",
        "snowflake or bigquery",
        "-q",
        "--tb=short",
        "--durations=0",
    ]
    env = os.environ.copy()
    env.setdefault("DATAFLOW_JOB_STORE", "memory")
    env.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    env.setdefault("DATAFLOW_ALLOW_STUB_WRITES", "1")
    env.setdefault("DATAFLOW_FAKESNOW_KEEP_PATCH", "1")
    proc = subprocess.run(
        cmd,
        cwd=str(ROOT),
        env=env,
        capture_output=True,
        text=True,
    )
    out = (proc.stdout or "") + (proc.stderr or "")
    passed, failed, skipped, summary_line = parse_pytest_summary(out)
    skip_reasons = _skip_reasons()
    if missing:
        skip_reasons.append(f"missing test modules: {', '.join(missing)}")

    proof = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "suite": "warehouse_sku",
        "targets": targets,
        "missing_targets": missing,
        "pytest_filter": "snowflake or bigquery",
        "exit_code": proc.returncode,
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "summary_line": summary_line,
        "skip_reasons": skip_reasons,
        "honesty": {
            "snowflake_path": "fakesnow (not live Snowflake account)",
            "bigquery_path": "goccy emulator when available; else skip",
            "saas_reverse_etl": "skipped without credentials — never counted as pass",
            "notes": [
                "This artifact proves warehouse SKU pytest slice outcomes only.",
                "Skip ≠ pass. Failed collection is counted as failed.",
            ],
        },
    }
    path = PROOF_DIR / "warehouse_sku_report.json"
    path.write_text(json.dumps(proof, indent=2) + "\n", encoding="utf-8")
    # Human-readable twin for tee/grep consumers.
    txt = PROOF_DIR / "warehouse_sku_report.txt"
    txt.write_text(out + "\n" + summary_line + "\n", encoding="utf-8")
    print(json.dumps(proof, indent=2))
    if proc.returncode != 0:
        print(out[-4000:], file=sys.stderr)
    return 0 if proc.returncode == 0 else proc.returncode


if __name__ == "__main__":
    raise SystemExit(main())
