"""Run PRODUCTION_SKU execute matrix on this desktop and persist honesty counts.

Emulator greens (fake-gcs, Azurite, goccy BQ, fakesnow) are not a customer-tenant
warehouse claim. Catalog tiles stay not transfer-live.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def run_production_sku_emulator_honesty(*, timeout_s: int = 1800) -> dict[str, Any]:
    api_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        [
            str(api_root),
            str(api_root / "src"),
            str(api_root.parent.parent / "packages" / "preflight" / "src"),
            env.get("PYTHONPATH", ""),
        ]
    )
    proc = subprocess.run(
        [
            sys.executable, "-m", "pytest",
            "tests/test_production_sku_matrix.py",
            "tests/test_production_sku_honesty.py",
            "-q", "--tb=line",
            # Emulator Google/BQ retries nanosleep past operator patience.
            # Per-test 90s is fail-closed, not invented green.
            "--timeout=90",
        ],
        cwd=str(api_root),
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout_s,
    )
    out = (proc.stdout or "") + "\n" + (proc.stderr or "")
    passed = failed = skipped = 0
    for token in out.replace(",", " ").split():
        if token.endswith("passed"):
            try:
                passed = int(token[:-6])
            except ValueError:
                pass
        elif token.endswith("failed"):
            try:
                failed = int(token[:-6])
            except ValueError:
                pass
        elif token.endswith("skipped"):
            try:
                skipped = int(token[:-7])
            except ValueError:
                pass
    payload = {
        "fixture": "tests.production_sku_emulator.run_production_sku_emulator_honesty",
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "exit_code": proc.returncode,
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "customer_tenant_warehouse_claimed": False,
        "catalog_tiles_are_not_transfer_live": True,
        "emulator_not_customer_tenant": True,
        "note": (
            "Execute matrix on this desktop (Postgres/MySQL/SQLite/Mongo + "
            "emulators). Not a customer-tenant Snowflake/BigQuery PRODUCTION_SKU."
        ),
        "tail": out[-2500:],
    }
    text = json.dumps(payload, indent=2) + "\n"
    proofs = api_root / "data" / "proofs"
    proofs.mkdir(parents=True, exist_ok=True)
    (proofs / "production_sku_emulator.json").write_text(text)
    artifacts = Path("/opt/cursor/artifacts")
    if artifacts.is_dir():
        (artifacts / "production_sku_emulator.json").write_text(text)
        lab = artifacts / "warehouse-emulator-lab"
        lab.mkdir(parents=True, exist_ok=True)
        (lab / "production_sku_emulator.json").write_text(text)
    return payload


if __name__ == "__main__":
    print(json.dumps(run_production_sku_emulator_honesty(), indent=2))
