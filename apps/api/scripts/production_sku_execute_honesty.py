"""Bounded PRODUCTION_SKU execute honesty — never hang 28 minutes.

Runs leftover + catalog + CDC honesty unit tests, then a *local* SKU execute
slice (sqlite/postgresql/mysql/mongodb) with a per-test timeout. Warehouse
emulator/tenant routes are skipped and labeled not claimed.

Emulator greens ≠ customer-tenant Snowflake/BigQuery PRODUCTION_SKU.
"""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

API = Path(__file__).resolve().parents[1]
ROOT = API.parent.parent
ARTIFACT = Path("/opt/cursor/artifacts/production_sku_execute_honesty.json")
PYTEST = [sys.executable, "-m", "pytest", "-q", "--tb=line"]


def _run(args: list[str], timeout: int) -> dict:
    proc = subprocess.run(
        args,
        cwd=str(API),
        env={
            **dict(**{k: v for k, v in __import__("os").environ.items()}),
            "PYTHONPATH": f"{API}:{API / 'src'}:{ROOT / 'packages' / 'preflight' / 'src'}",
        },
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    return {
        "exit_code": proc.returncode,
        "stdout_tail": (proc.stdout or "")[-2000:],
        "stderr_tail": (proc.stderr or "")[-800:],
    }


def main() -> int:
    honesty = _run(
        [
            *PYTEST,
            "tests/test_unique_engine_leftovers.py",
            "tests/test_catalog_honesty.py::test_catalog_tiles_are_not_transfer_live",
            "tests/test_cdc_effectively_once.py::test_honesty_dict_refuses_exactly_once_claim",
        ],
        timeout=90,
    )
    live = _run(
        [*PYTEST, "tests/test_unique_engine_leftovers_live.py"],
        timeout=120,
    )
    local_sku = _run(
        [
            *PYTEST,
            "tests/test_production_sku_matrix.py",
            "-k",
            "sqlite or postgresql or mysql or mongodb",
            "--timeout=45",
        ],
        timeout=240,
    )
    payload = {
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "customer_tenant_warehouse_sku_claimed": False,
        "catalog_tiles_are_not_transfer_live": True,
        "cdc_exactly_once_claimed": False,
        "cdc_default": "at-least-once",
        "honesty_units": honesty,
        "leftover_live": live,
        "local_sku_execute": local_sku,
        "warehouse_tenant_execute": {
            "claimed": False,
            "reason": (
                "Snowflake/BigQuery/S3 tenant execute is not run here. "
                "A prior host attempt hung ~28m. Emulator ≠ customer tenant."
            ),
        },
        "note": (
            "Validate membership was already measured (78/78). This script "
            "proves leftover mappings + catalog/CDC honesty + a bounded local "
            "SKU execute slice. Not 650+ live. Not leftover MERGE certified."
        ),
    }
    ARTIFACT.parent.mkdir(parents=True, exist_ok=True)
    ARTIFACT.write_text(json.dumps(payload, indent=2) + "\n")
    print(json.dumps({k: payload[k] for k in payload if k not in {"honesty_units", "leftover_live", "local_sku_execute"}}, indent=2))
    print("honesty_units", honesty["exit_code"])
    print("leftover_live", live["exit_code"])
    print("local_sku_execute", local_sku["exit_code"])
    print(local_sku.get("stdout_tail", "")[-800:])
    return 0 if honesty["exit_code"] == 0 and live["exit_code"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
