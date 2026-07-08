"""Preflight dry-run must validate the same transforms used by writers."""

from __future__ import annotations

import importlib.util
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]


def _load_preflight_service():
    path = _API_ROOT / "src" / "services" / "preflight_service.py"
    spec = importlib.util.spec_from_file_location("preflight_service_mod_unit", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def test_preflight_blocks_bad_transform_sample():
    service = _load_preflight_service()
    result = service.run_file_preflight(
        columns=["AMT"],
        column_types={"AMT": "DECIMAL"},
        row_count=1,
        mappings=[
            {
                "source": "AMT",
                "target": "payment_amount",
                "confidence": 0.98,
                "transform": "decimal",
            }
        ],
        destination_connected=True,
        sample_rows=[{"AMT": "not-a-number"}],
        estimated_bytes=128,
    )
    blockers = {b["id"]: b for b in result["blockers"]}
    assert "g5_dry_run" in blockers
    assert "Invalid decimal" in str(blockers["g5_dry_run"]["details"])
