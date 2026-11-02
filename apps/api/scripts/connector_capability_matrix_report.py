#!/usr/bin/env python3
"""Phase F7 — publish machine-readable connector capability matrix for live engines."""

from __future__ import annotations

import json
import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def main() -> int:
    from services.connector_capability_registry import export_live_capability_matrix

    matrix = export_live_capability_matrix()
    dest = _API_ROOT / "data" / "proofs" / "connector_capability_matrix.json"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(matrix, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({
        "unique_driver_count": matrix["unique_driver_count"],
        "missing_static_registry": matrix["missing_static_registry"],
        "path": str(dest),
    }, indent=2))
    if matrix["missing_static_registry"]:
        print("ERROR: live drivers missing static CAPABILITY_REGISTRY entries", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
