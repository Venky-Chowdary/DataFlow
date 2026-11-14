#!/usr/bin/env python3
"""Phase F7 — publish machine-readable connector capability matrix for live engines."""

from __future__ import annotations

import json
import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def _ci_evidence(path: str | None) -> dict:
    """Engine -> CI evidence. Without a CI evidence file nothing is live-verified."""
    from services.connector_ci_evidence import evidence_document, load_manifest

    if path:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    return evidence_document(load_manifest(), [])


def main(argv: list[str] | None = None) -> int:
    import argparse

    from services.connector_capability_registry import export_live_capability_matrix

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--ci-evidence",
        help="connector_ci_evidence.json from the CI evidence job (A4). Without it every "
        "engine is reported as not live-verified.",
    )
    args = parser.parse_args(argv)

    matrix = export_live_capability_matrix()
    # "Live-verified" comes from CI results only (scripts/check_live_skips.py
    # via connector_ci_evidence), never from what the workflows configure.
    evidence = _ci_evidence(args.ci_evidence)
    known = evidence.get("engines") or {}
    for engine in matrix["engines"]:
        ev = known.get(engine["engine_id"]) or {"status": "no_ci_job", "live_verified": False}
        engine["ci_status"] = ev["status"]
        engine["live_verified"] = bool(ev.get("live_verified"))
    matrix["live_verified_engines"] = sorted(
        e["engine_id"] for e in matrix["engines"] if e["live_verified"]
    )
    matrix["ci_evidence_source"] = args.ci_evidence or "none (no CI results supplied)"
    dest = _API_ROOT / "data" / "proofs" / "connector_capability_matrix.json"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(matrix, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({
        "unique_driver_count": matrix["unique_driver_count"],
        "live_verified_engines": matrix["live_verified_engines"],
        "missing_static_registry": matrix["missing_static_registry"],
        "path": str(dest),
    }, indent=2))
    if matrix["missing_static_registry"]:
        print("ERROR: live drivers missing static CAPABILITY_REGISTRY entries", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
