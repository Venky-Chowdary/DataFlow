#!/usr/bin/env python3
"""Regenerate the mapping evidence catalog (buyer-facing proof pack).

Usage (from repo root)::

    PYTHONPATH=apps/api:packages/preflight/src python apps/api/scripts/run_evidence_pack.py

Or from apps/api with preflight installed editable::

    python scripts/run_evidence_pack.py --refresh-pairs
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = API_ROOT.parents[1]
PREFLIGHT_SRC = REPO_ROOT / "packages" / "preflight" / "src"

for path in (API_ROOT, PREFLIGHT_SRC):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))


def main() -> int:
    parser = argparse.ArgumentParser(description="Build DataFlow mapping evidence catalog")
    parser.add_argument(
        "--refresh-pairs",
        action="store_true",
        help="Materialize missing N×M connector pair proof JSON files",
    )
    parser.add_argument(
        "--fail-on-miss",
        action="store_true",
        help="Exit 1 when any claim floor is missed (CI mode)",
    )
    args = parser.parse_args()

    from services.evidence_pack import build_evidence_catalog, write_evidence_catalog

    catalog = build_evidence_catalog(refresh_pairs=args.refresh_pairs)
    json_path, md_path = write_evidence_catalog(catalog)
    print(f"Wrote {json_path}")
    print(f"Wrote {md_path}")
    print(
        f"Result: {'PASS' if catalog['all_passed'] else 'FAIL'} "
        f"({catalog['passed_count']}/{catalog['claim_count']} claims)"
    )
    for claim in catalog["claims"]:
        mark = "OK" if claim["passed"] else "MISS"
        print(f"  [{mark}] {claim['id']}")
        if not claim["passed"]:
            for check in claim["checks"]:
                if not check["passed"]:
                    print(
                        f"       {check['metric']}: measured={check['measured']} "
                        f"floor={check['floor']}"
                    )
    if args.fail_on_miss and not catalog["all_passed"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
