#!/usr/bin/env python3
"""Build the route certification matrix from a pytest JUnit XML run.

    python scripts/route_certification_report.py --junit /tmp/pytest-junit.xml

Writes ``data/proofs/route_certification_matrix.json``. Every PRODUCTION_SKU
route × assessed sync mode appears; cells without evidence stay
``unverified``. Exits 1 when any cell failed, so CI cannot publish a failing
matrix as green.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def main(argv: list[str] | None = None) -> int:
    from services.route_certification import build_route_matrix, parse_junit

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--junit", required=True, nargs="+", help="pytest --junitxml file(s)")
    parser.add_argument(
        "--out",
        default=str(_API_ROOT / "data" / "proofs" / "route_certification_matrix.json"),
    )
    args = parser.parse_args(argv)

    tests = []
    for path in args.junit:
        tests.extend(parse_junit(path))
    matrix = build_route_matrix(tests, source=", ".join(args.junit))
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(matrix, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    s = matrix["summary"]
    by = s["cells_by_status"]
    print(
        f"Wrote {out}: {s['cells']} cells — "
        + ", ".join(f"{k} {v}" for k, v in by.items())
        + f"; tests passed {s['tests_passed']}, failed {s['tests_failed']}, "
        f"skipped {s['tests_skipped']}"
    )
    return 1 if by.get("failed") else 0


if __name__ == "__main__":
    raise SystemExit(main())
