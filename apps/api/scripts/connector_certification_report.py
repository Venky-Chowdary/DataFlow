from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(API_ROOT))

from connectors.sdk import list_descriptors
from tests.connector_certification.cases import build_certification_cases
from tests.connector_certification.harness import certify


def main() -> int:
    parser = argparse.ArgumentParser(description="Run synthetic SDK connector certification.")
    parser.add_argument(
        "--write-report",
        action="store_true",
        help="write the JSON report to data/proofs/connector_certification.json",
    )
    args = parser.parse_args()

    with tempfile.TemporaryDirectory(prefix="connector-certification-") as directory:
        cases = build_certification_cases(Path(directory))
        descriptors = list_descriptors()
        if set(cases) != {descriptor.id for descriptor in descriptors}:
            print("certification cases do not match the SDK descriptor registry", file=sys.stderr)
            return 2
        reports = [
            certify(cases[descriptor.id]).to_dict()
            for descriptor in descriptors
        ]
    report = {"evidence": "synthetic-fixture", "connectors": reports}
    rendered = json.dumps(report, indent=2, sort_keys=True)
    print(rendered)
    if args.write_report:
        output = API_ROOT / "data/proofs/connector_certification.json"
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(rendered + "\n", encoding="utf-8")
    return int(any(step["status"] == "fail" for connector in reports for step in connector["steps"]))


if __name__ == "__main__":
    raise SystemExit(main())
