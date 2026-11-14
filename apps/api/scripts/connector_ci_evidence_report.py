#!/usr/bin/env python3
"""Merge live-engine CI results into ``connector_ci_evidence.json`` (A4).

Input: the per-job JSON files ``scripts/check_live_skips.py`` writes (CI
uploads them as ``live-engine-*`` artifacts). Output: one evidence record
per engine (``services/connector_ci_evidence.py``), the file the connector
capability page and ``connector_capability_matrix_report.py --ci-evidence``
read. An engine is ``live_verified`` only when a job that proves it ran
green in these results; workflow configuration alone never counts.

Standard library only (no API dependencies), so the CI evidence job runs it
on the runner's Python without installing requirements.

Usage::

    python apps/api/scripts/connector_ci_evidence_report.py \\
        --results "$RUNNER_TEMP/live-engine" --out connector_ci_evidence.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.connector_ci_evidence import (  # noqa: E402
    MANIFEST,
    evidence_document,
    load_manifest,
    load_results,
)


def summary_markdown(doc: dict) -> str:
    lines = [
        "### Connector CI evidence",
        f"- jobs reported: {', '.join(doc['jobs_reported']) or 'none'}",
        f"- live-verified: {', '.join(doc['live_verified']) or 'none'}",
        "",
        "| Engine | CI status | Proving jobs | Missing results |",
        "|---|---|---|---|",
    ]
    for engine, ev in doc["engines"].items():
        status = ev["status"] + (" (emulator)" if ev["emulated"] else "")
        lines.append(
            f"| `{engine}` | {status} | {', '.join(ev['proving_jobs']) or '-'} "
            f"| {', '.join(ev['missing_results']) or '-'} |"
        )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--results", action="append", default=[], help="result JSON or folder (repeatable)")
    parser.add_argument("--manifest", default=str(MANIFEST))
    parser.add_argument("--out", required=True)
    args = parser.parse_args(argv)

    doc = evidence_document(
        load_manifest(Path(args.manifest)),
        load_results(Path(p) for p in args.results),
    )
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
    text = summary_markdown(doc)
    print(text)
    step_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if step_summary:
        with open(step_summary, "a", encoding="utf-8") as fh:
            fh.write(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
