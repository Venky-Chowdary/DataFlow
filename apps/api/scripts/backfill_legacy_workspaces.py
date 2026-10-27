"""Assign legacy unscoped resources to their owner's workspace (D154).

Dry run by default — prints what would be assigned per collection and writes
nothing. ``--apply`` performs the assignment; rerunning is a no-op because
only rows with no workspace are touched. Run it before enabling self-serve
signup (which enforces workspace isolation) so owners keep their history.

    python scripts/backfill_legacy_workspaces.py               # dry run
    python scripts/backfill_legacy_workspaces.py --apply
    python scripts/backfill_legacy_workspaces.py --apply --owner-email ops@example.com
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

API = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(API))

from services.legacy_workspace_backfill import backfill_legacy_workspaces  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", default=True, help="report only (default)")
    mode.add_argument("--apply", action="store_true", help="write the assignments")
    parser.add_argument(
        "--owner-email",
        default="",
        help="platform owner for rows with no owner on record (default: the bootstrap admin)",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        report = backfill_legacy_workspaces(apply=args.apply, owner_email=args.owner_email)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(report.to_dict(), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
