#!/usr/bin/env python3
"""List the product's old- and new-named artifacts in one destination (read-only).

    python scripts/brand_artifacts_dry_run.py --type postgresql \
        --config '{"host": "db", "port": 5432, "database": "app", "username": "u",
                   "password": "...", "schema": "public"}'

Prints JSON. Never renames, drops or alters anything (services.brand_artifacts).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--type", required=True, help="destination type, e.g. postgresql, mysql, snowflake")
    parser.add_argument("--config", required=True, help="connector config as JSON (or @file.json)")
    args = parser.parse_args(argv)
    raw = args.config
    if raw.startswith("@"):
        raw = Path(raw[1:]).read_text(encoding="utf-8")
    from services.brand_artifacts import scan_destination

    print(json.dumps(scan_destination(args.type, json.loads(raw)), indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
