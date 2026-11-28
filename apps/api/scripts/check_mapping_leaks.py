#!/usr/bin/env python3
"""Leak check for a labelled mapping fixture, counts only.

    PYTHONPATH=.:../../packages/preflight/src python scripts/check_mapping_leaks.py --fixture PATH

Polices every table of the fixture (or ``--split dev2`` of the default one) and
prints how many answers the mapper's knowledge already associates. It never
prints an answer unless ``--debug`` is given (never on a sealed blind set).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--fixture")
    ap.add_argument("--split", default="dev2")
    ap.add_argument("--debug", action="store_true")
    a = ap.parse_args()
    logging.disable(logging.WARNING)
    from services import mapping_holdout_leak as leak
    from services.mapping_holdout_eval import load_holdout

    doc = load_holdout(Path(a.fixture) if a.fixture else None)
    split = None if a.fixture else a.split
    print(json.dumps(leak.leak_summary(doc, split), indent=1))
    if a.debug:
        for row in leak.find_leaks(doc, split):
            print(row)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
