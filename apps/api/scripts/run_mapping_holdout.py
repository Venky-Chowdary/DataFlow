#!/usr/bin/env python3
"""Score the mapper on the held-out benchmark (tests/fixtures/mapping_holdout_v1.json).

Run from apps/api::

    PYTHONPATH=.:../../packages/preflight/src python scripts/run_mapping_holdout.py --split test

``--split dev`` is the only split to inspect row by row (``--rows``) while
tuning; ``test`` is for before/after numbers. ``--out`` writes the full result.
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
    ap.add_argument("--split", default="test", choices=["dev", "test", "all"])
    ap.add_argument("--names-only", action="store_true")
    ap.add_argument("--rows", action="store_true", help="print misses (dev only)")
    ap.add_argument("--out")
    a = ap.parse_args()
    logging.disable(logging.WARNING)
    from services.mapping_holdout_eval import evaluate

    res = evaluate(a.split, names_only=a.names_only)
    if a.out:
        Path(a.out).write_text(json.dumps(res, indent=1, ensure_ascii=False), encoding="utf-8")
    print(json.dumps({"split": res["split"], "mode": res["mode"], "overall": res["overall"]}, indent=1))
    for b, m in res["buckets"].items():
        print(
            f"{b:22s} n={m['n_matchable']:3d}/{m['n_negative']:2d}neg top1={m['top1']} top3={m['top3']} "
            f"P={m['proposal_precision']} autoP={m['auto_precision']} cov={m['auto_coverage']} "
            f"wrongAuto={m['wrong_auto']} negProp={m['neg_proposed']} negAuto={m['neg_auto']} "
            f"abstain={m['abstain_on_matchable']}"
        )
    if a.rows:
        if a.split != "dev":
            print("--rows is for the dev split only (never tune on test)", file=sys.stderr)
            return 2
        for r in res["rows"]:
            if r["pred"] != r["gold"]:
                print(
                    f"  {r['table']:28s} {r['source']:22s} gold={r['gold']!s:24s} pred={r['pred']!s:24s} "
                    f"raw={r['raw_target']!s:20s} conf={r['conf']:.3f} review={r['review']} "
                    f"auto={r['auto']} strat={r['strategy']} ranked={r['ranked'][:3]}"
                )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
