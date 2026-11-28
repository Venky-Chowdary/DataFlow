#!/usr/bin/env python3
"""Score Map's mapping on a labelled fixture (default tests/fixtures/mapping_holdout_v1.json).

Run from apps/api::

    PYTHONPATH=.:../../packages/preflight/src python scripts/run_mapping_holdout.py --split dev2
    PYTHONPATH=... python scripts/run_mapping_holdout.py --fixture /path/to/sealed.json

``--engine pipeline`` (default) is run_mapping_pipeline with use_llm=False, the
path Map uses; ``--engine scorer`` is map_columns alone (scorer-level).
Output is aggregates and 95% cluster-bootstrap intervals over tables only.
Per-row output needs ``--debug`` (never use it on a sealed blind set).
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
    ap.add_argument("--split", default="all", choices=["dev", "dev2", "all"])
    ap.add_argument("--fixture", help="score an external fixture (all its tables)")
    ap.add_argument("--engine", default="pipeline", choices=["pipeline", "scorer"])
    ap.add_argument("--names-only", action="store_true")
    ap.add_argument("--bootstrap", type=int, default=2000, help="cluster bootstrap reps (0 = off)")
    ap.add_argument("--debug", action="store_true", help="print per-row misses (never on a sealed set)")
    ap.add_argument("--out", help="write aggregates (and rows only with --debug) as JSON")
    a = ap.parse_args()
    logging.disable(logging.WARNING)
    from services.mapping_holdout_eval import evaluate

    path = Path(a.fixture) if a.fixture else None
    split = None if path else a.split
    res = evaluate(split, names_only=a.names_only, path=path, engine=a.engine, bootstrap=a.bootstrap)
    agg = {k: v for k, v in res.items() if k != "rows"}
    agg["fixture"] = "external" if path else "mapping_holdout_v1"
    if path:
        agg.pop("buckets", None)  # a sealed fixture's own labels are not echoed
    if a.out:
        Path(a.out).write_text(json.dumps(res if a.debug else agg, indent=1, ensure_ascii=False), encoding="utf-8")
    print(json.dumps({k: agg[k] for k in ("fixture", "split", "engine", "mode", "overall")}, indent=1))
    if "intervals" in agg:
        print("intervals (95%, cluster bootstrap over tables):", json.dumps(agg["intervals"]))
    for b, m in (agg.get("buckets") or {}).items():
        print(
            f"{b:22s} n={m['n_matchable']:3d}/{m['n_negative']:2d}neg top1={m['top1']} top3={m['top3']} "
            f"P={m['proposal_precision']} auto={m['auto_correct']}/{m['auto_count']} "
            f"negProp={m['neg_proposed']} negAuto={m['neg_auto']}"
        )
    if a.debug:
        for r in res["rows"]:
            if r["pred"] != r["gold"]:
                print(f"  {r['table']:28s} {r['source']:22s} gold={r['gold']!s:24s} pred={r['pred']!s:24s} "
                      f"conf={r['conf']:.3f} review={r['review']} auto={r['auto']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
