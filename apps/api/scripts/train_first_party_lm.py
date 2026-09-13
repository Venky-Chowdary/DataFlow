#!/usr/bin/env python3
"""Train Datawrap's own causal Transformer decoder and write ``pilot_lm_v1.npz``.

Run from ``apps/api``::

    python scripts/train_first_party_lm.py

No OpenAI. No HuggingFace download. Holdout lines printed at the end
are measurements on this run, not marketing scores.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
_SRC = _API_ROOT / "src"
for path in (_SRC, _API_ROOT):
    p = str(path)
    while p in sys.path:
        sys.path.remove(p)
    sys.path.insert(0, p)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--epochs", type=int, default=18)
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()

    from src.ai.first_party.context_pack import pack_context
    from src.ai.first_party.lm_checkpoint import train_lm

    bundle = train_lm(seed=args.seed, epochs=args.epochs)
    dest = bundle.save()
    print(f"wrote {dest} vocab={bundle.vocab.size}")

    holdout = [
        ("date today", pack_context(today_utc="Sunday, 13 September 2026"), "today"),
        (
            "is schedules working",
            pack_context(
                pipeline_count=2,
                parked_count=2,
                parked_names=["Nightly", "TestJob"],
                enabled_count=0,
            ),
            "pipeline",
        ),
        (
            "can you create connection",
            pack_context(connector_count=12, create_connection="confirm_gated"),
            "confirm",
        ),
    ]
    for question, ctx, needle in holdout:
        text = bundle.generate_grounded(question, ctx).lower()
        ok = needle in text
        mark = "OK" if ok else "CHECK"
        print(f"  [{mark}] {question!r} -> {text[:120]!r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
