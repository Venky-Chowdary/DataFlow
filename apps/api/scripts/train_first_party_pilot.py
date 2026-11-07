#!/usr/bin/env python3
"""Train the first-party copy-grounded Pilot generator and write the checkpoint.

Run from ``apps/api``::

    python scripts/train_first_party_pilot.py

The script fits a hashed dual encoder (InfoNCE) and a sentence-level
pointer-generator on the product corpus plus deterministic paraphrases.
It does not call OpenAI. It does not download HuggingFace weights.

Holdout checks printed at the end are measurements on this run, not
marketing scores.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
_SRC = _API_ROOT / "src"
# Same order as tests/conftest.py: api root wins so `services` is
# apps/api/services, not the src/services shim.
for path in (_SRC, _API_ROOT):
    p = str(path)
    while p in sys.path:
        sys.path.remove(p)
    sys.path.insert(0, p)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Destination npz (default: src/ai/first_party/artifacts/pilot_fp_v1.npz)",
    )
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--encoder-epochs", type=int, default=8)
    parser.add_argument("--generator-epochs", type=int, default=4)
    args = parser.parse_args()

    from src.ai.first_party.checkpoint import default_artifact_path, train_checkpoint
    from src.ai.first_party.dataset import align_pairs
    from src.ai.first_party.dual_encoder import nearest_gold

    bundle = train_checkpoint(
        seed=args.seed,
        encoder_epochs=args.encoder_epochs,
        generator_epochs=args.generator_epochs,
    )
    dest = bundle.save(args.out or default_artifact_path())

    pairs = align_pairs()
    holdout = [
        ("are you chatgpt", "chatgpt"),
        ("gotta have logical wal", "wal"),
        ("where do bad rows go", "row"),
        ("how do I cook rice tonight", None),
    ]
    print(f"wrote {dest}")
    print(f"align_pairs={len(pairs)} gold_questions={len(bundle.gold_questions)}")
    for query, needle in holdout:
        gold, score = nearest_gold(
            bundle.encoder,
            query,
            list(bundle.gold_questions),
            bundle.gold_vectors,
        )
        ok = True
        if needle is None:
            ok = score < 0.86
        elif needle not in gold.lower():
            ok = False
        mark = "OK" if ok else "CHECK"
        print(f"  [{mark}] {query!r} -> {gold!r} cosine={score:.3f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
