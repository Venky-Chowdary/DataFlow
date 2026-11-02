"""Train the Pilot intent router and write ``artifacts/pilot_intent_v1.npz``.

    cd apps/api && python scripts/train_pilot_intent_router.py

Prints fit accuracy on the augmented seeds and — the number that matters —
accuracy plus abstention rate on the held-out probe that shares no text with
the seeds. Deterministic: same seeds, same artifact.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.ai.first_party.intent_labels import augmented_examples, held_out_probe  # noqa: E402
from src.ai.first_party.intent_router import train  # noqa: E402


def main() -> int:
    examples = augmented_examples()
    router = train(examples)
    fit = sum(
        1 for text, act in examples if (r := router.route(text, min_confidence=0, min_margin=0)) and r.act == act
    )
    print(f"train examples: {len(examples)}  fit accuracy: {fit / len(examples):.3f}")

    probe = held_out_probe()
    correct = abstained = wrong = 0
    for text, act in probe:
        r = router.route(text)
        if r is None:
            abstained += 1
            print(f"  ABSTAIN  {text!r}  (want {act})")
        elif r.act == act:
            correct += 1
        else:
            wrong += 1
            print(f"  WRONG    {text!r}  got {r.act} ({r.confidence:.2f}) want {act}")
    print(
        f"held-out: {correct}/{len(probe)} correct, {abstained} abstained, {wrong} wrong"
    )
    dest = router.save()
    print(f"wrote {dest} ({dest.stat().st_size} bytes)")
    return 0 if wrong == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
