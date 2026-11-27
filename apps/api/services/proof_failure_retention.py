"""A proof-layer verdict alone never deletes the rows a run committed (D331, R9).

A failed run whose destination was empty before is rolled back by clearing
the batch (``services.batch_undo``). That is right when Gate-8 proved the rows
wrong by count or digest. It is wrong when the only failing evidence is a
proof that re-reads and re-derives the data itself: the transform-applied proof
(which re-reads the source and re-applies the recipe) or a check that could not
be measured at all. A defect in that re-read is indistinguishable, from the
verdict alone, from a wrong write, and QA r67-r70 watched a strict Oracle ->
Oracle upsert delete 21 correct rows that way.

So such a failure keeps the committed rows. The job still fails, with the
proof's own culprit keys named for the operator; nothing is weakened, only the
destructive rollback is withheld until a person or a sound check confirms it.
"""

from __future__ import annotations

from typing import Any

#: ``recon`` key listing the proof layers that turned a passing report into a failure.
PROOF_LAYER_FAILURES = "proof_layer_failures"

TRANSFORM_PROOF = "transform_applied_proof"
UNMEASURED_CHECK = "check_not_measured"


def mark_proof_layer_failure(recon: dict[str, Any], layer: str, *, was_passing: bool) -> None:
    """Record that ``layer`` (not Gate-8's count/digest) failed a passing report."""
    if not isinstance(recon, dict) or not was_passing:
        return
    layers = recon.setdefault(PROOF_LAYER_FAILURES, [])
    if layer not in layers:
        layers.append(layer)


def proof_layer_retention_note(recon: Any) -> str:
    """Why the committed rows are kept, or ``""`` when an ordinary undo applies."""
    if not isinstance(recon, dict) or recon.get("passed", True):
        return ""
    layers = list(recon.get(PROOF_LAYER_FAILURES) or [])
    details = recon.get("details") if isinstance(recon.get("details"), dict) else {}
    marker = recon.get("null_marker_check") if isinstance(recon.get("null_marker_check"), dict) else {}
    if marker.get("leaked"):
        # A measured marker leak is a wrong value, proven: the ordinary undo.
        return ""
    if str(details.get("reason") or "") == "null_marker_check_not_measured":
        layers.append(UNMEASURED_CHECK)
    if not layers:
        return ""
    if TRANSFORM_PROOF in layers:
        proof = recon.get("transform_applied") if isinstance(recon.get("transform_applied"), dict) else {}
        keys = proof.get("culprit_keys") if isinstance(proof.get("culprit_keys"), dict) else {}
        named = "; ".join(
            f"{kind}: {', '.join(str(k) for k in (vals or [])[:8])}"
            for kind, vals in keys.items()
            if vals
        )
        return (
            "The committed rows were kept: the failure comes from the transform "
            "proof's own re-read of the source, and a proof verdict alone never "
            "deletes rows."
            + (f" Rows the proof names — {named}." if named else "")
        )
    return (
        "The committed rows were kept: a check that could not be measured is "
        "not evidence the batch is wrong."
    )
