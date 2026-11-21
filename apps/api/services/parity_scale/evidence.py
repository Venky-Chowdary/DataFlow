"""The evidence record of one scan (the future ``reconciliation.parity_at_scale``).

Phase 1 produces the record and nothing reads it: Gate-8, the verdict, the
proof pack and the Trust Report are unchanged until phase 3. The record is
already worded the way those will read it: coverage is stated (a unit that was
not compared makes the outcome ``not_fully_compared``, never a match), and a
keyless table's differences are content counts that are never attributed to a
key.
"""

from __future__ import annotations

from typing import Any

from services.fingerprint_rules import FINGERPRINT_VERSION

CHECK_LABELS = {
    "segment_diff": "Every row compared inside both databases (segment digests, then row and cell drill-down).",
    "multiset_digest": "Content match (no key): every row compared as a set of row contents.",
}
KEYLESS_NOTE = (
    "The table has no key, so a difference cannot be attributed to a key: it is "
    "reported as rows of content missing from or extra in the destination."
)
TAMPER_NOTE = (
    "Sums of SHA-256 words protect against accidental change, not against an "
    "adversary who controls the data; tamper evidence is the signed proof pack."
)


NOT_ESTABLISHED_NOTE = (
    "The tables matched at scan time, but writes during the scan were not ruled out."
)


def consistency_of(state: dict[str, Any]) -> dict[str, Any]:
    """What the scan established about writes during it, and nothing more.

    ``pinned``: each side was read from one snapshot for the whole scan, so the
    comparison is of the destination as of its snapshot against the source as
    of its snapshot. Anything else is ``not_established`` with the reasons.
    """
    recorded = state.get("consistency") or {}
    sides = {role: recorded.get(role) or {} for role in ("source", "destination")}
    reasons = [str(recorded["broken"])] if recorded.get("broken") else []
    for role, info in sides.items():
        if info.get("kind") != "pinned":
            reasons.append(f"{role}: {info.get('reason') or 'read statement by statement, not from one snapshot'}")
    if not reasons:
        return {"kind": "pinned", "source": sides["source"], "destination": sides["destination"]}
    return {"kind": "not_established", "reasons": reasons, "note": NOT_ESTABLISHED_NOTE}


def build_evidence(state: dict[str, Any]) -> dict[str, Any]:
    plan = state["plan"]
    columns = plan["columns"]
    diff = state["diff"]
    judge = state["judge"]
    units = state["units"]
    not_compared = state["not_compared"]
    keyless = not plan.get("key")
    systematic = state.get("systematic")
    done = state["status"] == "done"
    complete = done and not not_compared and not systematic

    engine_changed = int(diff["changed"])
    judged_equal = int(judge["ce_stricter_than_fp"])
    changed_rows = max(engine_changed - judged_equal, 0)
    # Changed keys beyond the stored cap are counted but never fetched; keys
    # whose fetch found no row pair (moved in flight) are not judged either.
    unjudged = max(engine_changed - len(diff["changed_keys"]), 0) + int(judge["not_judged"])
    differences = {
        "missing_in_destination": int(diff["missing"]),
        "extra_in_destination": int(diff["extra"]),
        "at_least": not complete,
    }
    if keyless:
        differences.update(
            {
                "key_attribution": "cannot_attribute_to_key",
                "note": KEYLESS_NOTE,
                "missing_content_examples": state["examples"]["missing_content"],
                "extra_content_examples": state["examples"]["extra_content"],
            }
        )
    else:
        differences.update(
            {
                "key_attribution": "per_key",
                "changed_rows": changed_rows,
                "changed_rows_by_engine_digest": engine_changed,
                "duplicate_keys": int(diff["duplicate"]),
                "changed_cells_by_column": dict(judge["cells_by_column"]),
                "by_column": judge.get("by_column") or {},
                "causes": dict(judge["causes"]),
                "explained_rows": int(judge["explained_rows"]),
                "unexplained_rows": int(judge["unexplained_rows"]),
                "rows_not_judged": unjudged,
                "ce_stricter_than_fp": judged_equal,
                "keys_stored": len(diff["changed_keys"]) + len(diff["missing_keys"])
                + len(diff["extra_keys"]) + len(diff["duplicate_keys"]),
                "keys_capped": bool(diff["keys_capped"]),
                "missing_keys": diff["missing_keys"],
                "extra_keys": diff["extra_keys"],
                "changed_keys": diff["changed_keys"],
                "duplicate_key_values": diff["duplicate_keys"],
                "examples": judge["examples"],
            }
        )
    any_difference = bool(
        differences["missing_in_destination"]
        or differences["extra_in_destination"]
        or (not keyless and (changed_rows or differences["duplicate_keys"] or unjudged))
    )
    if not done:
        outcome = "running"
    elif any_difference:
        outcome = "differences_found"
    elif not complete:
        outcome = "not_fully_compared"
    else:
        outcome = "match"
    rows_src = int(state["rows"]["source"])
    held = state.get("held_out")
    return {
        "experimental": True,
        "verdict_effect": "none (phase 1: not read by Gate-8, the verdict, the proof pack or the Trust Report)",
        "scan_id": state["scan_id"],
        "algorithm": state["algorithm"],
        "ce_version": state["ce_version"],
        "fingerprint_version": FINGERPRINT_VERSION,
        "hash": state["hash"],
        "tamper_note": TAMPER_NOTE,
        "check_kind": state["check_kind"],
        "check_label": CHECK_LABELS[state["check_kind"]],
        "mode": state["mode"],
        "mode_reason": state["mode_reason"],
        "boundary_strategy": state.get("boundary_strategy"),
        "plan_hash": state["plan_hash"],
        "plan": {
            "columns": [
                {k: c.get(k) for k in ("source", "target", "cls", "kind", "projection", "width", "reason")}
                for c in columns
            ],
            "key": plan.get("key") or [],
            "notes": plan.get("notes") or [],
        },
        "source": {"engine": plan["source_engine"]},
        "destination": {"engine": plan["dest_engine"]},
        "certification": state.get("certification"),
        "population": {
            "rows_source": rows_src,
            "rows_destination": int(state["rows"]["destination"]),
            "rows_compared_complete": bool(state["rows"].get("level0_complete")) and not not_compared,
            "held_out": held,
        },
        "coverage": {
            "complete": complete,
            "units_not_compared": len(not_compared),
            "not_compared": not_compared[:50],
            "systematic": systematic,
            "columns_in_engine": sum(1 for c in columns if c["cls"] == "engine"),
            "columns_streamed": sum(1 for c in columns if c["cls"] == "stream"),
            "columns_excluded": sum(1 for c in columns if c["cls"] == "excluded"),
            "stream": state.get("stream"),
        },
        "units": dict(units),
        "differences": differences,
        "outcome": outcome,
        "consistency": consistency_of(state),
        "statements": state["statements"],
        "elapsed_s": state["elapsed_s"],
        "started_at": state["started_at"],
        "finished_at": state["finished_at"],
        "resumed": state["resumed"],
    }
