"""UX-19: Pilot's transfer answer, derived from the plan and gate result it shows.

The answer used to contradict itself: it said "no cast/mutate/lossy fidelity
risks" under a blocker about fidelity, called a column whose type is the same
on both sides a "collapse", reported a destination it could not read as an
"introspect probe failed", and listed every gate as a raw mark (``✗g6_target_ddl``).

Every sentence here is computed from the same plan (type conversions,
destination read) and preflight result (gates, blockers) the Studio shows, so
the parts of one answer cannot disagree. Gate names are the labels the Studio's
gate catalog uses (``apps/web/src/lib/preflightGates.ts``; a test keeps them in
step).
"""

from __future__ import annotations

import re
from typing import Any

#: The Studio's gate catalog labels, by gate id.
GATE_LABELS: dict[str, str] = {
    "g1_source": "Source readable",
    "g2_destination": "Destination write access",
    "g3_schema_contract": "Schema contract",
    "g4_mapping_confidence": "Column mappings",
    "g5_dry_run": "Sample dry-run",
    "g9_data_integrity": "Data integrity",
    "g6_target_ddl": "Target DDL",
    "g7_capacity": "Staging capacity",
    "g8_reconciliation": "Sample reconciliation",
    "g10_schema_policy": "Schema change policy",
    "g11_validation_posture": "Validation posture",
    "schema_drift": "Schema drift",
    "g13_source_coverage": "Source column coverage",
    "g14_destination_requirements": "Destination required columns",
    "g15_dest_exists_shape": "Dest-exists shape",
    "g16_field_reduction": "Field reduction governance",
    "g19_dest_schema_replacement": "Destination schema replacement",
    "g18_cdc_snapshot_mode": "CDC snapshot mode",
    "g24_append_continuity": "Append continuity",
    "g24_reversibility": "Reversibility",
    "g3f_population_fit": "Population fit",
    "g20_code_crosswalk": "Code crosswalk coverage",
    "constraint_fk": "Foreign key coverage",
    "g21_control_totals": "Control totals",
    "g23_stream_procedures": "Stream procedures",
    "g22_dest_referential_integrity": "Destination referential integrity",
    "g_create_dest_ddl": "Create destination table",
    "g_upsert_dest_key": "Upsert key on destination",
    "g12_staging_policy": "Staging policy",
    "g23_destination_name": "Destination name",
    "g9_sync_contract": "Sync contract",
}

UNREAD_FIDELITY = "dest_type_unread"
_RISKY_FIDELITY = {"lossy_cast", "cast", "mutate", "narrow", "lossy"}
_ROOT_PREFIX = "rc-"


def gate_label(gate_id: Any) -> str:
    """The Studio's name for a gate; an unlisted id reads as words, not an identifier."""
    gid = str(gate_id or "").strip().lower()
    if gid in GATE_LABELS:
        return GATE_LABELS[gid]
    words = re.sub(r"^g\d+[a-z]?_", "", gid).replace("_", " ").strip()
    return words[:1].upper() + words[1:] if words else "Check"


def _conversions(plan: dict[str, Any]) -> list[dict[str, Any]]:
    return [c for c in plan.get("type_conversions") or [] if isinstance(c, dict)]


def unread_conversions(plan: dict[str, Any]) -> list[dict[str, Any]]:
    """Columns whose destination type was never read: nothing to compare yet."""
    return [c for c in _conversions(plan) if str(c.get("fidelity") or "").strip().lower() == UNREAD_FIDELITY]


def _changes_type(conversion: dict[str, Any]) -> bool:
    src = str(conversion.get("from_type") or "").strip().upper()
    dst = str(conversion.get("to_type") or "").strip().upper()
    return bool(src and dst and src != dst)


def destination_read_problem(plan: dict[str, Any]) -> str:
    """Why the destination table could not be read, in words; "" when it was read."""
    dst = plan.get("destination") or {}
    if dst.get("table_exists") is not None:
        return ""
    table = str(dst.get("table") or "").strip()
    name = str(dst.get("connector_name") or "the destination").strip()
    raw = str(dst.get("read_error") or "").strip()
    engine = str(dst.get("type") or "").strip().lower()
    reason = ""
    denied = re.search(r"access denied for user [^\n]*? to database '([^']+)'", raw, re.I)
    unknown_db = re.search(r"unknown database '([^']+)'", raw, re.I)
    if denied:
        reason = f"this connection has no access to database `{denied.group(1)}`"
    elif unknown_db:
        reason = f"database `{unknown_db.group(1)}` does not exist"
    elif raw:
        reason = raw.splitlines()[0][:160].rstrip(". ")
    sentence = f"Could not read `{table}` on **{name}**" + (f": {reason}." if reason else ", so whether it exists is unknown.")
    own_db = str(dst.get("database") or "").strip()
    prefix = table.split(".", 1)[0] if "." in table else ""
    if engine in {"mysql", "mariadb"} and prefix and prefix != own_db:
        sentence += (
            f" On MySQL the part before the dot names a database"
            + (f"; this connection's database is `{own_db}`." if own_db else ".")
        )
    return sentence


def _is_fidelity_root(blocker: dict[str, Any]) -> bool:
    bid = str(blocker.get("id") or "")
    return bid.startswith(_ROOT_PREFIX) and "fidelity" in bid


def blocker_title(blocker: dict[str, Any]) -> str:
    bid = str(blocker.get("id") or "")
    if bid.startswith(_ROOT_PREFIX):
        message = str(blocker.get("message") or "")
        head = message.split(":", 1)[0].strip()
        return head or "Root cause"
    return gate_label(bid)


def honest_blockers(plan: dict[str, Any], preflight: dict[str, Any], limit: int = 4) -> list[tuple[str, str]]:
    """``(title, sentence)`` per blocker, reconciled with what the plan measured."""
    out: list[tuple[str, str]] = []
    unread = unread_conversions(plan)
    real_changes = [c for c in _conversions(plan) if _changes_type(c)]
    risky = [c for c in _conversions(plan) if str(c.get("fidelity") or "").strip().lower() in _RISKY_FIDELITY]
    read_problem = destination_read_problem(plan)
    for blocker in (preflight.get("blockers") or [])[:limit]:
        if not isinstance(blocker, dict) or not blocker.get("message"):
            continue
        bid = str(blocker.get("id") or "")
        message = str(blocker.get("message") or "").strip()
        if _is_fidelity_root(blocker) and unread and not real_changes:
            out.append((
                "Destination column types not read",
                f"{len(unread)} column(s) have no destination type to compare with, so type fidelity is "
                "unproven. This is not a type change; it clears once the destination table can be read.",
            ))
            continue
        if _is_fidelity_root(blocker) and not risky and not real_changes:
            # The plan keeps every type; the gate could not prove it. Saying
            # "collapse" here contradicted the plan two lines below.
            out.append((
                "Column types not proven",
                f"{gate_label('g4_mapping_confidence')} could not prove these columns against the destination "
                "types. The plan shows no type change on them, so this is not a collapse.",
            ))
            continue
        if bid.lower() == "g6_target_ddl" and "existence is unknown" in message and read_problem:
            out.append((gate_label(bid), read_problem))
            continue
        title = blocker_title(blocker)
        text = message.split(":", 1)[1].strip() if bid.startswith(_ROOT_PREFIX) and ":" in message else message
        out.append((title, text))
    return out


def conversions_line(plan: dict[str, Any], preflight: dict[str, Any]) -> str:
    """The one line about type conversions; never "no risks" beside a blocker."""
    conversions = _conversions(plan)
    unread = unread_conversions(plan)
    if unread:
        return (
            f"• Destination types not read for **{len(unread)}** column(s) — type fidelity is unproven "
            "until the destination table can be read."
        )
    if not conversions:
        return ""
    if preflight.get("blockers"):
        return f"• {len(conversions)} type conversion(s) keep their values (preserve)."
    return (
        f"• {len(conversions)} type conversion(s) stamped preserve — "
        "no cast/mutate/lossy fidelity risks in the plan."
    )


def gate_summary_line(preflight: dict[str, Any]) -> str:
    """Preflight in words: how many passed, which blocked or warned, by Studio name."""
    gates = [g for g in preflight.get("gates") or [] if isinstance(g, dict)]
    if not gates:
        return ""
    by_status: dict[str, list[str]] = {}
    for gate in gates:
        by_status.setdefault(str(gate.get("status") or "").lower(), []).append(gate_label(gate.get("id")))
    passed = preflight.get("passed_count")
    total = preflight.get("total_gates") or len(gates)
    parts = [f"• Preflight: **{passed if passed is not None else len(by_status.get('pass', []))} of {total}** checks passed"]
    if by_status.get("block"):
        parts.append("blocked: " + ", ".join(by_status["block"]))
    if by_status.get("warn"):
        parts.append("warning: " + ", ".join(by_status["warn"]))
    if by_status.get("skip"):
        parts.append(f"{len(by_status['skip'])} not applicable")
    return " · ".join(parts) + "."


def refusal_sentence(plan: dict[str, Any], preflight: dict[str, Any]) -> str:
    """The lead sentence when preflight refuses: named checks, no raw ids."""
    listed = "; ".join(f"{title}: {text.rstrip('.')}" for title, text in honest_blockers(plan, preflight))
    return "Preflight blocked this transfer, so I won't start it" + (f" — {listed}" if listed else "")
