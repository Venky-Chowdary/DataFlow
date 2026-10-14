"""Operator-facing titles for preflight blockers.

Proof-bundle blockers are numbered by position (``proof_0``, ``proof_1``) rather
than named like a gate, so any title that falls back to the blocker id spells an
internal identifier at the operator ("this transfer is blocked by proof_0").
This module is the single owner of that translation on the API side; the client
mirror is ``apps/web/src/lib/preflightGates.ts::blockerTitle``.
"""

from __future__ import annotations

import re

MAX_TITLE_LEN = 72

_INTERNAL_ID = re.compile(r"^proof_\d+$", re.IGNORECASE)
_CLAUSE_SPLIT = re.compile(r"[.;\n]|\s—\s")


def is_internal_blocker_id(gate_id: str) -> bool:
    """True when ``gate_id`` is a positional proof id, not a named gate."""
    return bool(_INTERNAL_ID.match(str(gate_id or "").strip()))


def is_schema_drift_echo(message: object) -> bool:
    """True when this text is the schema-drift gate, not a second root cause.

    The proof bundle copies the gate sentence ("Schema change detected — …")
    beside the catalog title "Schema drift". Both name the same review.
    """
    text = str(message or "").strip().lower()
    return (
        text.startswith("schema change detected")
        or text.startswith("schema drift")
        or text.startswith("schema change acknowledged")
        or text.startswith("breaking schema change")
    )


def blocker_title(gate_id: str, message: str = "", *, catalog_title: str = "") -> str:
    """Human title for a blocker, never the internal id.

    ``catalog_title`` is the gate-catalog title when one exists; it wins for
    named gates. Positional proof blockers are named from their own message.
    """
    gate_id = str(gate_id or "").strip()
    catalog_title = str(catalog_title or "").strip()
    if not is_internal_blocker_id(gate_id):
        return catalog_title or gate_id or "Validation blocker"
    if catalog_title and not is_internal_blocker_id(catalog_title):
        return catalog_title
    text = str(message or "").strip()
    if not text:
        return "Transfer proof blocker"
    clause = _CLAUSE_SPLIT.split(text)[0].strip() or text
    if len(clause) > MAX_TITLE_LEN:
        return clause[: MAX_TITLE_LEN - 3].rstrip() + "…"
    return clause
