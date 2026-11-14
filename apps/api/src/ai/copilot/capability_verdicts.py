"""Yes / no capability answers read from the tables the product enforces.

Pilot answered "Can I run CDC from DynamoDB?" with "Yes — DynamoDB is a
transfer-ready driver" (no DynamoDB change reader exists), "Can I mirror into
MongoDB?" with a paragraph about mirror semantics, and "Is Shopify
supported?" with a refusal (CLM-10, CLM-14). A capability question has a
verdict, and the verdict lives in four tables:

* CDC sources: ``services.preflight_cursor_gate.CDC_CAPABLE_SOURCES``, the
  list Validate blocks every other source with;
* sync mode x destination: ``services.destination_sync_modes``, the list the
  picker offers and Validate's destination gates agree with (CLM-8);
* source / destination side: the catalog's ``source_ready`` / ``dest_ready``;
* tier: the catalog's ``certification_tier`` (certified only with CI
  evidence, implemented, planned with why).

``capability_verdict`` returns None unless the question names both a
capability and a connector, so everything else keeps its documented answer.
"""

from __future__ import annotations

import re
from functools import lru_cache
from typing import Any

_MODE_WORDS: tuple[tuple[str, str, str], ...] = (
    # (regex, sync mode id, operator name)
    (r"\b(?:scd\s*(?:type\s*)?2|slowly\s+changing)\b", "scd2", "SCD Type 2"),
    (r"\bmirror(?:ing)?\b", "mirror", "Mirror"),
    (r"\b(?:upsert|dedup(?:e|ed|licated)?|merge)\b", "incremental_deduped", "upsert (Incremental deduped)"),
    (r"\boverwrite\b", "full_refresh_overwrite", "Full overwrite"),
    (r"\bappend\b", "full_refresh_append", "append"),
    (r"\b(?:cdc|change\s+data\s+capture)\b", "cdc", "CDC"),
)

_INTO = r"(?:into|to|in|on)"
_FROM = r"(?:from|out\s+of|off)"


@lru_cache(maxsize=1)
def _catalog_names() -> tuple[tuple[str, str], ...]:
    """(lower-case name or id, catalog id), longest first, for whole-word lookup."""
    from services.catalog_service import load_catalog

    pairs: dict[str, str] = {}
    for entry in load_catalog().get("connectors", []):
        cid = str(entry.get("id") or "").lower()
        name = str(entry.get("name") or "").lower()
        for key in {cid, cid.replace("_", " "), name}:
            if len(key) >= 2 and key not in pairs:
                pairs[key] = cid
    return tuple(sorted(pairs.items(), key=lambda kv: -len(kv[0])))


def _entity_after(text: str, preposition: str) -> dict[str, Any] | None:
    """The catalog connector named right after ``preposition`` in ``text``."""
    match = re.search(rf"\b{preposition}\s+(.+)$", text)
    if not match:
        return None
    return _first_entity(match.group(1))


def _first_entity(text: str) -> dict[str, Any] | None:
    from services.catalog_service import get_connector_by_id

    best: tuple[int, str] | None = None
    for key, cid in _catalog_names():
        found = re.search(rf"(?<![a-z0-9]){re.escape(key)}(?![a-z0-9])", text)
        if found and (best is None or found.start() < best[0]):
            best = (found.start(), cid)
    return get_connector_by_id(best[1]) if best else None


def _name(row: dict[str, Any]) -> str:
    return str(row.get("name") or row.get("id"))


def _tier_sentence(row: dict[str, Any]) -> str:
    tier = row.get("certification_tier")
    if tier == "certified":
        return f"{_name(row)} is Certified: a green CI run proves its engine."
    if tier == "implemented":
        return (
            f"{_name(row)} is Implemented — not yet proven: it runs, but no green CI run "
            "proves it yet, so it is not offered for production schedules."
        )
    return ""


def _cdc_source_verdict(row: dict[str, Any]) -> str:
    from services.preflight_cursor_gate import CDC_CAPABLE_SOURCES

    cid = str(row.get("id") or "").lower()
    driver = str(row.get("driver_type") or "").lower()
    names = ", ".join(sorted({s for s in CDC_CAPABLE_SOURCES if "_" not in s and s != "mssql"}))
    if cid in CDC_CAPABLE_SOURCES or driver in CDC_CAPABLE_SOURCES:
        return (
            f"Yes — {_name(row)} can be a CDC source: it is on the list Validate checks "
            f"(CDC sources: {names}). Delivery is at-least-once upsert."
        )
    return (
        f"No — {_name(row)} has no change-capture reader, so it cannot be a CDC source; "
        f"Validate blocks CDC from it. CDC sources are: {names}. "
        f"For {_name(row)}, use Incremental append or Incremental deduped on a cursor column."
    )


def _mode_into_verdict(row: dict[str, Any], mode: str, label: str) -> str:
    from services.destination_sync_modes import destination_sync_mode_refusal, sync_mode_notes

    if not row.get("dest_ready"):
        return _side_verdict(row, "destination")
    driver = str(row.get("driver_type") or row.get("id") or "").lower()
    refusal = destination_sync_mode_refusal(driver, mode)
    if refusal and refusal.startswith(f"{driver} "):
        refusal = f"{_name(row)} {refusal[len(driver) + 1:]}"
    if refusal:
        return f"No — {label} into {_name(row)} is not offered: {refusal}"
    note = sync_mode_notes(driver).get(mode, "")
    tier = _tier_sentence(row)
    return " ".join(p for p in (f"Yes — {_name(row)} accepts {label}.", note, tier) if p)


def _side_verdict(row: dict[str, Any], side: str) -> str:
    planned = row.get("certification_tier") == "planned"
    if planned:
        return _planned_verdict(row)
    ok = row.get("source_ready" if side == "source" else "dest_ready")
    role = str(row.get("role_label") or "")
    if ok:
        verb = "read from" if side == "source" else "write to"
        tier = _tier_sentence(row)
        return " ".join(p for p in (f"Yes — a transfer can {verb} {_name(row)} ({role}).", tier) if p)
    if side == "source":
        return f"No — {_name(row)} cannot be read by a transfer: it is {role.lower()}."
    return f"No — {_name(row)} cannot be written by a transfer: it is {role.lower()}."


def _planned_verdict(row: dict[str, Any]) -> str:
    why = str(row.get("planned_reason") or "").removeprefix("Planned: ")
    because = f" {why}" if why else " No driver serves it yet; a catalog tile is not a live connector."
    return f"No — {_name(row)} is Planned, not available for transfers today.{because}"


def _supported_verdict(row: dict[str, Any]) -> str:
    tier = row.get("certification_tier")
    if tier == "planned":
        return _planned_verdict(row)
    if tier == "connect_only":
        return f"Partly — {_name(row)} can be saved and connection-tested, but no transfer driver moves its data."
    if tier == "source_only":
        return f"Yes, as a source only — {_name(row)} can be read; it cannot be a destination."
    role = str(row.get("role_label") or "")
    return " ".join(p for p in (f"Yes — {_name(row)} is available for transfers ({role}).", _tier_sentence(row)) if p)


def cdc_sources_answer() -> str:
    from services.preflight_cursor_gate import CDC_CAPABLE_SOURCES

    names = ", ".join(sorted({s for s in CDC_CAPABLE_SOURCES if "_" not in s and s != "mssql"}))
    return (
        f"CDC sources: {names} (plus their managed hosting, such as RDS and Aurora). "
        "This is the list Validate enforces; every other source is blocked for CDC. "
        "Kafka, DynamoDB and the vector stores are not CDC sources."
    )


def capability_verdict(query: str, *, fallback: bool = False) -> str | None:
    """A yes / no answer for a capability question, or None when it is not one.

    Asked first (``fallback=False``) it answers only questions with a verdict
    table behind them about a connector that exists: CDC from X, a mode into
    X, reading from / writing to X. The documented capability cards keep
    their answers about Planned brands ("does not ship Looker"). Asked last
    (``fallback=True``), when nothing documented answered, it also says
    whether a named connector is supported (Shopify: Planned).
    """
    text = (query or "").lower().strip().rstrip("?.! ")
    if not text:
        return None
    if re.search(r"\b(?:which|what)\s+(?:sources?|engines?|databases?)\b.*\b(?:cdc|change\s+data\s+capture)\b", text):
        return cdc_sources_answer()

    def _ok(row: dict[str, Any] | None) -> bool:
        return row is not None and (fallback or row.get("certification_tier") != "planned")

    # "CDC from X" / "does X support CDC"
    if re.search(r"\b(?:cdc|change\s+data\s+capture)\b", text):
        row = _entity_after(text, _FROM)
        if row is None and re.search(r"\b(?:support|do|does|run|use)\b", text) and not re.search(rf"\b{_INTO}\b", text):
            row = _first_entity(text)
        if _ok(row):
            return _cdc_source_verdict(row)
    for pattern, mode, label in _MODE_WORDS:
        if re.search(pattern, text):
            row = _entity_after(text, _INTO)
            if _ok(row):
                return _mode_into_verdict(row, mode, label)
    if re.search(r"\b(?:read|extract|pull)\b", text):
        row = _entity_after(text, _FROM)
        if _ok(row):
            return _side_verdict(row, "source")
    if re.search(r"\b(?:write|load|push|send)\b", text):
        row = _entity_after(text, _INTO)
        if _ok(row):
            return _side_verdict(row, "destination")
    if fallback and re.search(r"\b(?:supported|support|available|work with|have)\b", text):
        row = _first_entity(re.sub(r"\bdatawrap\b", " ", text))
        if row is not None:
            return _supported_verdict(row)
    return None
