"""Validate's hash_pii checks (D8, QA browser 2 and review #7).

* ``g_pii_hash_key``: hash_pii fails closed without its HMAC key. Refuse from
  the plan, not the sample — an all-NULL sample never runs the digest and a
  shape-recipe step is not in the mapping replay — so the run cannot be the
  first to say.
* ``pii_hash_key_changed`` (warning): the workspace key changed since this
  route's last load, so hashed values will not match the rows it wrote.

Extracted from ``services.preflight_service`` (size budget).
"""

from __future__ import annotations

from typing import Any


def hashed_columns(plan_mappings: list[Any], shape_recipe: dict[str, Any] | None) -> list[str]:
    cols = [pm.source for pm in plan_mappings if getattr(pm, "transform", "") == "hash_pii"]
    for step in (shape_recipe or {}).get("steps") or []:
        if isinstance(step, dict) and step.get("enabled") is not False and step.get("op") == "hash_pii":
            cols.append(str(step.get("column") or ""))
    return list(dict.fromkeys(c for c in cols if c))


def pii_hash_key_gate(
    plan_mappings: list[Any], shape_recipe: dict[str, Any] | None, blockers: list[dict[str, Any]]
) -> dict[str, Any] | None:
    """The gate (and its blocker, appended to ``blockers``) or ``None`` when nothing hashes."""
    from services.transform_engine import PII_HASH_KEY_SETUP, pii_hash_key

    columns = hashed_columns(plan_mappings, shape_recipe)
    if not columns:
        return None
    key_missing = not pii_hash_key()
    gate = {
        "id": "g_pii_hash_key",
        "status": "block" if key_missing else "pass",
        "message": PII_HASH_KEY_SETUP if key_missing else "hash_pii HMAC key is configured",
        "duration_ms": 0,
        "details": {"columns": columns[:20], "kind": "pii_hash_key"},
    }
    if key_missing:
        blockers.append({"id": gate["id"], "message": PII_HASH_KEY_SETUP, "details": dict(gate["details"])})
    return gate


def apply_key_change_warning(out: dict[str, Any], source: dict[str, Any], destination: dict[str, Any]) -> None:
    """Warn when this route's last load hashed with another key version."""
    gate = next((g for g in out.get("gates") or [] if g.get("id") == "g_pii_hash_key"), None)
    if not gate or gate.get("status") != "pass":
        return
    from services.data_quality_history import last_pii_hash_key_version
    from services.egress_guard import current_egress_workspace
    from services.pii_hash_keys import key_change_warning
    from services.workspace_filter import current_workspace_stamp

    workspace_id = current_workspace_stamp(for_write=False) or current_egress_workspace()
    warning = key_change_warning(workspace_id, last_pii_hash_key_version(source, destination))
    if warning:
        warning["details"]["columns"] = list((gate.get("details") or {}).get("columns") or [])
        out.setdefault("warnings", []).append(warning)
