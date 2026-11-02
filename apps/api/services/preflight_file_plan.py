"""File-plan helpers extracted from :mod:`services.preflight_service`.

These pure helpers name drift columns, surface in-file duplicate-collapse
advisories, and resolve the dedupe / duplicate policies a schedule stamped
on a file transfer. They were extracted verbatim (Phase F8 god-module
freeze) so ``preflight_service`` stays within its line budget; behaviour
is unchanged.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def _fit_scan_deadline(
    absolute: float | None,
    seconds: float | None,
) -> float | None:
    """Studio budget is scan wall-clock, not time spent on sample gates first."""
    if seconds is not None and float(seconds) > 0:
        import time

        return time.monotonic() + float(seconds)
    return absolute

def _drift_column_phrase(evolution: dict[str, Any] | None) -> str:
    """Name the columns a drift gate is about. A bare 'schema change' is not enough."""
    names: list[str] = []
    seen: set[str] = set()

    def add(raw: object) -> None:
        name = str(raw or "").strip()
        key = name.lower()
        if not name or key in seen:
            return
        seen.add(key)
        names.append(name)

    evo = evolution or {}
    for bucket in ("hard_breaking", "additive", "soft_net_additive"):
        for item in evo.get(bucket) or []:
            if isinstance(item, dict):
                add(item.get("column") or item.get("to") or item.get("from") or item.get("source"))
            else:
                add(item)
    for item in evo.get("unmapped_sources") or []:
        if isinstance(item, dict):
            add(item.get("column") or item.get("source"))
        else:
            add(item)
    if not names:
        return ""
    shown = ", ".join(names[:8])
    extra = f" (+{len(names) - 8} more)" if len(names) > 8 else ""
    return f" Column(s): {shown}{extra}."

def _lift_in_file_collapse_advisories(out: dict[str, Any]) -> None:
    """Surface a passing in-file collapse where Pilot reads warnings.

    Gate details are dropped from the staged preflight. The advisory has to
    travel on the top-level warning list, with ``blocks_transfer`` false, or
    the operator only sees a silent pass.
    """
    warnings = out.get("warnings")
    if not isinstance(warnings, list):
        warnings = []
        out["warnings"] = warnings
    seen = {
        str(item.get("message") if isinstance(item, dict) else item)
        for item in warnings
    }
    for gate in out.get("gates") or []:
        if not isinstance(gate, dict) or gate.get("id") != "g9_data_integrity":
            continue
        if str(gate.get("status") or "") == "block":
            continue
        for item in (gate.get("details") or {}).get("warnings") or []:
            text = str(item or "").strip()
            if "counted once" not in text.lower():
                continue
            if text in seen:
                continue
            seen.add(text)
            warnings.insert(
                0,
                {
                    "id": "in_file_duplicate_collapsed",
                    "message": text,
                    "blocks_transfer": False,
                    "gate": "g9_data_integrity",
                },
            )

def _plan_in_file_dedupe(source_config: dict[str, Any] | None) -> str:
    """``keep_last`` or ``block`` from the source config the schedule stamped."""
    cfg = source_config if isinstance(source_config, dict) else {}
    extra = cfg.get("extra") if isinstance(cfg.get("extra"), dict) else {}
    raw = cfg.get("in_file_dedupe")
    if raw in (None, "") and isinstance(extra, dict):
        raw = extra.get("in_file_dedupe")
    token = str(raw or "").strip().lower().replace("-", "_")
    # Unset follows the sync mode. Upsert collapses in-file copies. An explicit
    # block still refuses them. Only keep_last used to be recognized, so a
    # missing value was stored as block and stopped dedup.
    if token == "keep_last":
        return "keep_last"
    if token == "block":
        return "block"
    return ""

def _plan_duplicate_policy(*configs: Mapping[str, Any] | None) -> str:
    """upsert / dedupe_skip / append from the source or destination the schedule stamped."""
    from services.duplicate_policy import normalize_duplicate_policy

    for cfg in configs:
        if not isinstance(cfg, Mapping):
            continue
        extra = cfg.get("extra") if isinstance(cfg.get("extra"), Mapping) else {}
        raw = str(cfg.get("duplicate_policy") or extra.get("duplicate_policy") or "")
        if raw.strip():
            return normalize_duplicate_policy(raw)
    return ""
