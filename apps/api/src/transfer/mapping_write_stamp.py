"""Execute-path mapping stamps — Decision Kernel additive + transform enrich.

Extracted from engine.py (F8) so write orchestration stays a thin shell.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger("datawrap.transfer")


def schema_for_endpoint(destination: Any) -> str | None:
    """Return the SQL schema name implied by a database endpoint config."""
    try:
        from connectors.generic_sql import get_sql_schema

        from .adapters import resolve_connector_config

        cfg = resolve_connector_config(destination)
        return get_sql_schema(cfg)
    except Exception as exc:
        logger.warning("Destination schema resolution failed: %s", exc, exc_info=exc)
        return None


def enrich_mappings_with_types(
    mappings: list[dict],
    dest_types: dict[str, str] | None = None,
    column_types: dict[str, str] | None = None,
) -> list[dict]:
    if not mappings:
        return mappings
    try:
        from services.transform_resolver import attach_transforms_to_mappings

        return attach_transforms_to_mappings(
            mappings,
            column_types=column_types or {},
            dest_types=dest_types or {},
        )
    except Exception as exc:
        logger.warning("Transform enrichment failed: %s", exc, exc_info=exc)
    out = []
    for m in mappings:
        enriched = dict(m)
        tgt = m.get("target")
        if tgt and dest_types and tgt in dest_types:
            enriched["target_type"] = dest_types[tgt]
        out.append(enriched)
    return out


def stamp_additive_mappings_for_write(
    request: Any,
    mappings: list[dict],
    *,
    column_types: dict[str, str] | None = None,
    dest_types: dict[str, str] | None = None,
    sample_rows: list[dict] | None = None,
    dest_table_exists: bool | None = None,
) -> list[dict]:
    """Decision Kernel stamp for ADD COLUMN under backfill / create-new.

    Prevents Execute fail-closed after Validate when Map left ``target_type``
    blank for additive columns (Excel→PG partial Studio cliff).
    """
    if not mappings:
        return mappings
    try:
        from services.batch_progress import effective_backfill_new_fields
        from services.decision_kernel import stamp_additive_mapping_types
        from services.sync_cursor import is_overwrite_sync, resolve_effective_sync_mode
    except Exception as exc:
        # Fail-closed: never Execute create/backfill without Kernel invent surface.
        raise ValueError(
            "Decision Kernel additive stamp unavailable — refuse Execute rather "
            f"than invent destination types without SSOT ({exc})"
        ) from exc
    try:
        backfill = effective_backfill_new_fields(
            backfill_new_fields=bool(getattr(request, "backfill_new_fields", False)),
            schema_policy=str(getattr(request, "schema_policy", "") or ""),
            mappings=mappings,
        )
    except Exception:
        backfill = bool(getattr(request, "backfill_new_fields", False))
    samples_by_src: dict[str, list] = {}
    for row in sample_rows or []:
        if not isinstance(row, dict):
            continue
        for k, v in row.items():
            if v is None:
                continue
            samples_by_src.setdefault(str(k), []).append(v)
    for k in list(samples_by_src.keys()):
        samples_by_src[k] = samples_by_src[k][:32]
    dest_db = str(getattr(request.destination, "format", "") or "").lower()
    # Overwrite recreates the object, so the rows land in a new table whether or
    # not one is there now — CREATE TABLE invent authority applies every run.
    # Gating this on "existence unknown" meant it only helped the first run: on
    # the second the probe reported the table it had just created, the stamp was
    # skipped, every target type stayed pending and the schema-contract gate
    # refused. Any schedule on overwrite failed from its second tick onward.
    table_exists = dest_table_exists
    try:
        from services.sync_cursor import destination_exists_for_typing

        sync = resolve_effective_sync_mode(str(getattr(request, "sync_mode", "") or ""))
        table_exists = destination_exists_for_typing(
            sync,
            table_exists,
            has_live_column_types=bool(dest_types or {}),
            dest_format=dest_db,
        )
    except Exception:
        table_exists = dest_table_exists
    try:
        stamped, unstamped = stamp_additive_mapping_types(
            mappings,
            dest_db=dest_db,
            live_dest_types=dest_types or {},
            source_types=column_types or {},
            samples_by_source=samples_by_src,
            backfill_new_fields=bool(backfill),
            dest_table_exists=table_exists,
        )
    except Exception as exc:
        raise ValueError(
            f"Decision Kernel additive stamp failed — refuse Execute ({exc})"
        ) from exc
    needs_stamp = bool(backfill) or table_exists is False or any(
        bool(m.get("create_new"))
        or str(m.get("assignment_strategy") or "")
        in {"create_compatible_new", "identity_passthrough"}
        for m in mappings
        if isinstance(m, dict)
    )
    if unstamped and needs_stamp:
        raise ValueError(
            f"Additive column(s) {', '.join(unstamped[:5])} lack Map target_type "
            "under partial Studio — stamp on Map or disable backfill_new_fields."
        )
    return stamped
