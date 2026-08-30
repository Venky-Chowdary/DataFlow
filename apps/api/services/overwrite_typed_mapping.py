"""Overwrite create-new typed mapping.

Overwrite sync recreates the destination table from the source shape. Identity
maps alone would copy source cells verbatim — a currency column ($1,000.00 /
€2.000,50) would land as raw text instead of a normalised decimal. This helper
runs the same create-new mapping pipeline the append path uses so typed
transforms (decimal/date/boolean) are inferred from samples, while identity
byte-copy still wins for clean native columns (infer returns "none") and
create-new never renames columns.

Extracted from ``engine.py`` to keep that module under its F8 size budget.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def build_overwrite_create_new_mappings(
    *,
    columns: list[str],
    schema: dict[str, Any],
    sample_rows: list[dict[str, Any]] | None,
    request: Any,
) -> list[dict[str, Any]]:
    """Build create-new mappings for an overwrite sync.

    Falls back to identity maps (with CREATE authority stamped) if the typed
    pipeline cannot run, so overwrite never blocks on inference failure.
    """
    from services.column_case import column_type_or_none
    from src.transfer.type_mapper import default_mappings

    mappings = default_mappings(columns)
    for m in mappings:
        if not isinstance(m, dict):
            continue
        # Property 2: identity maps must satisfy create-new gates — stamp CREATE
        # authority so the Kernel invents target_type instead of blocking with
        # "lack Map target_type under partial Studio".
        m["create_new"] = True
        m.setdefault("assignment_strategy", "create_compatible_new")
        src_name = str(m.get("source") or "")
        if src_name and not str(m.get("source_type") or "").strip():
            # Leave blank rather than stamping "TEXT" for a column the
            # introspected schema does not describe: a wrong declared source
            # type outranks sample evidence at invent and lands the whole table
            # as text.
            declared = column_type_or_none(schema, src_name)
            if declared:
                m["source_type"] = declared

    try:
        from services.data_profiler import source_types_are_authoritative
        from services.mapping_pipeline import run_mapping_pipeline
        from services.preflight_service import confidence_threshold_for_mode
        from services.value_serializer import cell_to_string

        head = (sample_rows or [])[:8]
        source_schemas = [
            {
                "name": c,
                "inferred_type": schema.get(c, "string"),
                "samples": [cell_to_string(r.get(c, "")) for r in head],
            }
            for c in columns
        ]
        source_samples = {
            c: [cell_to_string(r.get(c, "")) for r in head] for c in columns
        }
        overwrite_result = run_mapping_pipeline(
            source_columns=columns,
            target_columns=[],
            source_schemas=source_schemas,
            target_schemas=None,
            file_format=request.source.format,
            confidence_threshold=confidence_threshold_for_mode(request.validation_mode),
            source_samples=source_samples,
            validation_mode=request.validation_mode,
            use_llm=False,
            schema_policy=request.schema_policy,
            destination_db_type=(request.destination.format or "").lower(),
            source_db_type=(request.source.format or "").lower(),
            destination_table_exists=False,
            source_types_authoritative=source_types_are_authoritative(
                request.source.kind or "",
                request.source.format or "",
            ),
        )
        overwrite_auto = overwrite_result.get("mappings")
        if (
            overwrite_auto
            and isinstance(overwrite_auto, list)
            and any(mm.get("source") for mm in overwrite_auto)
        ):
            for mm in overwrite_auto:
                # Create-new keeps source column names (never canonicalise into a
                # shape the fresh table does not have).
                mm["target"] = mm.get("source") or mm.get("target", "")
                mm["create_new"] = True
            mappings = overwrite_auto
    except Exception as exc:
        logger.warning(
            "Overwrite create-new typed mapping failed: %s; identity maps kept", exc
        )
    return mappings
