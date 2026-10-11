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


def _has_mixed_temporal_timezone_polarity(values: list[Any]) -> bool:
    from connectors.sql_temporal import (
        input_has_timezone,
        parse_sql_date,
        parse_sql_datetime,
    )

    has_aware = False
    has_zoneless = False
    for value in values:
        if value is None or (isinstance(value, str) and not value.strip()):
            continue
        if parse_sql_datetime(value) is None and parse_sql_date(value) is None:
            continue
        if input_has_timezone(value):
            has_aware = True
        else:
            has_zoneless = True
        if has_aware and has_zoneless:
            return True
    return False


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
        mixed_temporal_columns = {
            c
            for c in columns
            if _has_mixed_temporal_timezone_polarity(
                [row.get(c) for row in head if isinstance(row, dict)]
            )
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
            for mapping_index, mm in enumerate(overwrite_auto):
                # Create-new keeps source column names (never canonicalise into a
                # shape the fresh table does not have).
                mm["target"] = mm.get("source") or mm.get("target", "")
                mm["create_new"] = True
                source = str(mm.get("source") or "")
                if source in mixed_temporal_columns:
                    from services.mapping_proof import stamp_mapping_fidelity
                    from services.type_system import (
                        create_new_mapping_target_type,
                        materialize_dest_ddl,
                    )

                    destination_db = (request.destination.format or "").lower()
                    text_type = create_new_mapping_target_type(
                        "VARCHAR",
                        destination_db,
                        source_db=(request.source.format or "").lower(),
                    )
                    if not text_type:
                        raise ValueError(
                            "Create-new VARCHAR carrier unavailable for mixed "
                            f"temporal field {source}"
                        )
                    mm.update(
                        {
                            "source_type": "VARCHAR",
                            "target_type": text_type,
                            "dest_native_type": materialize_dest_ddl(
                                destination_db, text_type, "VARCHAR"
                            ),
                            "transform": "none",
                            "reasoning": (
                                "Mixed zone-bearing and zoneless temporal samples "
                                "use the existing VARCHAR carrier to preserve "
                                "their serialized values."
                            ),
                        }
                    )
                    for key in (
                        "create_new_risks",
                        "type_narrowing",
                        "conversion_class",
                        "invents_capacity",
                        "requires_risk_contract",
                    ):
                        mm.pop(key, None)
                    mm = stamp_mapping_fidelity(
                        [mm],
                        source_types={source: "VARCHAR"},
                        target_types={source: text_type},
                        destination_db_type=destination_db,
                        dest_table_exists=False,
                        samples_by_source={source: source_samples.get(source, [])},
                    )[0]
                    from services.create_new_risk_stamp import (
                        apply_create_new_risk_stamps,
                    )

                    mm = apply_create_new_risk_stamps(
                        [mm],
                        destination_db,
                        source_samples={source: source_samples.get(source, [])},
                        dest_table_exists=False,
                        source_db_type=(request.source.format or "").lower(),
                    )[0]
                    overwrite_auto[mapping_index] = mm
            mappings = overwrite_auto
    except Exception as exc:
        logger.warning(
            "Overwrite create-new typed mapping failed: %s; identity maps kept", exc
        )
    return mappings
