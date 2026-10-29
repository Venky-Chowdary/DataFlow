from __future__ import annotations

from typing import Any

from src.transfer import engine as _engine
from src.transfer.type_mapper import default_mappings
from src.transfer.engine import (
    TransferRequest,
    confidence_threshold_for_mode,
    destination_exists_for_shape,
    is_overwrite_sync,
    logger,
    resolve_effective_sync_mode,
    schema_registry,
)
from services.value_serializer import wire_cell_text
from services.timezone_policy import (
    apply_declared_source_timezone,
    declare_zone_on_schema,
    validate_source_timezone,
)

def _auto_map(
    request: TransferRequest,
    columns: list[str],
    schema: dict[str, str],
    sample_rows: list[dict] | None = None,
    job_id: str = "",
) -> list[dict]:
    """Generate destination-aware mappings when no mapping contract was supplied.

    For append/upsert/merge into an existing target, the destination schema is
    introspected and the semantic mapper aligns source columns to target columns.
    For full-refresh/overwrites into a new target, identity mappings are used so
    the destination can be created from the source shape.
    """
    mappings: list[dict] | None = None
    from services.source_schema_authority import declared_decimal_for_dest

    create_schema = declared_decimal_for_dest(
        schema, request.source, request.destination.format
    )
    source_schemas = [
        {
            "name": column,
            "inferred_type": create_schema.get(column, "string"),
            "samples": [
                wire_cell_text(row.get(column, ""))
                for row in (sample_rows or [])[:8]
            ],
        }
        for column in columns
    ]
    zone, zone_error = validate_source_timezone(request.source_timezone)
    if zone_error:
        raise ValueError(zone_error)
    if zone:
        source_schemas, _ = declare_zone_on_schema(
            source_schemas,
            zone,
            source_db=(request.source.format or "").lower(),
            source_kind=(request.source.kind or "").lower(),
        )
        schema.update({
            str(row.get("name") or ""): str(row.get("inferred_type") or "")
            for row in source_schemas
            if row.get("name")
        })
    source_samples = {
        column: [
            wire_cell_text(row.get(column, ""))
            for row in (sample_rows or [])[:8]
        ]
        for column in columns
    }

    if request.mappings:
        mappings = request.mappings
    elif request.destination.kind != "database":
        mappings = default_mappings(columns)
    else:
        sync_mode = resolve_effective_sync_mode(request.sync_mode)
        if is_overwrite_sync(sync_mode):
            from services.overwrite_typed_mapping import (
                build_overwrite_create_new_mappings,
            )

            mappings = build_overwrite_create_new_mappings(
                columns=columns,
                schema=schema,
                sample_rows=sample_rows,
                request=request,
            )
            # Dest-exists overwrite must not invent extra dest columns. Extra
            # source stays unaccounted so G13 blocks instead of silent drop
            # or ADD COLUMN from source position.
            target_schema, probe_exists = _engine._destination_schema_probe(
                request.destination,
                sync_mode=sync_mode,
            )
            dest_exists = destination_exists_for_shape(
                probe_exists,
                dest_format=str(getattr(request.destination, "format", "") or ""),
            )
            if dest_exists is True:
                from services.mapping_constraints import (
                    dest_exists_write_column_names,
                    retain_dest_exists_write_mappings,
                )

                # Overwrite dest-exists returns empty types for typing, but
                # extra source must still stay unaccounted (G13) using the
                # replaced column names stamped on destination.extra.
                dest_cols = dest_exists_write_column_names(
                    target_schema=target_schema,
                    dest_extra=getattr(request.destination, "extra", None) or {},
                )
                if dest_cols:
                    mappings = retain_dest_exists_write_mappings(
                        mappings, dest_cols
                    )
        else:
            target_schema, probe_exists = _engine._destination_schema_probe(
                request.destination,
                sync_mode=sync_mode,
            )
            # Shape/existence is the raw probe. Typing may collapse
            # exists+empty-catalog to "no types to bind" — that must not
            # invent create-new on a listed dest table (G15 pending).
            dest_exists = destination_exists_for_shape(
                probe_exists,
                dest_format=str(getattr(request.destination, "format", "") or ""),
            )
            if not target_schema:
                # Empty columns: only invent identity create-new when the object
                # is confirmed missing. Existing/unknown → pending via mapper.
                try:
                    from services.mapping_pipeline import run_mapping_pipeline

                    from services.data_profiler import source_types_are_authoritative

                    result = run_mapping_pipeline(
                        source_columns=columns,
                        target_columns=[],
                        source_schemas=source_schemas,
                        target_schemas=None,
                        file_format=request.source.format,
                        confidence_threshold=confidence_threshold_for_mode(
                            request.validation_mode
                        ),
                        source_samples=source_samples,
                        validation_mode=request.validation_mode,
                        use_llm=False,
                        schema_policy=request.schema_policy,
                        destination_db_type=(request.destination.format or "").lower(),
                        source_db_type=(request.source.format or "").lower(),
                        destination_table_exists=dest_exists,
                        source_types_authoritative=source_types_are_authoritative(
                            request.source.kind or "",
                            request.source.format or "",
                        ),
                        job_id=job_id,
                    )
                    auto = result.get("mappings")
                    if (
                        auto
                        and isinstance(auto, list)
                        and any(m.get("source") for m in auto)
                    ):
                        # New tables should keep source column names; the semantic
                        # mapper may canonicalize (e.g. birth_date -> date_of_birth)
                        # which is only safe when the target schema is known.
                        # Preserve the mapper's inferred target_type.
                        if dest_exists is False:
                            for m in auto:
                                m["target"] = m.get("source") or m.get("target", "")
                                m["create_new"] = True
                        mappings = auto
                    elif dest_exists is False:
                        mappings = default_mappings(columns)
                except Exception as exc:
                    logger.warning(
                        "Empty-dest schema map failed: %s; fallback identity only if missing",
                        exc,
                    )
                    if dest_exists is False:
                        mappings = default_mappings(columns)
            else:
                # For MongoDB append/upsert, do not let the semantic mapper overwrite _id
                # unless the source literally contains an _id column or the user supplied a mapping.
                if (
                    request.destination.format == "mongodb"
                    and not is_overwrite_sync(sync_mode)
                    and "_id" not in columns
                ):
                    target_schema = {
                        k: v for k, v in target_schema.items() if k != "_id"
                    }

                try:
                    from services.mapping_pipeline import run_mapping_pipeline

                    target_columns = list(target_schema.keys())
                    target_schemas = [
                        {
                            "name": c,
                            "inferred_type": target_schema.get(c, "string"),
                            "samples": [],
                        }
                        for c in target_columns
                    ]
                    from services.data_profiler import source_types_are_authoritative

                    result = run_mapping_pipeline(
                        source_columns=columns,
                        target_columns=target_columns,
                        source_schemas=source_schemas,
                        target_schemas=target_schemas,
                        file_format=request.source.format,
                        confidence_threshold=confidence_threshold_for_mode(
                            request.validation_mode
                        ),
                        source_samples=source_samples,
                        validation_mode=request.validation_mode,
                        use_llm=False,
                        schema_policy=request.schema_policy,
                        # Without the dialect the fidelity verdict is judged in a
                        # vacuum: DECIMAL→TEXT reads as loss, when TEXT is the
                        # exact-digit carrier our own DDL picks on SQLite. Every
                        # existing-destination route shares this call.
                        destination_db_type=(request.destination.format or "").lower(),
                        source_db_type=(request.source.format or "").lower(),
                        destination_table_exists=dest_exists,
                        source_types_authoritative=source_types_are_authoritative(
                            request.source.kind or "",
                            request.source.format or "",
                        ),
                        job_id=job_id,
                    )
                    auto = result.get("mappings")
                    if (
                        auto
                        and isinstance(auto, list)
                        and any(m.get("source") for m in auto)
                    ):
                        mapped_sources = {str(m.get("source")) for m in auto}
                        if request.backfill_new_fields:
                            for c in columns:
                                if c not in mapped_sources:
                                    auto.append(
                                        {"source": c, "target": c, "confidence": 0.95}
                                    )
                        mappings = auto
                except Exception as exc:
                    logger.warning(
                        "Auto-mapping failed: %s; falling back to identity mappings",
                        exc,
                    )

    if mappings is None:
        mappings = default_mappings(columns)

    if zone:
        mappings = apply_declared_source_timezone(
            source_schemas,
            mappings,
            zone,
            (request.destination.format or "").lower(),
        )
        schema.update({
            str(row.get("name") or ""): str(row.get("inferred_type") or "")
            for row in source_schemas
            if row.get("name")
        })
        request.column_types = dict(schema)
        request.mappings = [dict(mapping) for mapping in mappings]
    _record_schema_and_lineage(request, mappings, schema, job_id)
    return mappings


def _record_schema_and_lineage(
    request: TransferRequest,
    mappings: list[dict[str, Any]],
    schema: dict[str, str],
    job_id: str,
) -> None:
    """Register source/target schemas and column-level lineage for this job."""
    src_id = (
        request.source.connector_id
        or request.source.host
        or (request.source_filename if request.source.kind == "file" else "")
        or f"{request.source.kind}:{request.source.format}"
    )
    src_object = (
        request.source.table
        or request.source.collection
        or (request.source_filename if request.source.kind == "file" else "")
        or "source"
    )
    dst_id = (
        request.destination.connector_id
        or request.destination.host
        or f"{request.destination.kind}:{request.destination.format}"
    )
    dst_object = (
        request.destination.table
        or request.destination.collection
        or f"{request.destination.format}_export"
    )

    source_columns = [
        {"name": name, "type": (schema.get(name) or "string"), "primary_key": False}
        for name in schema.keys()
    ]
    schema_registry.register_schema(
        columns=source_columns,
        connector_type=request.source.format,
        connector_id=src_id,
        object_name=src_object,
        job_id=job_id,
        source_of_truth=True,
    )

    # Build a best-effort target schema from the mappings.
    target_columns = []
    for m in mappings:
        src = str(m.get("source", "")).strip()
        tgt = str(m.get("target", "")).strip()
        if not src or not tgt:
            continue
        target_columns.append(
            {
                "name": tgt,
                "type": schema.get(src, "string"),
                "primary_key": False,
            }
        )
    if target_columns:
        schema_registry.register_schema(
            columns=target_columns,
            connector_type=request.destination.format,
            connector_id=dst_id,
            object_name=dst_object,
            job_id=job_id,
        )
        schema_registry.record_lineage(
            source={
                "connector_type": request.source.format,
                "connector_id": src_id,
                "object_name": src_object,
            },
            target={
                "connector_type": request.destination.format,
                "connector_id": dst_id,
                "object_name": dst_object,
            },
            mappings=mappings,
            job_id=job_id,
        )
