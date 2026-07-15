"""Human-readable pipeline narrative for transfer results."""

from __future__ import annotations

from typing import Any


def _fmt_endpoint(ep: Any) -> str:
    name = ep.table or ep.collection or ep.schema or ep.database or ""
    if name:
        return f"{ep.kind}/{ep.format} ({name})"
    return f"{ep.kind}/{ep.format}"


def _describe_type(column: str, schema: dict[str, str] | None) -> str:
    return (schema or {}).get(column, "inferred") or "inferred"


def _sync_mode_note(sync_mode: str) -> str:
    mode = (sync_mode or "full_refresh_overwrite").lower()
    notes = {
        "append": "New rows will be inserted without changing existing destination data.",
        "upsert": "Rows will be merged by primary key; existing matches are updated and new rows are inserted.",
        "incremental": "Only rows newer than the last watermark will be loaded.",
        "cdc": "Changed rows since the last watermark will be applied, including soft deletes.",
        "full_refresh_overwrite": "Destination will be cleared and fully replaced with source data.",
        "overwrite": "Destination will be cleared and fully replaced with source data.",
    }
    return notes.get(mode, f"Sync mode '{sync_mode}' will be applied.")


def _mapping_line(m: dict[str, Any], schema: dict[str, str] | None) -> str:
    src = m.get("source") or "?"
    tgt = m.get("target") or "?"
    transform = m.get("transform") or ""
    confidence = m.get("confidence")
    src_type = _describe_type(src, schema)
    parts = [f"{src} ({src_type}) → {tgt}"]
    if transform:
        parts.append(f"transform: {transform}")
    if confidence is not None:
        parts.append(f"confidence: {confidence:.0%}")
    return ", ".join(parts)


def build_pipeline_explanation(
    *,
    request: Any,
    columns: list[str],
    source_schema: dict[str, str] | None,
    mappings: list[dict[str, Any]],
    reconciliation: dict[str, Any] | None,
    destination_summary: dict[str, Any],
    validation_plan: dict[str, Any] | None = None,
    rows_written: int | None = None,
    rejected_rows: int | None = None,
    error: str | None = None,
) -> str:
    """Generate a plain-English description of what the pipeline did."""
    src = _fmt_endpoint(request.source)
    dst = _fmt_endpoint(request.destination)
    lines: list[str] = []
    lines.append(f"Transfer: {src} → {dst}")
    lines.append(f"Operation: {request.operation}, sync mode: {request.sync_mode}, validation: {request.validation_mode}")
    lines.append(f"Sync behavior: {_sync_mode_note(request.sync_mode)}")

    lines.append(
        f"Source inferred {len(columns)} columns: {', '.join(columns[:10])}"
        + ("..." if len(columns) > 10 else "")
    )
    if source_schema:
        type_sample = ", ".join(f"{c}: {_describe_type(c, source_schema)}" for c in columns[:5])
        lines.append(f"Sample types — {type_sample}")

    if mappings:
        mapped = [f"  • {_mapping_line(m, source_schema)}" for m in mappings[:20]]
        if len(mappings) > 20:
            mapped.append(f"  • ... and {len(mappings) - 20} more mappings")
        lines.append("Schema mapping:")
        lines.extend(mapped)
    else:
        lines.append("Schema mapping: identity (source columns copied to target)")

    rows = rows_written if rows_written is not None else destination_summary.get("rows_written", 0)
    rej = rejected_rows if rejected_rows is not None else destination_summary.get("rejected_rows", 0)
    lines.append(f"Rows written: {rows:,}" + (f", rejected: {rej:,}" if rej else ""))

    if reconciliation:
        status = "passed" if reconciliation.get("passed") else "failed"
        msg = reconciliation.get("message", status)
        lines.append(f"Reconciliation: {msg}")
        if reconciliation.get("source_checksum") and reconciliation.get("target_checksum"):
            lines.append(
                f"  source checksum: {reconciliation['source_checksum']}, "
                f"target checksum: {reconciliation['target_checksum']}"
            )

    if validation_plan:
        plan_notes = validation_plan.get("notes") or []
        if plan_notes:
            lines.append("Validation plan:")
            for note in plan_notes[:5]:
                lines.append(f"  • {note}")

    warnings = destination_summary.get("warnings") if destination_summary else None
    if warnings:
        lines.append("Data-quality / pipeline warnings:")
        for w in warnings[:5]:
            lines.append(f"  • {w}")

    if error:
        lines.append(f"Error: {error}")

    return "\n".join(lines)
