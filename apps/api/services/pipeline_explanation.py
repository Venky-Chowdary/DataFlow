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
        "full_refresh_mirror": "Destination will be kept in sync with the source; missing rows are soft-deleted and reappearing rows are reactivated.",
        "mirror": "Destination will be kept in sync with the source; missing rows are soft-deleted and reappearing rows are reactivated.",
        "scd2": "A full history of every row version will be kept; changed attributes close the old version and insert a new current version.",
    }
    return notes.get(mode, f"Sync mode '{sync_mode}' will be applied.")


def _selected_contracts(request: Any) -> list[dict[str, Any]]:
    raw = getattr(request, "stream_contracts", None) or []
    out: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict) or item.get("selected") is False:
            continue
        name = str(item.get("name") or item.get("stream") or "").strip()
        if name:
            out.append(item)
    return out


def _summary_stream_names(summary: dict[str, Any] | None) -> list[str]:
    """Engine stream order. Empty unless this summary is a multi-table run."""
    if not isinstance(summary, dict) or summary.get("multi_stream") is not True:
        return []
    names: list[str] = []
    for item in summary.get("streams") or []:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or item.get("stream") or "").strip()
        if name and name not in names:
            names.append(name)
    return names if len(names) >= 2 else []


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
    names = _summary_stream_names(destination_summary)
    contracts = _selected_contracts(request) if names else []
    lines: list[str] = []
    if names:
        listed = ", ".join(names)
        lines.append(f"Transfer: {src.split(' (')[0]} ({listed}) → {dst.split(' (')[0]} ({listed}) — {len(names)} tables")
    else:
        lines.append(f"Transfer: {src} → {dst}")
    lines.append(f"Operation: {request.operation}, sync mode: {request.sync_mode}, validation: {request.validation_mode}")
    sync_note = _sync_mode_note(request.sync_mode)
    if names and sync_note == "Destination will be cleared and fully replaced with source data.":
        sync_note = "Each selected table is cleared and fully replaced with that table's source rows."
    lines.append(f"Sync behavior: {sync_note}")

    if names and len(contracts) >= 2:
        by_name = {str(item.get("name") or item.get("stream") or "").strip(): item for item in contracts}
        ordered = [by_name[name] for name in names if name in by_name]
        ordered.extend(item for item in contracts if item not in ordered)
        lines.append("Schema mapping by table:")
        for contract in ordered:
            cname = str(contract.get("name") or contract.get("stream") or "").strip()
            maps = [m for m in (contract.get("mappings") or []) if isinstance(m, dict)]
            cols = [str(m.get("source")) for m in maps if m.get("source")]
            shown = ", ".join(cols[:10]) + ("..." if len(cols) > 10 else "")
            column_word = "column" if len(cols) == 1 else "columns"
            lines.append(f"{cname}: {len(cols)} {column_word}" + (f" ({shown})" if shown else ""))
            schema = {
                str(m.get("source")): str(m.get("source_type") or "inferred")
                for m in maps
                if m.get("source")
            }
            for mapping in maps[:12]:
                lines.append(f"  • {_mapping_line(mapping, schema)}")
            if len(maps) > 12:
                lines.append(f"  • ... and {len(maps) - 12} more mappings")
    else:
        if names:
            last = str((destination_summary or {}).get("table") or names[-1]).strip() or names[-1]
            lines.append(f"Column sample and schema mapping are the last stream ({last}), not every table.")
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
