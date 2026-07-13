"""Gate 8 reconciliation for the universal transfer engine."""

from __future__ import annotations

from typing import Any

from services.reconciliation import (
    checksum_rows,
    read_target_sample,
    reconcile,
    sample_compare_rows,
    verify_target,
)

from .adapters import records_to_matrix, resolve_connector_config
from .models import EndpointConfig


def run_reconciliation(
    *,
    endpoint: EndpointConfig,
    records: list[dict],
    columns: list[str],
    rows_written: int,
    writer_checksum: str,
    dest_summary: dict[str, Any],
    mappings: list[dict] | None = None,
) -> dict[str, Any]:
    """Verify row counts and checksums against the destination."""
    rejected_rows = int(dest_summary.get("rejected_rows", 0) or 0)
    source_rows = len(records) if records else rows_written + rejected_rows
    expected_written = max(source_rows - rejected_rows, 0)

    if endpoint.kind != "database":
        return {
            "passed": True,
            "message": "File export — reconciliation skipped",
            "source_rows": source_rows,
            "target_rows": rows_written,
            "rejected_rows": rejected_rows,
        }

    db_type = endpoint.format.lower()

    _, data_rows = records_to_matrix(records, columns)
    source_checksum = writer_checksum or checksum_rows(data_rows)
    cfg = resolve_connector_config(endpoint)
    schema = dest_summary.get("schema") or cfg.get("schema", "public")
    table_name = dest_summary.get("table") or endpoint.table or endpoint.collection or ""

    target_rows, target_checksum = verify_target(
        db_type,
        cfg,
        schema=schema,
        table_name=table_name,
        fallback_rows=rows_written,
        fallback_checksum=source_checksum,
    )

    mapping_dicts = mappings or [{"source": col, "target": col} for col in columns]
    sample_compare = None
    if records and table_name and db_type in {"postgresql", "mysql"}:
        mapped_targets = list(dict.fromkeys(
            str(m.get("target") or m.get("source") or "")
            for m in mapping_dicts if m.get("target") or m.get("source")
        ))
        target_sample = read_target_sample(
            db_type,
            cfg,
            schema=schema,
            table_name=table_name,
            columns=mapped_targets[:20] or None,
            limit=min(50, len(records)),
        )
        if target_sample:
            sample_compare = sample_compare_rows(
                records,
                target_sample,
                mapping_dicts,
                target_columns=mapped_targets,
                sample_size=min(50, len(records)),
            )

    # Destination tables may legitimately contain rows from earlier loads
    # (writers append). Fail only when the target holds fewer rows than we
    # just wrote — that indicates lost data from this transfer.
    if target_rows >= 0 and target_rows < rows_written:
        report = reconcile(
            source_rows=source_rows,
            target_rows=target_rows,
            source_checksum=source_checksum,
            target_checksum=target_checksum or source_checksum,
            rejected_rows=rejected_rows,
            sample_compare=sample_compare,
        )
        return report.to_dict()

    if records and sample_compare and not sample_compare.get("passed", True):
        report = reconcile(
            source_rows=source_rows,
            target_rows=target_rows if target_rows >= 0 else rows_written,
            source_checksum=source_checksum,
            target_checksum=target_checksum or source_checksum,
            rejected_rows=rejected_rows,
            sample_compare=sample_compare,
        )
        return report.to_dict()

    extra = target_rows - rows_written if target_rows >= rows_written else 0
    if not records and rows_written > 0 and target_rows >= rows_written:
        return {
            "passed": True,
            "message": f"Streaming transfer verified: {rows_written:,} rows written"
            + (f", {rejected_rows:,} rejected" if rejected_rows else "")
            + (f" (table now holds {target_rows:,} rows)" if target_rows > rows_written else ""),
            "source_rows": source_rows,
            "target_rows": target_rows if target_rows >= 0 else rows_written,
            "source_checksum": source_checksum,
            "target_checksum": target_checksum or source_checksum,
            "rejected_rows": rejected_rows,
        }

    return {
        "passed": rows_written == expected_written,
        "message": (
            f"Transfer verified: {rows_written}/{expected_written} expected rows written"
            + (f" ({rejected_rows} rejected)" if rejected_rows else "")
            + (f" (table now holds {target_rows} rows incl. {extra} pre-existing)" if extra else "")
        ),
        "source_rows": source_rows,
        "target_rows": target_rows if target_rows >= 0 else rows_written,
        "source_checksum": source_checksum,
        "target_checksum": target_checksum or source_checksum,
        "rejected_rows": rejected_rows,
    }
