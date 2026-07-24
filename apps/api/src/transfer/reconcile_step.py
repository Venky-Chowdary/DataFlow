"""Gate 8 reconciliation for the universal transfer engine."""

from __future__ import annotations

from typing import Any

from connectors.writer_common import build_mapped_rows, resolve_target_columns
from services.reconciliation import (
    checksum_rows,
    read_target_sample,
    reconcile,
    sample_compare_rows,
    verify_target,
)

from .adapters import records_to_matrix, resolve_connector_config
from .models import EndpointConfig


def _compute_source_checksum(
    records: list[dict],
    columns: list[str],
    mappings: list[dict],
    source_schema: dict[str, str] | None,
    writer_checksum: str,
    target_cols: list[str] | None = None,
) -> str:
    """Return the writer checksum, or recompute it from mapped source rows."""
    if writer_checksum:
        return writer_checksum
    if not records:
        return ""
    _, data_rows = records_to_matrix(records, columns)
    if target_cols is None:
        target_cols, _ = resolve_target_columns(mappings, source_schema or {}, preserve_case=True)
    mapped_rows, _ = build_mapped_rows(
        headers=columns,
        data_rows=data_rows,
        mappings=mappings,
        target_cols=target_cols,
        column_types=source_schema or {},
        error_policy="quarantine",
        preserve_case=True,
    )
    return checksum_rows(mapped_rows, target_cols)


def _mapped_targets(mappings: list[dict], columns: list[str]) -> list[str]:
    """Return the ordered list of target column names used for reconciliation."""
    targets = list(dict.fromkeys(
        str(m.get("target") or m.get("source") or "")
        for m in mappings if m.get("target") or m.get("source")
    ))
    return targets or columns


def _sort_key_for_columns(targets: list[str], mappings: list[dict] | None = None) -> str | None:
    """Pick a stable key for sample alignment.

    Prefer real identity columns (id / *_id / code / uuid) over the first mapped
    column — airports-like tables have no id, and aligning on ``city`` is weak
    when the destination already holds prior append loads.
    """
    if not targets:
        return None
    lower_map = {c.lower(): c for c in targets}
    # Operator / contract primary key from mapping metadata.
    for m in mappings or []:
        for key in ("primary_key", "is_primary_key", "identity"):
            if m.get(key) in (True, "true", "1", 1):
                tgt = str(m.get("target") or "").strip()
                if tgt and tgt.lower() in lower_map:
                    return lower_map[tgt.lower()]
    preferred = (
        "id",
        "uuid",
        "guid",
        "code",
        "pk",
        "airport_code",
        "iata",
        "icao",
    )
    for name in preferred:
        if name in lower_map:
            return lower_map[name]
    for c in targets:
        cl = c.lower()
        if cl.endswith("_id") or cl.endswith("_uuid") or cl.endswith("_code"):
            return c
    return targets[0]


def _source_key_values(
    records: list[dict],
    *,
    sort_key: str | None,
    mappings: list[dict],
    limit: int = 50,
) -> list[Any]:
    """Extract distinct source-side key values for keyed destination sample reads."""
    if not sort_key or not records:
        return []
    source_sort_key = sort_key
    sk = sort_key.lower()
    for m in mappings:
        if str(m.get("target") or "").lower() == sk and m.get("source"):
            source_sort_key = str(m["source"])
            break
    seen: set[str] = set()
    values: list[Any] = []
    for rec in records:
        if not isinstance(rec, dict):
            continue
        raw = rec.get(source_sort_key)
        if raw is None and source_sort_key != sort_key:
            raw = rec.get(sort_key)
        if raw is None or raw == "":
            continue
        marker = str(raw)
        if marker in seen:
            continue
        seen.add(marker)
        values.append(raw)
        if len(values) >= limit:
            break
    return values


def run_reconciliation(
    *,
    endpoint: EndpointConfig,
    records: list[dict],
    columns: list[str],
    rows_written: int,
    writer_checksum: str,
    dest_summary: dict[str, Any],
    mappings: list[dict] | None = None,
    source_schema: dict[str, str] | None = None,
    validation_mode: str = "strict",
) -> dict[str, Any]:
    """Verify row counts and checksums against the destination."""
    rejected_rows = int(dest_summary.get("rejected_rows", 0) or 0)
    coerced_null_rows = int(dest_summary.get("coerced_null_rows", 0) or 0)
    # Coerced rows are KEPT (a cell became NULL); only genuinely dropped rows are
    # absent from the destination. Reconstructing the source count from the write
    # side must therefore add back only the dropped rows, not the coerced ones.
    dropped_rows = max(rejected_rows - coerced_null_rows, 0)
    # Prefer independent source accounting when the caller provides it
    # (streaming paths should pass source_row_count from the read side).
    source_row_count = dest_summary.get("source_row_count")
    if isinstance(source_row_count, int) and source_row_count >= 0:
        source_rows = source_row_count
    else:
        source_rows = len(records) if records else rows_written + dropped_rows
    expected_written = max(source_rows - dropped_rows, 0)

    if endpoint.kind != "database":
        return {
            "passed": True,
            "message": "File export — reconciliation skipped",
            "source_rows": source_rows,
            "target_rows": rows_written,
            "rejected_rows": rejected_rows,
            "coerced_null_rows": coerced_null_rows,
        }

    db_type = endpoint.format.lower()
    cfg = resolve_connector_config(endpoint)
    from services.dialect_profiles import schema_from_cfg

    schema = dest_summary.get("schema") or schema_from_cfg(db_type, cfg)
    table_name = dest_summary.get("table") or endpoint.table or endpoint.collection or ""

    mapping_dicts = mappings or [{"source": col, "target": col} for col in columns]
    if source_schema:
        try:
            from services.transform_resolver import attach_transforms_to_mappings

            mapping_dicts = attach_transforms_to_mappings(
                mapping_dicts,
                column_types=source_schema,
                dest_types={},
            )
        except Exception:
            pass

    target_cols = _mapped_targets(mapping_dicts, columns)
    source_checksum = _compute_source_checksum(
        records, columns, mapping_dicts, source_schema, writer_checksum, target_cols=target_cols
    )

    # Mirror (inferred-delete) and SCD2 transfers already compute an active-row
    # checksum while applying history/soft deletes; use it directly so closed or
    # deleted rows do not fail strict reconciliation. The streaming staging path
    # surfaces these at the top level; the buffered database path nests them
    # under the "scd2"/"mirror" keys.
    active_checksum = (dest_summary or {}).get("active_checksum")
    active_rows = (dest_summary or {}).get("active_rows") if active_checksum else None
    if not active_checksum:
        for sub_key in ("mirror", "scd2"):
            sub_summary = (dest_summary or {}).get(sub_key)
            if sub_summary and sub_summary.get("active_checksum"):
                active_rows = sub_summary.get("active_rows")
                active_checksum = sub_summary["active_checksum"]
                break
    if active_checksum:
        report = reconcile(
            source_rows=source_rows,
            target_rows=int(active_rows or 0),
            source_checksum=source_checksum,
            target_checksum=active_checksum,
            rejected_rows=rejected_rows,
            strict_checksum=True,
            allow_extra_rows=False,
            sample_compare=None,
            coerced_null_rows=coerced_null_rows,
        )
        return report.to_dict()

    # Request a real read-back; if the verifier is unavailable we will detect
    # the negative row count and surface a softer "writer only" result.
    # Strict/maximum modes verify the whole target table; balanced samples 5000 rows.
    checksum_limit = 0 if validation_mode in ("strict", "maximum") else 5000
    target_rows, target_checksum = verify_target(
        db_type,
        cfg,
        schema=schema,
        table_name=table_name,
        fallback_rows=-1,
        fallback_checksum="",
        target_columns=target_cols,
        limit=checksum_limit,
    )

    strict_checksum = validation_mode in ("strict", "maximum")

    # Streaming transfers pass records=[] — use the bounded sample the writer
    # stashed so append/upsert Gate-8 can still prove key-aligned fidelity.
    sample_records = list(records or [])
    if not sample_records:
        stashed = (
            dest_summary.get("reconcile_sample")
            or dest_summary.get("sample_records")
            or []
        )
        if isinstance(stashed, list):
            sample_records = [r for r in stashed if isinstance(r, dict)]

    sample_compare = None
    if sample_records and table_name and target_cols:
        sort_key = _sort_key_for_columns(target_cols, mapping_dicts)
        key_values = _source_key_values(
            sample_records,
            sort_key=sort_key,
            mappings=mapping_dicts,
            limit=min(50, len(sample_records)),
        )
        target_sample = read_target_sample(
            db_type,
            cfg,
            schema=schema,
            table_name=table_name,
            columns=target_cols[:20] or None,
            limit=min(50, len(sample_records)),
            sort_key=sort_key,
            key_values=key_values or None,
        )
        if target_sample:
            sample_compare = sample_compare_rows(
                sample_records,
                target_sample,
                mapping_dicts,
                target_columns=target_cols,
                sample_size=min(50, len(sample_records)),
                sort_key=sort_key,
            )

    # No read-back verifier available for this destination.
    if target_rows < 0:
        # dest_only sinks (pgvector, milvus, …) have no independent SQL read-back
        # by design — fail-closed strict mode would ban every production write.
        # Accept writer-ack when row counts match; surface that read-back was N/A.
        dest_only = False
        try:
            from src.transfer.connector_capabilities import _DRIVER_CAPS

            dest_only = bool(_DRIVER_CAPS.get(db_type, {}).get("dest_only"))
        except Exception:
            dest_only = False
        if strict_checksum and not dest_only:
            return {
                "passed": False,
                "message": (
                    "Strict reconciliation requires an independent destination read-back; "
                    f"verifier unavailable for '{db_type}'"
                ),
                "source_rows": source_rows,
                "target_rows": -1,
                "source_checksum": source_checksum,
                "target_checksum": "",
                "rejected_rows": rejected_rows,
                "coerced_null_rows": coerced_null_rows,
            }
        if rows_written == expected_written:
            return {
                "passed": True,
                "message": (
                    f"Transfer verified by writer: {rows_written:,} rows written"
                    + (f", {rejected_rows:,} rejected" if rejected_rows else "")
                    + " (read-back verifier not available for this destination)"
                ),
                "source_rows": source_rows,
                "target_rows": rows_written,
                "source_checksum": source_checksum,
                "target_checksum": "",
                "rejected_rows": rejected_rows,
                "coerced_null_rows": coerced_null_rows,
            }
        report = reconcile(
            source_rows=source_rows,
            target_rows=rows_written,
            source_checksum=source_checksum,
            target_checksum="",
            rejected_rows=rejected_rows,
            strict_checksum=False,
            coerced_null_rows=coerced_null_rows,
        )
        return report.to_dict()

    # Data loss signal: the target table holds fewer rows than we just wrote.
    if target_rows < rows_written:
        report = reconcile(
            source_rows=source_rows,
            target_rows=target_rows,
            source_checksum=source_checksum,
            target_checksum=target_checksum,
            rejected_rows=rejected_rows,
            strict_checksum=strict_checksum,
            sample_compare=sample_compare,
            coerced_null_rows=coerced_null_rows,
        )
        return report.to_dict()

    # We have a verified read-back. Run the full reconciliation; allow extra
    # rows because destinations may legitimately contain pre-existing data.
    report = reconcile(
        source_rows=source_rows,
        target_rows=target_rows,
        source_checksum=source_checksum,
        target_checksum=target_checksum,
        rejected_rows=rejected_rows,
        strict_checksum=strict_checksum,
        allow_extra_rows=True,
        sample_compare=sample_compare,
        coerced_null_rows=coerced_null_rows,
    )
    return report.to_dict()
