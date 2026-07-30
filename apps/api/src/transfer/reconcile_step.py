"""Gate 8 reconciliation for the universal transfer engine."""

from __future__ import annotations

import logging
from typing import Any

from connectors.writer_common import build_mapped_rows, resolve_target_columns
from services.reconciliation import (
    checksum_rows,
    read_target_sample,
    reconcile,
    sample_compare_rows,
    stamp_post_write_phase,
    verify_target,
)

from .adapters import records_to_matrix, resolve_connector_config
from .models import EndpointConfig


def _finalize_reconcile(payload: dict[str, Any]) -> dict[str, Any]:
    """Every post-write reconcile return path gets an explicit phase."""
    return stamp_post_write_phase(payload)

def _dest_types_from_mappings(mappings: list[dict]) -> dict[str, str]:
    return {
        str(m.get("target") or ""): str(
            m.get("target_type") or m.get("inferredType") or ""
        )
        for m in mappings
        if m.get("target")
        and (m.get("target_type") or m.get("inferredType"))
    }


def _compute_source_checksum(
    records: list[dict],
    columns: list[str],
    mappings: list[dict],
    source_schema: dict[str, str] | None,
    writer_checksum: str,
    target_cols: list[str] | None = None,
    *,
    dest_db_type: str = "",
    dest_types: dict[str, str] | None = None,
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
        dest_types=dest_types or {},
        preserve_case=True,
    )
    return checksum_rows(
        mapped_rows,
        target_cols,
        dest_db_type=dest_db_type,
        dest_types=dest_types,
    )


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
    rows_skipped = int(dest_summary.get("rows_skipped", 0) or 0)
    # Coerced rows are KEPT (a cell became NULL); quarantine hold-outs are absent
    # from the destination. Skipped rows (e.g. stale CDC LSN) are not written.
    dropped_rows = max(rejected_rows - coerced_null_rows, 0)
    # Prefer independent source accounting when the caller provides it
    # (streaming paths should pass source_row_count from the read side).
    source_row_count = dest_summary.get("source_row_count")
    if isinstance(source_row_count, int) and source_row_count >= 0:
        source_rows = source_row_count
    else:
        source_rows = len(records) if records else rows_written + dropped_rows + rows_skipped
    expected_written = max(source_rows - dropped_rows - rows_skipped, 0)

    if endpoint.kind != "database":
        return _finalize_reconcile({
            "passed": True,
            "message": "File export — reconciliation skipped",
            "source_rows": source_rows,
            "target_rows": rows_written,
            "rejected_rows": rejected_rows,
            "coerced_null_rows": coerced_null_rows,
            "rows_skipped": rows_skipped,
        })

    db_type = endpoint.format.lower()
    cfg = resolve_connector_config(endpoint)
    from services.dialect_profiles import schema_from_cfg

    schema = dest_summary.get("schema") or schema_from_cfg(db_type, cfg)
    table_name = dest_summary.get("table") or endpoint.table or endpoint.collection or ""

    mapping_dicts = mappings or [{"source": col, "target": col} for col in columns]
    dest_types = _dest_types_from_mappings(mapping_dicts)
    # Prefer physical types stamped by the writer when present.
    physical = dest_summary.get("column_types") or dest_summary.get("target_types")
    if isinstance(physical, dict):
        for k, v in physical.items():
            if v:
                dest_types[str(k)] = str(v)
    if source_schema:
        try:
            from services.transform_resolver import attach_transforms_to_mappings

            mapping_dicts = attach_transforms_to_mappings(
                mapping_dicts,
                column_types=source_schema,
                dest_types=dest_types,
            )
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)

    target_cols = _mapped_targets(mapping_dicts, columns)
    source_checksum = _compute_source_checksum(
        records,
        columns,
        mapping_dicts,
        source_schema,
        writer_checksum,
        target_cols=target_cols,
        dest_db_type=db_type,
        dest_types=dest_types,
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
            rows_skipped=rows_skipped,
        )
        return _finalize_reconcile(report.to_dict())

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
        dest_types=dest_types,
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
        # Compare EVERY mapped column. Truncating to 20 falsely treated columns
        # 21+ as NULL (Mongo→MySQL users with 24 fields: Gate-8 failed on
        # referral_invite_modal_dismissed with source 0 vs invented NULL).
        target_sample = read_target_sample(
            db_type,
            cfg,
            schema=schema,
            table_name=table_name,
            columns=target_cols or None,
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
                dest_db_type=db_type,
                dest_types=dest_types,
            )

    # CDC delete proof: stashed PKs must be absent on destination read-back.
    delete_pks = [
        str(k)
        for k in (dest_summary.get("reconcile_deletes") or [])
        if k is not None and str(k) != ""
    ]
    if delete_pks and table_name and target_cols and strict_checksum:
        sort_key = _sort_key_for_columns(target_cols, mapping_dicts)
        if sort_key:
            still_present = read_target_sample(
                db_type,
                cfg,
                schema=schema,
                table_name=table_name,
                columns=[sort_key],
                limit=len(delete_pks),
                sort_key=sort_key,
                key_values=delete_pks,
            )
            if still_present:
                return _finalize_reconcile({
                    "passed": False,
                    "message": (
                        f"Gate-8 delete proof failed: {len(still_present)} deleted "
                        "PK(s) still present on destination after CDC delete"
                    ),
                    "source_rows": source_rows,
                    "target_rows": target_rows,
                    "source_checksum": source_checksum,
                    "target_checksum": target_checksum,
                    "rejected_rows": rejected_rows,
                    "coerced_null_rows": coerced_null_rows,
                    "rows_skipped": rows_skipped,
                    "delete_keys_checked": len(delete_pks),
                    "delete_keys_still_present": len(still_present),
                })

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
            return _finalize_reconcile({
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
                "rows_skipped": rows_skipped,
            })
        if rows_written == expected_written:
            return _finalize_reconcile({
                "passed": True,
                "message": (
                    f"Transfer verified by writer: {rows_written:,} rows written"
                    + (f", {rejected_rows:,} rejected" if rejected_rows else "")
                    + (f", {rows_skipped:,} skipped" if rows_skipped else "")
                    + " (read-back verifier not available for this destination)"
                ),
                "source_rows": source_rows,
                "target_rows": rows_written,
                "source_checksum": source_checksum,
                "target_checksum": "",
                "rejected_rows": rejected_rows,
                "coerced_null_rows": coerced_null_rows,
                "rows_skipped": rows_skipped,
            })
        report = reconcile(
            source_rows=source_rows,
            target_rows=rows_written,
            source_checksum=source_checksum,
            target_checksum="",
            rejected_rows=rejected_rows,
            strict_checksum=False,
            coerced_null_rows=coerced_null_rows,
            rows_skipped=rows_skipped,
        )
        return _finalize_reconcile(report.to_dict())

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
            rows_skipped=rows_skipped,
        )
        return _finalize_reconcile(report.to_dict())

    # We have a verified read-back. Extra dest rows are legitimate for append /
    # upsert into a non-empty sink; overwrite/mirror/replace must not soft-pass
    # extras (Airbyte/Fivetran-class honesty: mode-aware reconcile).
    from services.sync_cursor import is_overwrite_sync

    sync_mode = str(
        dest_summary.get("sync_mode")
        or dest_summary.get("effective_sync_mode")
        or ""
    )
    allow_extra = not is_overwrite_sync(sync_mode)
    if sync_mode.lower() in {"full_refresh_mirror", "mirror", "scd2"}:
        allow_extra = False

    # Streaming append/upsert soft-pass of extra dest rows without a stashed
    # sample cannot claim key-aligned proof (Airbyte/Fivetran honesty bar).
    is_streaming = bool(dest_summary.get("streaming"))
    if (
        strict_checksum
        and is_streaming
        and allow_extra
        and int(rows_written or 0) > 0
        and not sample_compare
        and not sample_records
        and db_type
        not in {"pinecone", "qdrant", "weaviate", "milvus", "pgvector", "email", "kafka"}
    ):
        from services.reconciliation import ReconciliationReport

        return ReconciliationReport(
            passed=False,
            source_rows=source_rows,
            target_rows=target_rows,
            source_checksum=source_checksum,
            target_checksum=target_checksum,
            message=(
                "Gate-8 refused: streaming write completed but no reconcile_sample "
                "was stashed for key-aligned proof. Re-run with sample stash enabled."
            ),
            rejected_rows=rejected_rows,
            coerced_null_rows=coerced_null_rows,
        ).to_dict()

    report = reconcile(
        source_rows=source_rows,
        target_rows=target_rows,
        source_checksum=source_checksum,
        target_checksum=target_checksum,
        rejected_rows=rejected_rows,
        strict_checksum=strict_checksum,
        allow_extra_rows=allow_extra,
        sample_compare=sample_compare,
        coerced_null_rows=coerced_null_rows,
        rows_skipped=rows_skipped,
    )
    return _finalize_reconcile(report.to_dict())
