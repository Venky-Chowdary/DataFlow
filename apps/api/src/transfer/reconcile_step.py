"""Gate 8 reconciliation for the universal transfer engine."""

from __future__ import annotations

import logging
from typing import Any

from connectors.writer_common import (
    map_rows_for_fingerprint,
    resolve_target_columns,
    transform_error_policy_for_validation_mode,
)
from services.reconciliation import (
    TargetSampleUnavailable,
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
    validation_mode: str = "strict",
    destination_pk_columns: list[str] | None = None,
) -> str:
    """Return the writer checksum, or recompute it from mapped source rows.

    Remap uses the same error policy / dest_kind / PK kwargs as the write path
    so Gate-8 cannot invent a different quarantine hold-out set than the load.
    """
    if writer_checksum:
        return writer_checksum
    if not records:
        return ""
    _, data_rows = records_to_matrix(records, columns)
    if target_cols is None:
        target_cols, _ = resolve_target_columns(mappings, source_schema or {}, preserve_case=True)
    mapped_rows, _rejected = map_rows_for_fingerprint(
        headers=columns,
        data_rows=data_rows,
        mappings=mappings,
        target_cols=target_cols,
        column_types=source_schema or {},
        error_policy=transform_error_policy_for_validation_mode(validation_mode),
        dest_types=dest_types or {},
        preserve_case=True,
        dest_kind=dest_db_type or "",
        destination_pk_columns=destination_pk_columns,
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
        # Object/file exports have no destination cell read-back. Writer checksum
        # proves bytes landed, not per-cell fidelity. Operational write may pass;
        # never stamp migration_proven / cell-fidelity Gate-8 green.
        checksum = str(writer_checksum or dest_summary.get("checksum") or "").strip()
        return _finalize_reconcile({
            "passed": True,
            "unproven": True,
            "skipped_readback": True,
            "migration_proven": False,
            "message": (
                "File/object export wrote successfully — Gate-8 cell fidelity "
                "unproven (no destination read-back). "
                + (
                    f"Writer checksum present ({checksum[:16]}…) — count/bytes only."
                    if checksum
                    else "No writer checksum; treat as operational pass only."
                )
            ),
            "source_rows": source_rows,
            "target_rows": rows_written,
            "rejected_rows": rejected_rows,
            "coerced_null_rows": coerced_null_rows,
            "rows_skipped": rows_skipped,
            "checksum": checksum,
        })

    from .connector_capabilities import resolve_driver_type

    cfg = resolve_connector_config(endpoint)
    # Prefer canonical driver (amazon_s3→s3) while preserving catalog type on cfg.
    db_type = resolve_driver_type(
        str(cfg.get("type") or endpoint.format or "")
    ).lower()
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
    pk_cols = list(
        dest_summary.get("primary_key_columns")
        or dest_summary.get("conflict_columns")
        or []
    )
    # Quarantine replay / upsert writers may stamp written_ids without PK meta.
    # Resolve identity from Map so keyed Gate-8 can re-scope the target digest.
    if not pk_cols and mapping_dicts:
        try:
            from services.primary_key import resolve_identity_key

            _src_pk, tgt_pk = resolve_identity_key(
                mappings=mapping_dicts,
                source_columns=list(columns or []),
                dest_kind=str(db_type or endpoint.format or ""),
                validation_mode=validation_mode,
                purpose="uniqueness",
            )
            if tgt_pk:
                pk_cols = [str(tgt_pk)]
                dest_summary.setdefault("primary_key_columns", list(pk_cols))
                dest_summary.setdefault("conflict_columns", list(pk_cols))
        except Exception as exc:
            logging.getLogger(__name__).debug(
                "Gate-8 identity resolve skipped: %s", exc, exc_info=exc
            )
    source_checksum = _compute_source_checksum(
        records,
        columns,
        mapping_dicts,
        source_schema,
        writer_checksum,
        target_cols=target_cols,
        dest_db_type=db_type,
        dest_types=dest_types,
        validation_mode=validation_mode,
        destination_pk_columns=[str(c) for c in pk_cols if c] or None,
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

    from services.sync_cursor import is_overwrite_sync

    sync_mode_early = str(
        dest_summary.get("sync_mode")
        or dest_summary.get("effective_sync_mode")
        or ""
    )
    allow_extra_early = (
        not is_overwrite_sync(sync_mode_early)
        and sync_mode_early.lower() not in {"full_refresh_mirror", "mirror", "scd2"}
    )
    written_ids = [
        str(x)
        for x in (dest_summary.get("written_ids") or [])
        if x is not None and str(x) != ""
    ] or None
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
    pk_column = str(pk_cols[0]) if len(pk_cols) == 1 else None
    if allow_extra_early and pk_column and not written_ids and sample_records:
        written_ids = [
            str(x)
            for x in _source_key_values(
                sample_records,
                sort_key=pk_column,
                mappings=mapping_dicts,
                limit=500,
            )
            if x is not None and str(x) != ""
        ] or None

    # Always full-table first for SQL. Keyed batch proof is a fallback when the
    # sink legitimately has extras (upsert/append into non-empty) — never for a
    # first load where target_rows == source_rows (would fingerprint a sample).
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
        written_ids=None,
        pk_column=None,
    )

    strict_checksum = validation_mode in ("strict", "maximum")

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
        try:
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
        except TargetSampleUnavailable as exc:
            # A failed read is not "no rows to compare". Skipping Gate-8 here
            # used to report a clean reconcile while the destination was
            # unreachable — the exact silent-pass the proof bar forbids.
            return _finalize_reconcile({
                "passed": False,
                "message": (
                    "Gate-8 sample compare unavailable: could not read destination "
                    f"sample ({exc}). Refusing to treat a failed read as fidelity proof."
                ),
                "source_rows": source_rows,
                "target_rows": target_rows,
                "source_checksum": source_checksum,
                "target_checksum": target_checksum,
                "rejected_rows": rejected_rows,
                "coerced_null_rows": coerced_null_rows,
                "sample_compare": {"passed": False, "error": str(exc)},
            })
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
            try:
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
            except TargetSampleUnavailable as exc:
                return _finalize_reconcile({
                    "passed": False,
                    "message": (
                        "Gate-8 delete proof unavailable: could not read destination "
                        f"keys ({exc}). Refusing to treat a failed read as proof that "
                        "deleted PKs are absent."
                    ),
                    "source_rows": source_rows,
                    "target_rows": target_rows,
                    "source_checksum": source_checksum,
                    "target_checksum": target_checksum,
                    "rejected_rows": rejected_rows,
                    "coerced_null_rows": coerced_null_rows,
                })
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
        # SaaS / Kafka: keyed sample compare is independent enough for reverse-ETL
        # (Hightouch/Census row sync status) when full-table COUNT is unavailable.
        if (
            sample_compare
            and sample_compare.get("passed")
            and int(sample_compare.get("compared") or 0) > 0
            and rows_written == expected_written
        ):
            return _finalize_reconcile({
                "passed": True,
                "message": (
                    f"Gate-8 sample-verified {int(sample_compare.get('compared') or 0)} "
                    f"key-aligned field(s) for '{db_type}' "
                    f"({rows_written:,} rows written"
                    + (f", {rejected_rows:,} rejected" if rejected_rows else "")
                    + ")"
                ),
                "source_rows": source_rows,
                "target_rows": rows_written,
                "source_checksum": source_checksum,
                "target_checksum": "",
                "rejected_rows": rejected_rows,
                "coerced_null_rows": coerced_null_rows,
                "rows_skipped": rows_skipped,
                "sample_compare": sample_compare,
            })
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
                "sample_compare": sample_compare,
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
                "sample_compare": sample_compare,
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
            sample_compare=sample_compare,
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
    sync_mode = sync_mode_early
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
        not in {"pinecone", "qdrant", "weaviate", "milvus", "pgvector", "email"}
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

    # Upsert/append into a larger sink: whole-table digests are not comparable to
    # the batch. Re-fingerprint destination WHERE pk IN (batch keys) while keeping
    # full-table cardinality for the operator report.
    expected_batch = max(source_rows - dropped_rows - rows_skipped, 0)
    # Upsert/append/quarantine-replay into a non-empty table: whole-table digests
    # are not comparable. Keyed fingerprint of written_ids proves the batch for
    # balanced and strict alike (strict_checksum only governs fail-closed severity
    # inside reconcile(), not whether we may re-scope the target digest).
    if (
        allow_extra
        and pk_column
        and written_ids
        and target_rows > expected_batch
        and source_checksum
        and target_checksum
        and source_checksum != target_checksum
        and db_type
        in {"sqlite", "postgresql", "redshift", "generic_sql", "mongodb"}
    ):
        _keyed_rows, keyed_checksum = verify_target(
            db_type,
            cfg,
            schema=schema,
            table_name=table_name,
            fallback_rows=target_rows,
            fallback_checksum=target_checksum,
            target_columns=target_cols,
            limit=checksum_limit,
            dest_types=dest_types,
            written_ids=written_ids,
            pk_column=pk_column,
        )
        if keyed_checksum:
            target_checksum = keyed_checksum

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
