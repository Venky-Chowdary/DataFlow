"""Destination-owned exactly-once commits for Iceberg catalog tables."""

from __future__ import annotations

import hashlib
import logging
from typing import Any

from connectors.iceberg_commit import CommitRetryPolicy, commit_with_retry
from connectors.iceberg_catalog import (
    load_catalog,
    parse_iceberg_catalog_config,
)
from connectors.iceberg_writer import (
    _PK_SCAN_SLICE,
    _filter_delete_keys_by_lsn,
    _iceberg_present_fields,
    _iceberg_delete_predicate,
    _iceberg_map_rows,
    _iceberg_row_pk,
    _prepare_pyiceberg_write,
)
from connectors.writer_common import DF_LSN_COL
from connectors.lsn_guards import lsn_family
from services.cdc_exactly_once import (
    ALGORITHM,
    REASON_DEST_NOT_WIRED,
    DestWmView,
    EosApplyResult,
    EosOpenResult,
    ExactlyOnceRouteError,
    batch_apply_checksum,
    committed_apply_checksum,
    combine_change_batch,
    decide_from_view,
    encode_resume_blob,
    extract_cdc_phase,
    extract_snapshot_last_pk,
    extract_snapshot_signal_id,
    extract_snapshot_window_id,
    filter_snapshot_rows_after_dest_hi,
    incoming_pk_keys,
    is_incremental_snapshot_token,
    load_reduce_into_dest,
    next_dest_snapshot_progress,
    next_dest_window_id,
    next_handoff_phase,
    plan_open_session,
    planned_apply_seq,
    verify_dest_commit,
)

logger = logging.getLogger(__name__)

_WATERMARK_FIELDS = (
    "stream_key",
    "committed_lsn",
    "lsn_family",
    "epoch",
    "fence_epoch",
    "apply_seq",
    "phase",
    "apply_checksum",
    "resume_blob",
    "window_id",
    "snapshot_signal_id",
    "window_hi_pk",
    "batch_id",
    "dest_object",
    "algorithm",
)


class IcebergEosWatermarkError(RuntimeError):
    """A table carries an incomplete, malformed, or colliding EOS watermark."""


def iceberg_eos_catalog_ready(dest_cfg: dict[str, Any] | None) -> bool:
    """Return whether the supplied endpoint describes a catalog Iceberg write path."""
    if not isinstance(dest_cfg, dict):
        logger.debug(
            "Iceberg EOS catalog readiness refused reason=%s",
            type(dest_cfg).__name__,
        )
        return False
    try:
        from connectors.iceberg_writer import resolve_iceberg_write_path

        ready = resolve_iceberg_write_path(dest_cfg) == "catalog"
        if not ready:
            logger.debug("Iceberg EOS catalog readiness refused reason=filesystem")
        return ready
    except Exception as exc:
        logger.debug(
            "Iceberg EOS catalog readiness refused reason=%s",
            type(exc).__name__,
        )
        return False


class _IcebergNoCommit(Exception):
    def __init__(self, result: Any) -> None:
        super().__init__("Iceberg EOS action requires no catalog commit")
        self.result = result


def _stream_prefix(stream_key: str) -> str:
    digest = hashlib.sha256(stream_key.encode("utf-8")).hexdigest()[:16]
    return f"dataflow.eos.{digest}."


def _endpoint_config(
    dest_cfg: dict[str, Any], dest_table: str = "", dest_type: str = ""
) -> dict[str, Any]:
    endpoint = dict(dest_cfg or {})
    if dest_type:
        endpoint.setdefault("type", dest_type)
    if dest_table:
        endpoint["table"] = dest_table
        endpoint["table_name"] = dest_table
    from connectors.iceberg_writer import resolve_iceberg_write_path

    try:
        write_path = resolve_iceberg_write_path(endpoint)
    except Exception as exc:
        raise ExactlyOnceRouteError(
            "Iceberg exactly-once requires a valid catalog destination "
            f"({type(exc).__name__}).",
            reason=REASON_DEST_NOT_WIRED,
        ) from exc
    if write_path != "catalog":
        raise ExactlyOnceRouteError(
            "Iceberg exactly-once is available only for catalog-backed writes; "
            "filesystem Iceberg remains at-least-once.",
            reason=REASON_DEST_NOT_WIRED,
        )
    return endpoint


def _catalog_table(
    dest_cfg: dict[str, Any], dest_table: str = "", dest_type: str = ""
) -> tuple[Any, tuple[str, ...], str, str]:
    endpoint = _endpoint_config(dest_cfg, dest_table, dest_type)
    config = parse_iceberg_catalog_config(endpoint)
    catalog = load_catalog(endpoint)
    identifier = tuple(config["namespace"]) + (config["table_name"],)
    return catalog, identifier, ".".join(identifier), endpoint


def _watermark_from_table(tbl: Any, stream_key: str) -> DestWmView:
    prefix = _stream_prefix(stream_key)
    properties = dict(getattr(tbl.metadata, "properties", None) or {})
    namespaced = {
        field: properties.get(prefix + field) for field in _WATERMARK_FIELDS
    }
    present = [value is not None for value in namespaced.values()]
    if not any(present):
        if any(key.startswith(prefix) for key in properties):
            raise IcebergEosWatermarkError(
                "Iceberg EOS watermark has no recognized fields"
            )
        return DestWmView()
    if not all(present):
        missing = [
            field
            for field, value in namespaced.items()
            if value is None
        ]
        raise IcebergEosWatermarkError(
            f"Iceberg EOS watermark is partial; missing fields {missing!r}"
        )
    if namespaced["stream_key"] != stream_key:
        raise IcebergEosWatermarkError(
            "Iceberg EOS watermark hash collision: stored stream_key does not match"
        )
    try:
        epoch = int(namespaced["epoch"])
        fence_epoch = int(namespaced["fence_epoch"])
        apply_seq = int(namespaced["apply_seq"])
    except (TypeError, ValueError) as exc:
        raise IcebergEosWatermarkError(
            "Iceberg EOS watermark has a malformed epoch or apply_seq"
        ) from exc
    if min(epoch, fence_epoch, apply_seq) < 0:
        raise IcebergEosWatermarkError(
            "Iceberg EOS watermark epoch and apply_seq values must be non-negative"
        )
    committed_lsn = str(namespaced["committed_lsn"] or "")
    actual_family = lsn_family(committed_lsn) if committed_lsn else "empty"
    if (
        str(namespaced["lsn_family"] or "") != actual_family
        or actual_family == "opaque"
        or namespaced["algorithm"] != ALGORITHM
        or not str(namespaced["phase"] or "").strip()
    ):
        raise IcebergEosWatermarkError(
            "Iceberg EOS watermark has a malformed LSN family, phase, or algorithm"
        )
    return DestWmView(
        committed_lsn=committed_lsn or None,
        epoch=epoch,
        fence_epoch=fence_epoch,
        phase=str(namespaced["phase"]),
        apply_checksum=str(namespaced["apply_checksum"] or ""),
        resume_blob=str(namespaced["resume_blob"] or ""),
        apply_seq=apply_seq,
        window_id=str(namespaced["window_id"] or ""),
        snapshot_signal_id=str(namespaced["snapshot_signal_id"] or ""),
        window_hi_pk=str(namespaced["window_hi_pk"] or ""),
        exists=True,
    )


def _watermark_properties(
    *,
    stream_key: str,
    committed_lsn: str | None,
    epoch: int,
    fence_epoch: int,
    apply_seq: int,
    phase: str,
    apply_checksum: str,
    resume_blob: str,
    window_id: str,
    snapshot_signal_id: str,
    window_hi_pk: str,
    batch_id: str,
    dest_object: str,
) -> dict[str, str]:
    values = {
        "stream_key": stream_key,
        "committed_lsn": str(committed_lsn or ""),
        "lsn_family": lsn_family(committed_lsn) if committed_lsn else "empty",
        "epoch": str(epoch),
        "fence_epoch": str(fence_epoch),
        "apply_seq": str(apply_seq),
        "phase": phase or "streaming",
        "apply_checksum": apply_checksum or "",
        "resume_blob": resume_blob or "",
        "window_id": window_id or "",
        "snapshot_signal_id": snapshot_signal_id or "",
        "window_hi_pk": window_hi_pk or "",
        "batch_id": batch_id,
        "dest_object": dest_object,
        "algorithm": ALGORITHM,
    }
    prefix = _stream_prefix(stream_key)
    return {prefix + key: value for key, value in values.items()}


def _stage_empty_snapshot(txn: Any, snapshot_properties: dict[str, str]) -> None:
    producer = txn.update_snapshot(
        snapshot_properties=snapshot_properties
    ).fast_append()
    producer.commit()


def _scan_dest_rows(
    tbl: Any,
    pk_cols: list[str],
    keys: set[str],
) -> list[dict[str, Any]]:
    if not keys:
        return []
    projection = list(dict.fromkeys([*pk_cols, DF_LSN_COL]))
    rows: list[dict[str, Any]] = []
    ordered_keys = sorted(keys)
    for start in range(0, len(ordered_keys), _PK_SCAN_SLICE):
        chunk = set(ordered_keys[start : start + _PK_SCAN_SLICE])
        predicate = _iceberg_delete_predicate(tbl, pk_cols, chunk)
        scanned = tbl.scan(row_filter=predicate).select(*projection).to_arrow()
        columns = {
            name: scanned.column(name).to_pylist()
            for name in scanned.column_names
        }
        for index in range(scanned.num_rows):
            rows.append({name: values[index] for name, values in columns.items()})
    return rows


def _mapped_records(
    *,
    records: list[dict[str, Any]],
    mappings: list[dict[str, Any]],
    column_types: dict[str, str],
    pk_cols: list[str],
) -> tuple[list[dict[str, Any]], list[str]]:
    from connectors.writer_common import (
        resolve_studio_or_map_dest_types,
        resolve_target_columns,
        transform_error_policy,
    )

    effective_mappings = [dict(mapping) for mapping in mappings]
    effective_types = dict(column_types)
    target_cols, logical_types = resolve_target_columns(
        effective_mappings,
        effective_types,
        preserve_case=True,
        table_exists=None,
        dest_db="iceberg",
    )
    if DF_LSN_COL not in target_cols:
        target_cols = [*target_cols, DF_LSN_COL]
    dest_types, error = resolve_studio_or_map_dest_types(
        target_cols,
        effective_mappings,
        effective_types,
        logical_types=logical_types,
        product="Iceberg",
        dest_db="iceberg",
    )
    if error:
        raise ValueError(error)
    headers = list(
        dict.fromkeys(str(key) for record in records for key in record)
    )
    data_rows = [[record.get(header) for header in headers] for record in records]
    mapped, errors, rejected = _iceberg_map_rows(
        headers=headers,
        data_rows=data_rows,
        mappings=effective_mappings,
        target_cols=target_cols,
        column_types=effective_types,
        dest_types=dest_types,
        policy=transform_error_policy("fail"),
        conflict_columns=pk_cols,
    )
    if errors or rejected:
        raise ValueError(
            f"Iceberg EOS row mapping rejected input: {rejected[:3]!r}"
        )
    mapped_records = [
        _iceberg_present_fields(dict(zip(target_cols, values, strict=True)))
        for values in mapped
    ]
    return mapped_records, target_cols


def apply_eos_iceberg(
    *,
    dest_type: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    change: Any,
    mappings: list[dict[str, Any]],
    column_types: dict[str, str],
    pk_target_cols: list[str],
    stream_key: str,
    incoming_lsn: str,
    batch_id: str,
    crash_after: str | None = None,
    writer_fence: int = 0,
) -> EosApplyResult:
    if not pk_target_cols:
        raise ExactlyOnceRouteError(
            "Iceberg exactly-once apply requires destination primary-key columns.",
            reason="exactly_once_requires_primary_key",
        )
    catalog, identifier, dest_object, endpoint = _catalog_table(
        dest_cfg, dest_table, dest_type
    )
    change = combine_change_batch(change, pk_cols=pk_target_cols)
    incoming_phase = extract_cdc_phase(change.resume_token)
    incoming_checksum = batch_apply_checksum(
        change, incoming_lsn=incoming_lsn, pk_cols=pk_target_cols
    )
    resume_blob = encode_resume_blob(change.resume_token)
    records = list(change.inserts or []) + list(change.updates or [])

    def load_table() -> Any:
        return catalog.load_table(identifier)

    def stage(
        fresh_tbl: Any, txn: Any, commit_props: dict[str, str]
    ) -> EosApplyResult:
        dest = _watermark_from_table(fresh_tbl, stream_key)
        action, fence = decide_from_view(
            incoming_lsn=incoming_lsn,
            dest=dest,
            incoming_fence=writer_fence,
            incoming_phase=incoming_phase,
            incoming_checksum=incoming_checksum,
            incremental_snapshot=is_incremental_snapshot_token(
                change.resume_token
            ),
            change=change,
        )
        if action in {
            "already_committed",
            "stream_wins_skip",
            "window_closed_skip",
            "chunk_closed_skip",
        }:
            raise _IcebergNoCommit(
                EosApplyResult(
                    status=action,
                    committed_lsn=dest.committed_lsn,
                    batch_id=batch_id,
                    epoch=dest.epoch,
                    already_committed=True,
                    fence_epoch=fence,
                    phase=dest.phase or "streaming",
                    apply_checksum=dest.apply_checksum,
                    apply_seq=dest.apply_seq,
                    window_id=dest.window_id,
                    snapshot_signal_id=dest.snapshot_signal_id,
                    window_hi_pk=dest.window_hi_pk,
                )
            )

        phase = next_handoff_phase(incoming_phase, dest.phase or None)
        next_window_id = next_dest_window_id(
            extract_snapshot_window_id(change.resume_token), dest.window_id
        )
        signal_id, hi_pk = next_dest_snapshot_progress(
            incoming_signal_id=extract_snapshot_signal_id(change.resume_token),
            dest_signal_id=dest.snapshot_signal_id,
            incoming_last_pk=extract_snapshot_last_pk(change.resume_token),
            dest_last_pk=dest.window_hi_pk,
        )
        dest_seq = planned_apply_seq(dest.apply_seq)
        if action == "handoff_phase":
            committed_lsn = dest.committed_lsn or incoming_lsn
            checksum = committed_apply_checksum(
                incoming_checksum, dest.apply_checksum, change
            )
            result = EosApplyResult(
                status="handoff_phase",
                committed_lsn=committed_lsn,
                batch_id=batch_id,
                epoch=dest.epoch + 1,
                already_committed=True,
                fence_epoch=fence,
                phase="streaming",
                apply_checksum=checksum,
                apply_seq=dest_seq,
                window_id=next_window_id,
                snapshot_signal_id=signal_id,
                window_hi_pk=hi_pk,
            )
            watermark = _watermark_properties(
                stream_key=stream_key,
                committed_lsn=committed_lsn,
                epoch=result.epoch,
                fence_epoch=fence,
                apply_seq=dest_seq,
                phase="streaming",
                apply_checksum=checksum,
                resume_blob=resume_blob or dest.resume_blob,
                window_id=next_window_id,
                snapshot_signal_id=signal_id,
                window_hi_pk=hi_pk,
                batch_id=batch_id,
                dest_object=dest_object,
            )
            snapshot_properties = {**commit_props, **watermark}
            _stage_empty_snapshot(txn, snapshot_properties)
            if crash_after == "after_apply_before_watermark":
                from services.cdc_exactly_once import EosCrash

                raise EosCrash(crash_after)
            txn.set_properties(watermark)
            if crash_after == "after_watermark_before_commit":
                from services.cdc_exactly_once import EosCrash

                raise EosCrash(crash_after)
            return result

        inc_signal = extract_snapshot_signal_id(change.resume_token)
        if (
            records
            and dest.snapshot_signal_id
            and dest.window_hi_pk
            and inc_signal
            and inc_signal == dest.snapshot_signal_id
        ):
            records_for_write = filter_snapshot_rows_after_dest_hi(
                records,
                pk_cols=pk_target_cols,
                dest_last_pk=dest.window_hi_pk,
            )
        else:
            records_for_write = list(records)

        mapped_records, target_cols = _mapped_records(
            records=records_for_write,
            mappings=mappings,
            column_types=column_types,
            pk_cols=pk_target_cols,
        ) if records_for_write else ([], [])
        record_key_set = (
            set(incoming_pk_keys(mapped_records, pk_target_cols))
            if mapped_records
            else set()
        )
        delete_keys = {str(key) for key in (change.deletes or [])}
        scan_keys = record_key_set | delete_keys
        schema = fresh_tbl.schema().as_arrow()
        if DF_LSN_COL not in schema.names:
            raise ValueError(
                f"Iceberg EOS table requires the {DF_LSN_COL!r} column"
            )
        missing_columns = [column for column in target_cols if column not in schema.names]
        if missing_columns:
            raise ValueError(
                f"Iceberg EOS mapped columns are absent from the table: "
                f"{missing_columns[:12]!r}"
            )
        dest_rows = _scan_dest_rows(fresh_tbl, pk_target_cols, scan_keys)
        dest_docs = {
            str(key): row
            for row in dest_rows
            if (key := _iceberg_row_pk(row, pk_target_cols))
        }
        reduced = load_reduce_into_dest(
            incoming_rows=mapped_records,
            dest_rows=dest_docs,
            pk_cols=pk_target_cols,
            incoming_lsn=incoming_lsn,
        )
        record_keys_to_replace = set(incoming_pk_keys(reduced, pk_target_cols))
        existing_keys = {
            str(key)
            for row in dest_rows
            if (key := _iceberg_row_pk(row, pk_target_cols))
        }
        surviving_delete_keys = _filter_delete_keys_by_lsn(
            dest_rows,
            pk_target_cols,
            delete_keys,
            incoming_lsn=incoming_lsn,
            lsn_column=DF_LSN_COL,
        ) & existing_keys
        keys_to_delete = (record_keys_to_replace & existing_keys) | surviving_delete_keys
        rows_written = len(reduced)
        deleted = len(surviving_delete_keys)
        snapshot_properties = {
            **commit_props,
            **_watermark_properties(
                stream_key=stream_key,
                committed_lsn=incoming_lsn,
                epoch=dest.epoch + 1,
                fence_epoch=fence,
                apply_seq=dest_seq,
                phase=phase,
                apply_checksum=incoming_checksum,
                resume_blob=resume_blob,
                window_id=next_window_id,
                snapshot_signal_id=signal_id,
                window_hi_pk=hi_pk,
                batch_id=batch_id,
                dest_object=dest_object,
            ),
        }
        if keys_to_delete:
            txn.delete(
                delete_filter=_iceberg_delete_predicate(
                    fresh_tbl, pk_target_cols, keys_to_delete
                ),
                snapshot_properties=snapshot_properties,
            )
        if reduced:
            stamped = []
            for record in reduced:
                row = dict(record)
                row[DF_LSN_COL] = incoming_lsn
                stamped.append(row)
            import pyarrow as pa

            pa_table, _checksum = _prepare_pyiceberg_write(stamped, schema, pa)
            txn.append(pa_table, snapshot_properties=snapshot_properties)
        else:
            _stage_empty_snapshot(txn, snapshot_properties)
        if crash_after == "after_apply_before_watermark":
            from services.cdc_exactly_once import EosCrash

            raise EosCrash(crash_after)
        watermark = {
            key: value
            for key, value in snapshot_properties.items()
            if key.startswith(_stream_prefix(stream_key))
        }
        txn.set_properties(watermark)
        if crash_after == "after_watermark_before_commit":
            from services.cdc_exactly_once import EosCrash

            raise EosCrash(crash_after)
        return EosApplyResult(
            status="applied" if (rows_written or deleted) else "empty",
            rows_written=rows_written,
            deleted=deleted,
            committed_lsn=incoming_lsn,
            batch_id=batch_id,
            epoch=dest.epoch + 1,
            fence_epoch=fence,
            phase=phase,
            apply_checksum=incoming_checksum,
            apply_seq=dest_seq,
            window_id=next_window_id,
            snapshot_signal_id=signal_id,
            window_hi_pk=hi_pk,
        )

    try:
        outcome = commit_with_retry(
            load_table,
            stage,
            operation="eos_apply",
            policy=CommitRetryPolicy.from_endpoint(endpoint),
        )
        result = outcome.value
    except _IcebergNoCommit as no_commit:
        return no_commit.result
    if result.committed_lsn:
        verify_dest_commit(
            dest=iceberg_dest_watermark_view(endpoint, stream_key),
            expected_lsn=result.committed_lsn,
            expected_fence=result.fence_epoch,
            expected_seq=result.apply_seq,
        )
    return result


def open_iceberg_eos_session(
    *,
    dest_type: str,
    dest_cfg: dict[str, Any],
    stream_key: str,
    incoming_fence: int = 0,
    job_resume: Any = None,
) -> EosOpenResult:
    catalog, identifier, dest_object, endpoint = _catalog_table(
        dest_cfg, dest_type=dest_type
    )

    def load_table() -> Any:
        return catalog.load_table(identifier)

    def stage(tbl: Any, txn: Any, commit_props: dict[str, str]) -> EosOpenResult:
        view = _watermark_from_table(tbl, stream_key)
        from services.cdc_slot_resume import without_retired_slot_resume

        view, effective_resume = without_retired_slot_resume(
            view, job_resume, cursor_key=stream_key
        )
        opened = plan_open_session(
            dest=view,
            incoming_fence=incoming_fence,
            job_resume=effective_resume,
        )
        if not opened.fence_raised:
            raise _IcebergNoCommit(opened)
        watermark = _watermark_properties(
            stream_key=stream_key,
            committed_lsn=view.committed_lsn,
            epoch=view.epoch,
            fence_epoch=opened.fence_epoch,
            apply_seq=view.apply_seq,
            phase=view.phase or "streaming",
            apply_checksum=view.apply_checksum,
            resume_blob=view.resume_blob,
            window_id=view.window_id,
            snapshot_signal_id=view.snapshot_signal_id,
            window_hi_pk=view.window_hi_pk,
            batch_id="eos-open",
            dest_object=dest_object,
        )
        txn.set_properties(watermark)
        _stage_empty_snapshot(txn, {**commit_props, **watermark})
        return opened

    try:
        outcome = commit_with_retry(
            load_table,
            stage,
            operation="eos_open",
            policy=CommitRetryPolicy.from_endpoint(endpoint),
        )
        return outcome.value
    except _IcebergNoCommit as no_commit:
        return no_commit.result


def iceberg_dest_watermark_view(
    dest_cfg: dict[str, Any], stream_key: str
) -> DestWmView:
    catalog, identifier, _dest_object, _endpoint = _catalog_table(dest_cfg)
    return _watermark_from_table(catalog.load_table(identifier), stream_key)


def iceberg_dest_watermark_lsn(
    dest_cfg: dict[str, Any], stream_key: str
) -> str | None:
    return iceberg_dest_watermark_view(dest_cfg, stream_key).committed_lsn


def iceberg_dest_resume_blob(dest_cfg: dict[str, Any], stream_key: str) -> str:
    return iceberg_dest_watermark_view(dest_cfg, stream_key).resume_blob


def iceberg_blank_eos_resume(
    dest_cfg: dict[str, Any], stream_key: str, *, lsn: str
) -> bool:
    target_lsn = str(lsn or "").strip()
    key = str(stream_key or "").strip()
    if not target_lsn or not key:
        return False
    try:
        catalog, identifier, dest_object, endpoint = _catalog_table(dest_cfg)

        def load_table() -> Any:
            return catalog.load_table(identifier)

        def stage(
            tbl: Any, txn: Any, commit_props: dict[str, str]
        ) -> bool:
            view = _watermark_from_table(tbl, key)
            if view.committed_lsn != target_lsn:
                raise _IcebergNoCommit(False)
            watermark = _watermark_properties(
                stream_key=key,
                committed_lsn="",
                epoch=view.epoch,
                fence_epoch=view.fence_epoch,
                apply_seq=view.apply_seq,
                phase=view.phase or "streaming",
                apply_checksum=view.apply_checksum,
                resume_blob="",
                window_id=view.window_id,
                snapshot_signal_id=view.snapshot_signal_id,
                window_hi_pk=view.window_hi_pk,
                batch_id="eos-resume-blank",
                dest_object=dest_object,
            )
            txn.set_properties(watermark)
            _stage_empty_snapshot(txn, {**commit_props, **watermark})
            return True

        outcome = commit_with_retry(
            load_table,
            stage,
            operation="eos_resume_blank",
            policy=CommitRetryPolicy.from_endpoint(endpoint),
        )
        return bool(outcome.value)
    except _IcebergNoCommit as no_commit:
        return bool(no_commit.result)
    except Exception as exc:
        logger.warning(
            "Could not blank Iceberg EOS resume stream_key=%s lsn=%s error=%s",
            key,
            target_lsn,
            type(exc).__name__,
        )
        return False
