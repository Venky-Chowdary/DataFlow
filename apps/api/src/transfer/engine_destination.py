from __future__ import annotations

from typing import Any

from src.transfer import engine as _engine
from src.transfer.engine import (
    EndpointConfig,
    FullRefreshDropFailed,
    _raise_if_job_cancelled,
    is_overwrite_sync,
    logger,
)

#: Longest the engine waits for the destination to answer its first contact.
DEFAULT_DESTINATION_CONNECT_DEADLINE_SECONDS = 60.0


class DestinationConnectTimeout(TimeoutError):
    """The destination did not answer its first contact inside the deadline (D19)."""


def _destination_connect_deadline() -> float:
    from src.ai.copilot.tool_deadline import _seconds_from_env

    return _seconds_from_env(
        "DATAFLOW_DESTINATION_CONNECT_DEADLINE_SECONDS",
        DEFAULT_DESTINATION_CONNECT_DEADLINE_SECONDS,
    )


def _connect_destination(
    destination: EndpointConfig,
    *,
    sync_mode: str,
    mongo: Any,
    job_id: str,
) -> tuple[dict[str, str], bool | None]:
    """First contact with the destination: bounded, and the job keeps pulsing.

    A destination that accepts the connection and never answers held the run's
    worker for the sum of every driver timeout it stacked, with the job document
    frozen at 5% (D19). The orphan sweeper cannot settle that job (its worker is
    alive and holds the lease) and the step watchdog reads a pulse as progress,
    so the engine ends it itself: the probe runs on the connector executor with
    a deadline, and the run fails naming the destination.
    """
    if destination.kind != "database":
        return _engine._destination_schema_probe(destination, sync_mode=sync_mode)
    from services.connector_executor import (
        ConnectorCallTimeout,
        ConnectorCapacityExceeded,
        get_executor,
    )
    from services.workspace_filter import current_workspace_stamp
    from src.ai.copilot.tool_deadline import describe_target

    seconds = _destination_connect_deadline()
    try:
        cfg = _engine.resolve_connector_config(destination)
    except Exception:  # noqa: BLE001 - only names the target in the message
        cfg = {}
    target = describe_target(
        {
            "host": cfg.get("host") or destination.host,
            "port": cfg.get("port") or destination.port,
            "connection_string": cfg.get("connection_string")
            or destination.connection_string,
        }
    ) or str(destination.format or "destination")

    def _timeout_message() -> str:
        return (
            f"Destination {target} did not answer within {seconds:g}s while the "
            "run was connecting to it (step: reading the destination schema). "
            "Nothing was written. Check the host and port, the firewall or VPN, "
            "and that the database is healthy, then run it again."
        )

    try:
        with _engine._job_phase_heartbeat(
            mongo,
            job_id,
            phase="preflight",
            message="Connecting to destination…",
            interval_s=max(0.25, min(15.0, seconds / 4)),
        ):
            return get_executor().run(
                "destination_connect",
                lambda: _engine._destination_schema_probe(
                    destination, sync_mode=sync_mode
                ),
                workspace=current_workspace_stamp(),
                seconds=seconds,
                describe_timeout=_timeout_message,
            )
    except (ConnectorCallTimeout, ConnectorCapacityExceeded) as exc:
        raise DestinationConnectTimeout(str(exc)) from None


def _destination_schema_probe(
    destination: EndpointConfig,
    sync_mode: str = "",
) -> tuple[dict[str, str], bool | None]:
    """Return (column_types, table_exists).

    ``table_exists`` is independent of whether columns loaded — a listed table
    with empty column metadata must not be treated as missing / create-new.

    Overwrite sync still needs a proven existence boolean for preflight DDL
    gates; stale destination *types* are cleared so recreate is not typed
    against the old table shape.
    """
    if destination.kind != "database":
        return {}, None
    try:
        from .endpoint_intelligence import introspect_endpoint

        # Match Studio Destination/Validate: destination probes stay in the
        # operator-chosen namespace (no cross-DB/schema "heal").
        destination.extra = {
            **(destination.extra or {}),
            "introspect_purpose": "destination",
        }
        info = introspect_endpoint(destination)
        from services.pgvector_layout import document_table_engine_view

        info = document_table_engine_view(destination, info)
        schema = dict(info.get("schema") or {})
        nullability = {
            str(k): bool(v)
            for k, v in dict(info.get("schema_nullability") or {}).items()
        }
        if "table_exists" in info:
            raw = info.get("table_exists")
            # Preserve explicit None (unknown) — bool(None) is False and would
            # dishonestly trigger create-new on Execute after a soft-fail probe.
            exists = None if raw is None else bool(raw)
        elif schema:
            exists = True
        else:
            exists = None
        # Stamp probe SSOT for Map/Validate CTAs. Sticky None is an error;
        # create-new (exists=False) messages are informational, not failures.
        probe_err = str(info.get("sample_error") or info.get("error") or "").strip()
        probe_msg = str(info.get("message") or "").strip()
        extra = dict(destination.extra or {})
        if exists is None:
            detail = probe_err or probe_msg or (
                "Destination schema unknown — existence not proven (fail-closed)"
            )
            extra["schema_probe_error"] = detail[:500]
            extra["schema_probe_message"] = detail[:500]
        elif probe_err:
            extra["schema_probe_error"] = probe_err[:500]
            extra["schema_probe_message"] = (probe_msg or probe_err)[:500]
        else:
            extra.pop("schema_probe_error", None)
            if str(extra.get("schema_probe_message") or "").startswith(
                "Destination schema unknown"
            ):
                extra.pop("schema_probe_message", None)
            if probe_msg and exists is False:
                extra["schema_probe_message"] = probe_msg[:500]
        # Stamp PK/UNIQUE/FK catalog for Execute preflight SSOT with Validate
        # (preflight_router passes dest_meta.primary_key_columns / unique_keys).
        extra["primary_key_columns"] = list(info.get("primary_key_columns") or [])
        extra["dynamo_key_schema"] = [
            dict(row)
            for row in (info.get("dynamo_key_schema") or [])
            if isinstance(row, dict)
        ]
        extra["dynamo_index_attributes"] = dict(
            info.get("dynamo_index_attributes") or {}
        )
        extra["unique_keys"] = list(info.get("unique_keys") or [])
        extra["foreign_keys"] = list(
            info.get("foreign_keys") or info.get("destination_foreign_keys") or []
        )
        # Create-new overwrite has no dest contract — clear stale types.
        if is_overwrite_sync(sync_mode) and exists is not True:
            extra["schema_nullability"] = {}
            extra["schema_defaults"] = {}
            extra["identity_columns"] = []
            extra["generated_columns"] = []
            extra["schema_carrier_authority"] = {}
            extra.pop("schema_types", None)
            destination.extra = extra
            return {}, exists
        # Dest-exists overwrite on a relational engine empties rows and keeps
        # the table, so the live types, nullability and constraints are the
        # contract. Engines that still DROP+CREATE must not bind the doomed
        # carrier (a stale VARCHAR(64) refused a TEXT source whose CREATE
        # would have been LONGTEXT).
        from services.db_type_utils import dest_schema_is_recreated_on_overwrite

        overwrite_recreates_existing = (
            is_overwrite_sync(sync_mode)
            and exists is True
            and dest_schema_is_recreated_on_overwrite(
                str(getattr(destination, "format", "") or "")
            )
        )
        # G19 reads this only when the run really replaces the declaration.
        extra["overwrite_replaced_column_types"] = (
            dict(schema) if overwrite_recreates_existing else {}
        )
        extra["schema_nullability"] = nullability
        # Sampled vs declared — D1: a page-measured DECIMAL(2,2) is a profile,
        # not a catalog. Writers and Map both read this stamp.
        extra["schema_carrier_authority"] = dict(info.get("schema_authority") or {})
        if overwrite_recreates_existing:
            extra.pop("schema_types", None)
        else:
            extra["schema_types"] = dict(schema)
        # Who fills a required column when the mapping does not (G14).
        extra["schema_defaults"] = dict(info.get("schema_defaults") or {})
        extra["identity_columns"] = list(info.get("identity_columns") or [])
        extra["generated_columns"] = list(info.get("generated_columns") or [])
        destination.extra = extra
        return ({} if overwrite_recreates_existing else schema), exists
    except Exception as exc:
        logger.warning(
            "Destination schema probe failed for %s: %s",
            getattr(destination, "format", ""),
            exc,
            exc_info=exc,
        )
        destination.extra = {
            **(destination.extra or {}),
            "schema_probe_error": str(exc)[:500],
            "schema_probe_message": str(exc)[:500],
            "schema_nullability": {},
            "schema_defaults": {},
            "identity_columns": [],
            "generated_columns": [],
            "primary_key_columns": [],
            "unique_keys": [],
            "foreign_keys": [],
            "schema_carrier_authority": {},
            "schema_types": {},
        }
        return {}, None


def _pin_overwrite_rows_before(
    destination: EndpointConfig,
    checkpoint: Any = None,
    checkpoint_service: Any = None,
) -> None:
    """Count the destination before overwrite removes it.

    The stream measures COUNT after the drop, so a table that held rows
    looks empty and the failure path deletes the replacement. The count
    taken here is the one undo and Gate-8 must keep.
    """
    extra = dict(getattr(destination, "extra", None) or {})
    pinned = extra.get("overwrite_rows_before")
    if isinstance(pinned, int):
        return
    if checkpoint is not None and getattr(checkpoint, "target_rows_before", None) is not None:
        extra["overwrite_rows_before"] = int(checkpoint.target_rows_before)
        destination.extra = extra
        return
    counted: int | None = None
    try:
        from services.dest_precount import precount_destination

        from .adapters import resolve_connector_config

        raw = precount_destination(destination, resolve_connector_config(destination))
        if isinstance(raw, int):
            counted = int(raw)
    except Exception as exc:  # noqa: BLE001 - a missed count must not skip the overwrite
        logger.warning("Overwrite pre-count failed: %s", exc, exc_info=exc)
        return
    if counted is None:
        return
    extra["overwrite_rows_before"] = counted
    destination.extra = extra
    if checkpoint is None:
        return
    checkpoint.target_rows_before = counted
    if checkpoint_service is None:
        return
    try:
        checkpoint_service.require_save(checkpoint)
    except Exception as exc:  # noqa: BLE001 - the in-memory pin still guards this process
        logger.warning("Overwrite pre-count was not checkpointed: %s", exc, exc_info=exc)



def _note_failed_batch_undo(
    request: Any,
    dest_summary: Any,
    message: str,
    *,
    recon: dict[str, Any] | None = None,
) -> str:
    """Undo note plus an honest quarantine pointer for the operator.

    The pointer names the persisted evidence location: the sibling
    ``{table}_df_quarantine`` when one was written and kept, or the job's
    control-plane quarantine record when this run's destination table (and
    its litter) was dropped — a FAIL_JOB message must never claim a table
    that the undo just removed (X-46).
    """
    out = _failed_batch_undo_note_impl(
        request, dest_summary, message, recon=recon
    )
    if not isinstance(dest_summary, dict):
        return out
    qt = str(dest_summary.get("dest_quarantine_table") or "")
    if qt:
        note = (
            f"Rejected row(s) were preserved in the destination table {qt} "
            "for review and replay."
        )
    elif dest_summary.get("partial_batch_undo") == "cleared":
        note = (
            "Rejected-row evidence is retained in this job's quarantine "
            "record for inspection and replay."
        )
    else:
        return out
    return out if note in out else f"{out} {note}"


def _failed_batch_undo_note_impl(
    request: Any,
    dest_summary: Any,
    message: str,
    *,
    recon: dict[str, Any] | None = None,
) -> str:
    """Clear a partial SQL batch when this run found the destination empty.

    A MySQL overwrite renamed the previous table aside. Restoring that
    backup is the rollback. Deleting the replacement after the rename
    leaves the live name empty and the pre-run rows only in the backup
    — or gone, if a second start already dropped the backup.

    When verification could not *read* the destination (transient fault
    after retries), nothing was proven wrong with the committed rows, so
    they are kept rather than undone (QA MX2-15).
    """
    if not isinstance(dest_summary, dict):
        return message or "Reconciliation failed"
    if isinstance(recon, dict) and recon.get("verification_unavailable"):
        note = (
            "The committed rows were kept: verification could not read the "
            "destination, which is not evidence the batch is wrong."
        )
        logger.warning(
            "Skipping failed-batch undo for table=%s: verification unavailable",
            dest_summary.get("table"),
        )
        dest_summary["partial_batch_undo"] = "retained"
        dest_summary["partial_batch_undo_note"] = note
        base = message or "Reconciliation failed"
        return base if note in base else f"{base} {note}"
    destination = getattr(request, "destination", None)
    extra = dict(getattr(destination, "extra", None) or {})
    backup_engine = str(extra.get("overwrite_backup_engine") or "")
    from src.transfer.engine_finish import RESTORABLE_BACKUP_ENGINES

    if extra.get("overwrite_backup") and backup_engine in RESTORABLE_BACKUP_ENGINES:
        backup_name = str(extra.get("overwrite_backup") or "")
        restored = _engine._settle_overwrite_backup(destination, restore=True)
        if restored:
            note = (
                "The previous destination table was restored. "
                "This run's replacement was removed."
            )
            dest_summary["partial_batch_undo"] = "restored"
        else:
            note = (
                "The previous destination table could not be restored. "
                f"The backup {backup_name} was left in place."
            )
            dest_summary["partial_batch_undo"] = "restore_failed"
        dest_summary["partial_batch_undo_note"] = note
        base = message or "Reconciliation failed"
        if note not in base:
            return f"{base} {note}"
        return base
    # Routed replace-mode run: each occupied route table was renamed aside
    # before the write — restoring them is the rollback (same pattern as the
    # MySQL rename-aside above, one per route).
    if extra.get("route_backups"):
        restored = _engine._settle_overwrite_backup(destination, restore=True)
        _what = (
            "The destination table"
            if len(extra.get("route_backups") or {}) == 1
            else "Every route table"
        )
        if restored:
            note = (
                f"{_what} was restored to its pre-run content: "
                "this run's new rows were removed and the pre-run rows were "
                "restored from the backup copies."
            )
            dest_summary["partial_batch_undo"] = "restored"
            from services.batch_undo import restore_route_watermark_after_undo

            wm_note = restore_route_watermark_after_undo(dest_summary)
            if wm_note:
                note = f"{note}{wm_note}"
        else:
            note = (
                "One or more backed-up tables could not be restored. "
                "The __dfbak backup tables were left in place."
            )
            dest_summary["partial_batch_undo"] = "restore_failed"
        dest_summary["partial_batch_undo_note"] = note
        base = message or "Reconciliation failed"
        return base if note in base else f"{base} {note}"
    # Routed append/incremental run: rows were ADDED to live route tables.
    # When the run stamped its write transaction id, delete exactly this
    # run's rows by xmin — byte-identical copies from earlier runs keep
    # their own txid and survive, and pre-existing rows are never touched
    # (P2 keyed undo). Without a txid, report honestly which rows were
    # added and that prior content is intact.
    route_map = dest_summary.get("routes") or {}
    if route_map:
        _run_txids = dest_summary.get("route_run_txid")
        _run_tables = list(dest_summary.get("route_run_tables") or [])
        # The keyed undo only runs when the run actually fails: under
        # strict/maximum a Gate-8 failure removes this run's rows. Under
        # balanced/lenient the rows are kept by contract — passed=False
        # carries the failed verdict, never a delete.
        _vmode = str(getattr(request, "validation_mode", "") or "").lower()
        _undo_allowed = _vmode in ("strict", "maximum", "")
        if _run_txids and _run_tables and not _undo_allowed:
            added = {
                str(name): int(info.get("rows") or 0)
                for name, info in route_map.items()
                if isinstance(info, dict)
            }
            added_txt = (
                ", ".join(f"{n}: {c}" for n, c in added.items()) or "unknown"
            )
            note = (
                "Verification failed but the run's rows are kept under "
                f"{_vmode} validation — prior rows intact, this run added "
                f"({added_txt}); reconcile before re-running."
            )
            dest_summary["partial_batch_undo"] = "retained"
            dest_summary["partial_batch_undo_note"] = note
            base = message or "Reconciliation failed"
            return base if note in base else f"{base} {note}"
        if _run_txids and _run_tables and _undo_allowed:
            try:
                from .adapters import resolve_connector_config
                from .connector_capabilities import resolve_driver_type
                from connectors.table_manager import undo_pg_route_run

                _db_type = resolve_driver_type(
                    str(getattr(destination, "format", "") or "")
                )
                _undone = (
                    undo_pg_route_run(
                        resolve_connector_config(destination),
                        _run_txids,
                        _run_tables,
                    )
                    if _db_type == "postgresql"
                    else []
                )
            except Exception as exc:  # noqa: BLE001 - report, don't mask
                logger.warning("Route run undo failed: %s", exc)
                _undone = []
            if _undone:
                note = (
                    "This run's routed rows were removed from every route "
                    "table it wrote (" + ", ".join(sorted(_undone)) + "); "
                    "the prior content — including byte-identical copies "
                    "from earlier runs — is intact. A retry will not "
                    "duplicate."
                )
                dest_summary["partial_batch_undo"] = "restored"
                from services.batch_undo import restore_route_watermark_after_undo

                wm_note = restore_route_watermark_after_undo(dest_summary)
                if wm_note:
                    note = f"{note}{wm_note}"
            else:
                note = (
                    "The prior rows in every route table are intact, but "
                    "this run's added rows could not be selectively removed "
                    "— investigate the route tables before re-running."
                )
                dest_summary["partial_batch_undo"] = "restore_failed"
            dest_summary["partial_batch_undo_note"] = note
            base = message or "Reconciliation failed"
            return base if note in base else f"{base} {note}"
        added = {
            str(name): int(info.get("rows") or 0)
            for name, info in route_map.items()
            if isinstance(info, dict)
        }
        added_txt = ", ".join(f"{n}: {c}" for n, c in added.items()) or "unknown"
        note = (
            "The prior rows in every route table are intact. This run added "
            f"the rows it routed ({added_txt}) and those added rows could "
            "not be selectively removed — investigate the route tables "
            "before re-running."
        )
        dest_summary["partial_batch_undo"] = "retained"
        dest_summary["partial_batch_undo_note"] = note
        base = message or "Reconciliation failed"
        return base if note in base else f"{base} {note}"
    from services.batch_undo import undo_failed_batch_if_dest_was_empty

    note = undo_failed_batch_if_dest_was_empty(
        destination=getattr(request, "destination", None),
        dest_summary=dest_summary,
        sync_mode=str(getattr(request, "sync_mode", "") or ""),
    )
    base = message or "Reconciliation failed"
    if note and note not in base:
        return f"{base} {note}"
    return base


def _remember_preserved_columns(destination: EndpointConfig, kept: list[dict]) -> None:
    if not kept:
        return
    extra = dict(destination.extra or {})
    extra["preserve_columns"] = kept
    destination.extra = extra


def _drop_destination_table(
    destination: EndpointConfig,
    *,
    mappings: list[dict] | None = None,
    checkpoint: Any = None,
    checkpoint_service: Any = None,
    mongo: Any = None,
    job_id: str = "",
) -> bool:
    """Drop the destination object for full-refresh overwrite sync modes.

    Raises :class:`FullRefreshDropFailed` when a drop was attempted and failed.
    Swallowing it is the difference between an overwrite and an append: the old
    rows survive, the new rows land on top, and the job reports success with a
    doubled destination. A ``full_refresh`` that cannot clear the destination
    must fail loudly instead of quietly changing its own semantics.

    Returns ``False`` only when the driver has no drop support, which is a
    genuine "nothing to do" and not an error.
    """
    if destination.kind != "database":
        return False

    _raise_if_job_cancelled(mongo, job_id)
    _pin_overwrite_rows_before(destination, checkpoint, checkpoint_service)

    from connectors.table_manager import TableDropError, drop_table

    from .adapters import (
        carry_dest_spelling_across_drop,
        resolve_connector_config,
        resolve_dest_table,
    )
    from .connector_capabilities import resolve_driver_type

    try:
        db_type = resolve_driver_type(destination.format)
        cfg = resolve_connector_config(destination)
        table_name = resolve_dest_table(db_type, destination)
        schema = cfg.get("schema")
    except Exception as exc:
        # Could not even work out what to drop. Treated as fatal for the same
        # reason: proceeding would append onto an uncleared table.
        raise FullRefreshDropFailed(
            "unknown", f"could not resolve destination for drop: {exc}"
        ) from exc

    carry_dest_spelling_across_drop(destination, db_type, cfg, table_name, schema)
    from connectors.table_manager import overwrite_clear_kind

    clear_kind = overwrite_clear_kind(db_type)
    if clear_kind == "rename_collection":
        from connectors.table_manager import retire_mongodb_overwrite

        try:
            backup = retire_mongodb_overwrite(cfg, table_name)
        except TableDropError as exc:
            logger.error("full_refresh mongo retire failed for %s: %s", table_name, exc)
            raise FullRefreshDropFailed(table_name, str(exc.cause)) from exc
        if backup:
            extra = dict(destination.extra or {})
            extra["overwrite_backup"] = backup
            extra["overwrite_backup_engine"] = "mongodb"
            destination.extra = extra
        return True
    if clear_kind == "empty":
        from connectors.table_manager import empty_existing_for_overwrite

        # PostgreSQL TRUNCATE is transactional. Doing it here on autocommit
        # published an empty table, then each chunk commit published a prefix
        # (R4-D06). The writer truncates inside the load transaction and
        # commits once. MySQL TRUNCATE auto-commits and stays on this path.
        if db_type == "postgresql":
            extra = dict(destination.extra or {})
            extra["pg_overwrite_in_transaction"] = True
            destination.extra = extra
            return True
        # MySQL TRUNCATE auto-commits. A later refusal used to leave the
        # live table empty. Copy the prior rows aside, empty the live table
        # in place (its constraints stay, D195), and let the failure path
        # copy them back; success drops the backup.
        if db_type in {"mysql", "mariadb"}:
            from connectors.table_manager import retire_mysql_overwrite

            try:
                backup, kept = retire_mysql_overwrite(cfg, table_name, mappings)
            except TableDropError as exc:
                logger.error("full_refresh mysql retire failed for %s: %s", table_name, exc)
                raise FullRefreshDropFailed(table_name, str(exc.cause)) from exc
            if backup:
                extra = dict(destination.extra or {})
                extra["overwrite_backup"] = backup
                extra["overwrite_backup_engine"] = "mysql"
                destination.extra = extra
            _remember_preserved_columns(destination, kept)
            return True
        from connectors.table_manager import (
            ROW_COPY_BACKUP_ENGINE,
            overwrite_keeps_row_copy,
            retire_generic_sql_overwrite,
        )

        if overwrite_keeps_row_copy(db_type):
            # The writer's batches commit one by one, so the empty cannot
            # share their transaction. Keep the previous rows recoverable and
            # let the failure path put them back (_settle_overwrite_backup).
            try:
                _state, backup = retire_generic_sql_overwrite(
                    db_type, cfg, table_name, schema
                )
            except TableDropError as exc:
                logger.error("full_refresh empty failed for %s: %s", table_name, exc)
                raise FullRefreshDropFailed(table_name, str(exc.cause)) from exc
            if backup:
                extra = dict(destination.extra or {})
                extra["overwrite_backup"] = backup
                extra["overwrite_backup_engine"] = ROW_COPY_BACKUP_ENGINE
                destination.extra = extra
            return True
        try:
            empty_existing_for_overwrite(db_type, cfg, table_name, schema)
        except TableDropError as exc:
            logger.error("full_refresh empty failed for %s: %s", table_name, exc)
            raise FullRefreshDropFailed(table_name, str(exc.cause)) from exc
        return True
    if db_type in ("postgresql", "redshift"):
        try:
            from connectors.table_manager import postgres_columns_to_keep

            kept = postgres_columns_to_keep(cfg, table_name, schema, mappings)
            _remember_preserved_columns(destination, kept)
        except Exception as exc:  # noqa: BLE001 - keep-column read must not block the overwrite
            logger.warning(
                "Could not read destination columns to keep on %s: %s",
                table_name,
                exc,
            )
    try:
        return drop_table(db_type, cfg, table_name, schema)
    except TableDropError as exc:
        logger.error("full_refresh drop failed for %s: %s", table_name, exc)
        raise FullRefreshDropFailed(table_name, str(exc.cause)) from exc
    except Exception as exc:
        logger.error("full_refresh drop failed for %s: %s", table_name, exc, exc_info=exc)
        raise FullRefreshDropFailed(table_name, str(exc)) from exc
