"""Remove a failed batch from a destination this run found empty.

Writers commit each chunk before the job checksum runs. A failed checksum
used to leave those rows in place (40 of 50) while the job was marked failed.
Deleting by the capped ``written_ids`` list is not a rollback: the list is not
the batch. Truncating a table that already held rows would destroy them.

The only safe restore is the one the copy paths already use. When the
pre-write count was measured as zero, every row now in the table came from
this failed run, so clearing the table returns it to that empty state. CDC,
SCD2, and mirror are not batch loads and are left untouched. An occupied
destination is left untouched and the job says so.
"""

from __future__ import annotations

import logging
import sqlite3
from contextlib import closing
from typing import Any

from services.dest_precount import PRECOUNT_KEY, count_dialect, precount_table
from services.sync_cursor import normalize_sync_mode

logger = logging.getLogger(__name__)

_HISTORY_MODES = frozenset({"cdc", "scd2", "mirror"})


def restore_route_watermark_after_undo(dest_summary: dict[str, Any]) -> str:
    """Roll the route watermark back when this run's batch was undone.

    ACC-05 advances the route watermark as each batch commits because the rows
    are durable at that moment. When verification later removes (or restores)
    exactly those rows, the watermark advance is no longer true — leaving it
    makes the next run skip every source row and report "nothing written"
    while the destination holds stale or no data (D110).

    The stream stamps ``watermark_restore_point = {cursor_key, watermark}``
    with the pre-run value the first time it advances the route watermark.
    A pre-run value of ``None`` means this route had never run: the cursor
    record is deleted so the next run is a true first run.

    Never raises — the undo must not hide the failure that triggered it.
    """
    removed = bool(dest_summary.get("partial_batch_removed"))
    restored = str(dest_summary.get("partial_batch_undo") or "") == "restored"
    if not (removed or restored):
        return ""
    restore = dest_summary.get("watermark_restore_point")
    if not isinstance(restore, dict):
        return ""
    cursor_key = str(restore.get("cursor_key") or "").strip()
    if not cursor_key:
        return ""
    prior = restore.get("watermark")
    try:
        from services.sync_cursor import clear_watermark, get_watermark, set_watermark

        advanced = get_watermark(cursor_key)
        if advanced in (None, "") and prior in (None, ""):
            return ""
        if prior in (None, ""):
            result = clear_watermark(cursor_key)
            if result.get("cleared"):
                return (
                    " The incremental watermark was rolled back to unset "
                    "because this run's batch was removed — the next run "
                    "re-reads the full source instead of skipping it."
                )
            return ""
        set_watermark(
            cursor_key,
            str(prior),
            metadata={
                "watermark_restore": "batch_undo",
                "job_id": str(dest_summary.get("job_id") or ""),
            },
        )
        return (
            " The incremental watermark was rolled back to its pre-run value "
            "because this run's batch was removed — the next run re-reads "
            "from that point instead of skipping source rows."
        )
    except Exception as exc:  # noqa: BLE001 — report, never hide the undo result
        logger.warning(
            "Watermark restore after batch undo failed for %s: %s",
            cursor_key,
            exc,
            exc_info=True,
        )
        return (
            " WARNING: this run's batch was removed but the incremental "
            "watermark could not be rolled back — clear the route cursor "
            "before re-running or the next run will skip source rows."
        )


def undo_failed_batch_if_dest_was_empty(
    *,
    destination: Any,
    dest_summary: dict[str, Any],
    sync_mode: str,
) -> str:
    """Clear a partial batch or record why it was kept. Never raises.

    Stamps ``partial_batch_undo`` and ``partial_batch_undo_note`` on the
    summary. A second call is a no-op.
    """
    if dest_summary.get("partial_batch_undo"):
        return str(dest_summary.get("partial_batch_undo_note") or "")
    try:
        decision, note = _decide(destination, dest_summary, sync_mode)
        if decision == "clear":
            note = _clear(destination, dest_summary)
            decision = "cleared" if dest_summary.get("partial_batch_removed") else "retained"
        dest_summary["partial_batch_undo"] = decision
        wm_note = restore_route_watermark_after_undo(dest_summary)
        if wm_note:
            note = f"{note}{wm_note}"
        dest_summary["partial_batch_undo_note"] = note
        return note
    except Exception as exc:  # noqa: BLE001 — undo must not hide the checksum failure
        logger.warning("Failed-batch undo did not run: %s", exc, exc_info=True)
        note = (
            "The failed batch was left in place because the undo could not run."
        )
        dest_summary["partial_batch_undo"] = "retained"
        dest_summary["partial_batch_undo_note"] = note
        return note


def _decide(
    destination: Any,
    dest_summary: dict[str, Any],
    sync_mode: str,
) -> tuple[str, str]:
    mode = normalize_sync_mode(sync_mode or dest_summary.get("sync_mode"), default="")
    if mode in _HISTORY_MODES or str(dest_summary.get("sync_mode") or "").lower() == "cdc":
        return (
            "retained",
            "CDC and history loads are not rolled back as a batch.",
        )
    store = _object_store_family(destination)
    if getattr(destination, "kind", "") != "database" and not store:
        return (
            "retained",
            "This destination is not a SQL table, so the failed batch was not deleted.",
        )
    if "resume_rows_before" in dest_summary:
        # D134 — a resumed pass: the job-level "before" is not what this pass
        # may undo. Rows an earlier pass committed stay, whatever the job's
        # first-pass precount says.
        held = dest_summary.get("resume_rows_before")
        if not isinstance(held, int):
            return (
                "retained",
                "The destination count when this job was resumed was not measured, "
                "so the failed resumed batch was left in place.",
            )
        if held > 0:
            return (
                "retained",
                f"The destination already held {held} row(s) when this job was "
                "resumed, including rows an earlier pass of this job committed. "
                "The failed resumed batch was left in place — deleting by a "
                "partial key list would not be a rollback.",
            )
    before = dest_summary.get(PRECOUNT_KEY)
    if not isinstance(before, int):
        # Worker-path failures reach this undo through the engine's job
        # summary, which need not carry the stream layer's precount — the
        # same measurement rides destination.extra.
        _eb = (getattr(destination, "extra", None) or {}).get("dest_rows_before")
        if isinstance(_eb, int):
            before = _eb
    if not isinstance(before, int):
        # X-35: the run created the table (existence stamp False) — a
        # nonexistent destination holds 0 rows by definition, so the missing
        # precount must not leave a 0-row table behind on a write failure.
        _existed = dest_summary.get("dest_table_existed_before")
        if _existed is None:
            _existed = (getattr(destination, "extra", None) or {}).get(
                "dest_table_existed_before"
            )
        if _existed is False:
            before = 0
    if not isinstance(before, int):
        return (
            "retained",
            "Pre-write destination count was not measured, so the failed batch "
            "was left in place.",
        )
    if before != 0:
        return (
            "retained",
            f"Destination already held {before} row(s) before this run. "
            "The failed batch was left in place — deleting by a partial key "
            "list would not be a rollback.",
        )
    return ("clear", "")


def _clear(destination: Any, dest_summary: dict[str, Any]) -> str:
    from src.transfer.adapters import resolve_connector_config, resolve_dest_table

    cfg = resolve_connector_config(destination)
    db_type = count_dialect(str(cfg.get("type") or getattr(destination, "format", "") or ""))
    table = str(
        dest_summary.get("table")
        or dest_summary.get("collection")
        or resolve_dest_table(db_type, destination, "dt_import")
        or ""
    ).strip()
    if not table:
        return "The failed batch was left in place because the destination table was not named."
    from services.dialect_profiles import schema_from_cfg

    schema = schema_from_cfg(db_type, cfg)
    store = _object_store_kind(db_type)
    if store in {"s3", "gcs", "adls"}:
        return _clear_object_store(
            dest_summary, store, cfg, schema=schema, table_name=table
        )
    # A router partitioned the batch into per-route tables. Rolling back only
    # ``dest_summary["table"]`` (the last write's table) would clear one route
    # and leave the rest half-written — the failed batch spans every route
    # table (RT-1).
    route_tables = [
        str(info.get("table") or "")
        for info in (dest_summary.get("routes") or {}).values()
        if isinstance(info, dict) and info.get("table")
    ]
    tables = route_tables or [table]
    # Byte-identical undo distinguishes two "empty before" shapes: the run
    # created the table (drop it — the destination had no table) or the table
    # existed empty (delete rows — its DDL must stay).
    drop_created = (
        dest_summary.get("dest_table_existed_before") is False
        # The stamp also rides destination.extra: worker-path failures reach
        # this undo through the engine's job summary, which does not carry
        # every stream-layer key.
        or (getattr(destination, "extra", None) or {}).get(
            "dest_table_existed_before"
        )
        is False
    )
    if drop_created:
        # FAIL_JOB litters the {table}_df_quarantine DLQ beside the dest
        # table (D1): a destination that had no table before holds no
        # quarantine table either — drop this run's litter with it.
        from services.dest_quarantine import dlq_table_name

        tables = tables + [dlq_table_name(_t) for _t in tables]
    removed_any = False
    remaining_after: list[int] = []
    dropped_all = drop_created
    for _t in tables:
        if drop_created:
            try:
                from connectors.table_manager import drop_table

                dropped = bool(drop_table(db_type, cfg, _t, schema=schema))
            except Exception as drop_exc:  # noqa: BLE001
                # A table the run never reached CREATE on is already
                # 'byte-identical to before' — missing-object errors (SQL
                # Server 'Invalid object name' 208) must not fall through to
                # a DELETE against the same phantom ref.
                _missing = any(
                    tok in str(drop_exc).lower()
                    for tok in (
                        "invalid object",
                        "does not exist",
                        "unknown table",
                        "no such table",
                        "not exist",
                    )
                )
                dropped = _missing
            if not dropped:
                # drop_table answered False without erroring — check whether
                # the table is even there before attempting a DELETE.
                try:
                    _still_there = precount_table(db_type, cfg, _t)
                except Exception:  # noqa: BLE001
                    _still_there = None
                if _still_there is None:
                    dropped = True
            if dropped:
                remaining_after.append(0)
                continue
            dropped_all = False
        # A table the run never reached CREATE on is already 'byte-identical
        # to before' — a DELETE against it only raises a missing-object error
        # (SQL Server 'Invalid object name' 208, MySQL 1146, PG 42P01) that
        # then logs as a failed undo. Probe the row count first: 0 — missing
        # or genuinely empty — means there is nothing to DELETE.
        _probe: int | None
        try:
            _probe = precount_table(db_type, cfg, _t)
        except Exception as _probe_exc:  # noqa: BLE001
            _missing = any(
                tok in str(_probe_exc).lower()
                for tok in (
                    "invalid object",
                    "does not exist",
                    "unknown table",
                    "no such table",
                    "not exist",
                    "undefined table",
                    "base table or view not found",
                )
            )
            if not _missing:
                raise
            _probe = 0
        if _probe == 0:
            removed = True
            remaining_after.append(0)
            continue
        try:
            removed = _delete_all_rows(
                db_type,
                cfg,
                schema=schema,
                table_name=_t,
                job_id=str(dest_summary.get("job_id") or ""),
            )
        except Exception as del_exc:  # noqa: BLE001
            # Belt-and-suspenders for engines whose probe cannot distinguish
            # 'missing' from 'unknowable' (returned None): a missing-object
            # error on the DELETE itself means the table is already gone,
            # not that the rollback failed.
            _missing = any(
                tok in str(del_exc).lower()
                for tok in (
                    "invalid object",
                    "does not exist",
                    "unknown table",
                    "no such table",
                    "not exist",
                    "undefined table",
                    "base table or view not found",
                )
            )
            if not _missing:
                raise
            removed = True
            remaining_after.append(0)
            continue
        removed_any = removed_any or removed
        remaining_after.append(precount_table(db_type, cfg, _t) or 0)
    removed = removed_any
    after = max(remaining_after) if remaining_after else 0
    dest_summary["partial_batch_rows_after_undo"] = after
    if after == 0 and (removed or dropped_all):
        dest_summary["partial_batch_removed"] = True
        if dropped_all:
            return (
                "The destination had no such table before this run. The table "
                "this run created was dropped, so the destination is "
                "byte-identical to before."
            )
        return (
            "The destination was empty before this run. The partial batch was "
            "removed, so the table is empty again."
        )
    if after == 0:
        dest_summary["partial_batch_removed"] = True
        return "The destination was empty before this run and is still empty."
    return (
        "The destination was empty before this run, but the partial batch "
        "could not be removed. The failed rows are still in the table."
    )


def _object_store_family(destination: Any) -> str:
    from services.dest_precount import _object_store_kind

    fmt = str(getattr(destination, "format", "") or "")
    kind = _object_store_kind(fmt)
    return kind if kind in {"s3", "gcs", "adls"} else ""


def _object_store_kind(db_type: str) -> str:
    from services.dest_precount import _object_store_kind as family

    return family(db_type)


def _clear_object_store(
    dest_summary: dict[str, Any],
    kind: str,
    cfg: dict[str, Any],
    *,
    schema: str,
    table_name: str,
) -> str:
    """Delete the object this run created when the bucket key was absent before.

    A blank CSV counts as zero rows and a JSONL of ``{}`` counts as rows.
    Either way the key is the partial batch. Saying the destination is still
    empty while the object remains is a false proof (R3-C1).
    """
    from connectors.object_store_common import normalize_object_base_key
    from connectors.object_store_leftover import _delete_object_key
    from services.dest_precount import observed_object_export_keys

    bucket = str(cfg.get("database") or "").strip()
    prefix = schema or str(cfg.get("schema") or "")
    base = normalize_object_base_key(table_name, prefix, str(cfg.get("file_format") or ""))
    # Presence is the listing, not the Gate-8 read set. That set invents
    # ``[base]`` when the parts prefix is empty so a GET is attempted, and
    # undo used to report that invented key as a file still in the bucket.
    present = observed_object_export_keys(kind, cfg, bucket, table_name)
    keys = list(present or [])
    if present is not None and base not in keys:
        keys.append(base)
    deleted = False
    error = ""
    for obj_key in keys:
        try:
            _delete_object_key(kind, cfg, bucket, obj_key)
            deleted = True
        except Exception as exc:  # noqa: BLE001 — a missing key is not presence
            error = str(exc)
    after_keys = observed_object_export_keys(kind, cfg, bucket, table_name)
    dest_summary["partial_batch_rows_after_undo"] = (
        None if after_keys is None else len(after_keys)
    )
    if after_keys == []:
        dest_summary["partial_batch_removed"] = True
        if present == []:
            return (
                "The destination object was absent before this run and is still absent."
            )
        return (
            "The destination object was absent before this run. "
            "The partial object was removed."
        )
    if after_keys:
        return (
            "The destination was empty before this run, but the partial object "
            f"could not be removed ({base}). The failed file is still in the bucket."
            + (f" {error}" if error else "")
        )
    if deleted or present == []:
        dest_summary["partial_batch_removed"] = True
        return (
            "The destination object was absent before this run. "
            "The partial object was removed."
        )
    return (
        "The destination was empty before this run. The bucket listing did not "
        "confirm whether the object remains, so this run does not claim the "
        "failed file is still in the bucket."
    )


def _delete_all_rows(
    db_type: str,
    cfg: dict[str, Any],
    *,
    schema: str,
    table_name: str,
    job_id: str = "",
) -> bool:
    from connectors.sql_identifiers import quote_table_ref

    if db_type == "sqlite":
        from connectors.sqlite_common import sqlite_file_path

        database = sqlite_file_path(
            str(cfg.get("database") or ""),
            str(cfg.get("connection_string") or ""),
            str(cfg.get("host") or ""),
        )
        if not database:
            return False
        ref = quote_table_ref(table_name, dialect="sqlite")
        with closing(sqlite3.connect(database)) as conn:
            conn.execute(f"DELETE FROM {ref}")  # nosec B608
            conn.commit()
        return True

    if db_type == "postgresql":
        from connectors.postgresql_conn import get_connection

        ref = quote_table_ref(table_name, schema or "public", dialect="postgresql")
        conn = get_connection(
            host=str(cfg.get("host") or ""),
            port=int(cfg.get("port") or 5432),
            database=str(cfg.get("database") or ""),
            username=str(cfg.get("username") or ""),
            password=str(cfg.get("password") or ""),
            connection_string=str(cfg.get("connection_string") or ""),
            ssl=bool(cfg.get("ssl", False)),
        )
        try:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {ref}")  # nosec B608
            from services.file_manifest import forget_job

            forget_job(
                conn,
                dialect="postgresql",
                schema=schema or "public",
                job_id=job_id,
            )
            conn.commit()
        finally:
            conn.close()
        return True

    if db_type == "mysql":
        from connectors.mysql_conn import get_connection

        ref = quote_table_ref(table_name, dialect="mysql")
        conn = get_connection(
            host=str(cfg.get("host") or ""),
            port=int(cfg.get("port") or 3306),
            database=str(cfg.get("database") or ""),
            username=str(cfg.get("username") or ""),
            password=str(cfg.get("password") or ""),
            connection_string=str(cfg.get("connection_string") or ""),
            ssl=bool(cfg.get("ssl", False)),
        )
        try:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {ref}")  # nosec B608
            conn.commit()
        finally:
            conn.close()
        return True

    if db_type in {"mongodb", "mongo"}:
        from pymongo import MongoClient
        from connectors.mongodb_common import guarded_mongo_client

        from src.transfer.adapters import mongodb_connection_string

        database = str(cfg.get("database") or "")
        if not database:
            return False
        client: MongoClient = guarded_mongo_client(
            mongodb_connection_string(cfg),
            workspace_id=str(cfg.get("workspace_id") or ""),
            serverSelectionTimeoutMS=5000,
        )
        try:
            client[database][table_name].delete_many({})
        finally:
            client.close()
        return True

    if db_type == "bigquery":
        from connectors.bigquery_conn import get_client
        from services.copy_bigquery_common import bigquery_table_ref
        from services.dest_precount import _bigquery_run_job

        project = str(cfg.get("database") or cfg.get("project_id") or "")
        dataset = str(schema or cfg.get("schema") or cfg.get("dataset") or "")
        if not project or not dataset:
            return False
        client = get_client(
            project_id=project,
            credentials_path=str(cfg.get("connection_string") or ""),
            service_account=str(cfg.get("service_account") or ""),
            host=str(cfg.get("host") or ""),
            port=int(cfg.get("port") or 0),
        )
        ref = bigquery_table_ref(project, dataset, table_name)
        _bigquery_run_job(client, f"DELETE FROM {ref} WHERE TRUE")  # nosec B608
        return True

    from services.dialect_profiles import warehouse_sql_quote_dialect
    from services.dest_precount import warehouse_sql_engine, warehouse_sql_table_ref

    dialect = warehouse_sql_quote_dialect(db_type)
    if dialect not in {"sqlserver", "oracle"}:
        # SQLAlchemy-writer dialects (DuckDB, Trino, RisingWave, Databricks…)
        # fell through to "left in place" with no cleanup attempted — a
        # failed batch on them silently stayed landed (G-1i-R1 / X-18).
        from src.transfer.connector_capabilities import resolve_driver_type

        raw_type = str(cfg.get("type") or db_type)
        if resolve_driver_type(raw_type) != "generic_sql":
            return False
        dialect = db_type
    import sqlalchemy as sa

    ref = warehouse_sql_table_ref(dialect, cfg, schema, table_name)
    with warehouse_sql_engine(db_type, cfg) as engine:
        with engine.begin() as conn:
            conn.execute(sa.text(f"DELETE FROM {ref}"))  # nosec B608
    return True
