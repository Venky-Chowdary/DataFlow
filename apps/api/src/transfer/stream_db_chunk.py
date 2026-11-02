"""Per-chunk write path of the streaming DB loop."""

from __future__ import annotations

import time
from functools import partial
from typing import Any

from connectors.writer_common import (
    map_rows_for_fingerprint,
    resolve_target_columns,
    row_fingerprints,
)
from services.data_quality import BatchDriftDetector, run_integrity_audit
from services.dest_precount import matrix_to_key_tuples
from services.dialect_profiles import schema_from_cfg
from services.phase_profile import PHASE_TRANSFORM_WRITE
from services.row_conservation import observe_keyed_batch

from .adapters import WriteBatchBlocked
from .stream_routes import _purge_route_key_tuples, _split_routed_batch
from .stream_row_accounting import (
    _raw_page_cursor,
    _raw_page_cursor_bounded,
    _raw_page_filtered,
    _raw_page_keyset,
    _raw_page_rows,
)
from .type_mapper import ddl_carrier_type
from . import stream as _stream


def _absorb_schemaless_discovered_attrs(run, batch) -> None:
    cfg = run.cfg
    from connectors.header_union import (
        SCHEMALESS_SOURCE_TYPES,
        union_attribute_keys,
    )

    if cfg.src_type not in SCHEMALESS_SOURCE_TYPES or not batch or not getattr(batch, "headers", None):
        return
    _pad_schemaless_absent_attrs(cfg, batch)

    with cfg._schema_absorb_lock:
        known = set(cfg.columns)
        new_headers = [h for h in batch.headers if h and h not in known]
        if not new_headers and set(batch.headers).issubset(known):
            return
        cfg.columns = union_attribute_keys(cfg.columns, batch.headers)
        mapped_sources = {str(m.get("source") or "") for m in cfg.mappings}
        meta = getattr(batch, "meta", None) or {}
        native_types = meta.get("native_types") if isinstance(meta, dict) else {}
        if not isinstance(native_types, dict):
            native_types = {}
        for h in new_headers:
            lt = str(native_types.get(h) or cfg.schema.get(h) or "VARCHAR")
            cfg.schema[h] = lt
            cfg.column_types[h] = ddl_carrier_type(lt)
            if h not in mapped_sources:
                cfg.mappings.append({
                    "source": h,
                    "target": h,
                    "confidence": 0.9,
                    # Force ADD COLUMN on SQL dests when attrs appear after CREATE.
                    "create_new": True,
                    "assignment_strategy": "create_compatible_new",
                })
                mapped_sources.add(h)
                cfg.ddl_log.append(
                    f"{cfg.dest_type.upper()} COLUMN {h} {_stream.ddl_type(cfg.dest_type, cfg.schema.get(h, 'string'))} "
                    f"(discovered mid-transfer from {cfg.src_type} — no silent drop)"
                )
        if new_headers:
            cfg.backfill_new_fields = True
            cfg.target_cols, _ = resolve_target_columns(cfg.mappings, cfg.column_types, preserve_case=True)


def _pad_schemaless_absent_attrs(cfg, batch) -> None:
    """A mapped attribute no document on this page carries is *absent*, not unknown.

    A schemaless reader names a page's columns from the documents it returned,
    so an incremental page whose changed documents all lack ``maybe`` has no
    ``maybe`` header, and the writer refused every row ("mapped source column
    not found in batch headers"): run 2 of a Mongo sync wrote 0 rows (D211).
    The reader already spells a field missing from one document as the missing
    sentinel (NULL on insert, left alone on update); a field missing from the
    whole page is the same fact.
    """
    from services.value_serializer import DF_MISSING_SENTINEL

    present = set(batch.headers)
    absent = [
        src
        for src in dict.fromkeys(str(m.get("source") or "") for m in cfg.mappings or [])
        if src and src not in present and src in (cfg.schema or {})
    ]
    if not absent or not all(isinstance(r, (list, tuple)) for r in batch.rows):
        return
    from services.value_serializer import SQL_NULL_SENTINEL

    # A flattened child (``maybe_v``) of a parent the row *has* (null, or an
    # object without that key) is null, not missing: missing would keep the
    # previous run's value on upsert. Same rule as the Mongo reader's projection.
    parents = {
        col: [i for i, h in enumerate(batch.headers) if h and col.startswith(f"{h}_")]
        for col in absent
    }

    def _cell(row, col):
        for i in parents[col]:
            cell = row[i] if i < len(row) else DF_MISSING_SENTINEL
            if cell == SQL_NULL_SENTINEL or str(cell).lstrip().startswith("{"):
                return SQL_NULL_SENTINEL
        return DF_MISSING_SENTINEL

    batch.headers = [*batch.headers, *absent]
    batch.rows = [[*row, *(_cell(row, c) for c in absent)] for row in batch.rows]


def _ensure_snowflake_conn(run) -> Any:
    cfg = run.cfg
    tally = run.tally
    if tally.sf_conn_state["conn"] is not None:
        return tally.sf_conn_state["conn"]
    from connectors.snowflake_conn import get_connection, normalize_account

    from services.connector_auth import engine_login_role

    pem = str(cfg.dest_cfg.get("private_key") or "")
    tally.sf_conn_state["conn"] = get_connection(
        account=normalize_account(cfg.dest_cfg.get("host", "")),
        username=cfg.dest_cfg.get("username", ""),
        password=cfg.dest_cfg.get("password", ""),
        database=cfg.dest_cfg.get("database", ""),
        schema=cfg.dest_cfg.get("schema", "PUBLIC"),
        warehouse=cfg.dest_cfg.get("warehouse", ""),
        connection_string=cfg.dest_cfg.get("connection_string", ""),
        role=engine_login_role(cfg.dest_cfg.get("auth_role"), cfg.dest_cfg.get("role")),
        private_key=pem,
        private_key_passphrase=str(cfg.dest_cfg.get("password") or "") if pem else "",
    )
    return tally.sf_conn_state["conn"]


def _ensure_pg_conn(run) -> Any:
    cfg = run.cfg
    tally = run.tally
    existing = tally.pg_conn_state.get("conn")
    if existing is not None:
        try:
            if getattr(existing, "closed", 0) == 0:
                return existing
        except Exception:
            pass
        tally.pg_conn_state["conn"] = None
    from connectors.postgresql_conn import get_connection as pg_get_connection

    pg_port = int(
        cfg.dest_cfg.get("port")
        or (5439 if cfg.dest_type == "redshift" else 5432)
    )
    conn = pg_get_connection(
        host=cfg.dest_cfg.get("host", ""),
        port=pg_port,
        database=cfg.dest_cfg.get("database", ""),
        username=cfg.dest_cfg.get("username", ""),
        password=cfg.dest_cfg.get("password", ""),
        connection_string=cfg.dest_cfg.get("connection_string", ""),
        ssl=cfg.dest_cfg.get("ssl", False),
    )
    try:
        conn.autocommit = False
    except Exception:
        pass
    tally.pg_conn_state["conn"] = conn
    return conn


def _process_db_chunk(run, idx: int, batch: Any) -> dict[str, Any]:
    """Timed wrapper around the transform + destination write for one chunk.

    Runs on the worker pool, so these totals overlap the reader's — that is
    what ``overlap_factor`` in the profile snapshot reports.
    """
    cfg = run.cfg
    rows = len(getattr(batch, "rows", None) or [])
    started = time.perf_counter()
    try:
        return _process_db_chunk_inner(run, idx, batch)
    finally:
        cfg.phase_profile.add(
            PHASE_TRANSFORM_WRITE, time.perf_counter() - started, rows=rows
        )


def _fk_held_out(run, batch: Any) -> list[dict[str, Any]]:
    return list(getattr(batch, "fk_orphan_details", None) or [])


def _kafka_meta_rows(run, batch: Any) -> tuple[list[dict[str, Any]], int]:
    """Broker-side rejects (poison messages) and tombstone count the Kafka
    reader carried on ``batch.meta`` — folded into this page's rejected
    ledger so poison lands in the DLQ, never as an all-NULL row."""
    meta = getattr(batch, "meta", None)
    if not isinstance(meta, dict):
        return [], 0
    return (
        list(meta.get("rejected_rows") or []),
        int(meta.get("kafka_tombstones_seen") or 0),
    )


def _chunks_for_page(run, idx: int, batch: Any) -> int:
    """The batch count the writer is told while writing page ``idx``.

    A Kafka topic has no row count to size the run by, so the estimate
    can undershoot; a staged overwrite publishes on the last page and
    would never publish (or publish early). The reader flags the page that
    reaches the run's frozen log end, which is the last one.
    """
    cfg = run.cfg
    if cfg.src_type != "kafka":
        return cfg.chunks
    meta = getattr(batch, "meta", None)
    if isinstance(meta, dict) and meta.get("final_page"):
        return idx
    return max(cfg.chunks, idx + 1)


def _process_db_chunk_inner(run, idx: int, batch: Any) -> dict[str, Any]:
    cfg = run.cfg
    tally = run.tally
    from services.sync_cursor import is_overwrite_sync
    from services.sync_cursor import max_cursor_value
    from services.keyset_pagination import max_keyset_bookmark
    batch_sdk_state = None
    if cfg.sdk_source and isinstance(getattr(batch, "meta", None), dict):
        batch_sdk_state = batch.meta.get("sdk_state", "")
    kafka_rejects, kafka_tombstones = _kafka_meta_rows(run, batch)
    if kafka_rejects or kafka_tombstones:
        tally.dest_summary["rejected_details"] = list(
            tally.dest_summary.get("rejected_details") or []
        ) + kafka_rejects
        tally.dest_summary["rejected_rows"] = int(
            tally.dest_summary.get("rejected_rows") or 0
        ) + len(kafka_rejects)
        tally.dest_summary["kafka_tombstones_seen"] = int(
            tally.dest_summary.get("kafka_tombstones_seen") or 0
        ) + kafka_tombstones
        tally.rejected_total += len(kafka_rejects)
        if kafka_rejects and cfg.job_id:
            from services.quarantine_dlq import persist_rejected_rows

            persist_rejected_rows(
                job_id=cfg.job_id,
                rejected_details=kafka_rejects,
                workspace_id=cfg.workspace_id,
                source="transfer",
                connector=cfg.src_type,
            )
    if not batch or not getattr(batch, "rows", None):
        # A page a filter or a recipe emptied still consumed source rows, so
        # the offset, the watermark and the keyset bookmark advance past it.
        # Reporting zero here would re-read that page on resume for ever.
        # A page the FK orphan guard emptied is *quarantine*, not removal:
        # its rows must reach the DLQ and the ledger as rejects.
        orphans = _fk_held_out(run, batch)
        return {
            "batch_written": 0,
            "last_checksum": "",
            "dest_summary": (
                {
                    "rejected_details": orphans,
                    "rejected_rows": len(orphans),
                    "orphan_fk_rows": len(orphans),
                }
                if orphans
                else {}
            ),
            "rejected": len(orphans),
            "coerced_null": 0,
            "warnings": [],
            "batch_max": _raw_page_cursor(batch) or None,
            "batch_keyset": _raw_page_keyset(batch) or None,
            "batch_rows": _raw_page_rows(batch) + len(kafka_rejects),
            "filter_page_removed": _raw_page_filtered(batch),
            "cursor_bounded_page": _raw_page_cursor_bounded(batch),
            "shape_page_removed": max(
                0,
                _raw_page_rows(batch)
                - _raw_page_filtered(batch)
                - _raw_page_cursor_bounded(batch)
                - len(orphans),
            ),
            "shape_page_diverted": int(
                getattr(batch, "raw_page_diverted", 0) or 0
            ),
            "shape_page_held": int(
                getattr(batch, "raw_page_held", 0) or 0
            ),
            "reconcile_sample_rows": [],
            "fingerprints": [],
            "sdk_state_after": batch_sdk_state,
        }
    # Absorb sparse schemaless attributes discovered on this page.
    _absorb_schemaless_discovered_attrs(run, batch)
    local_warnings: list[str] = []
    sample_rows: list[dict[str, Any]] = [
        dict(zip(batch.headers, row)) for row in batch.rows[:cfg._RECONCILE_SAMPLE_CAP]
    ]
    # Per-batch data-quality / anomaly gate for database streams.
    # Routed batches are audited PER ROUTE inside the split loop — each
    # route table is a separate destination; auditing the mixed batch
    # reads one route's mapped/diverted population as another's NULLs
    # and shrinkage (RT-3).
    _batch_routed = (
        cfg.shape_runner is not None
        and getattr(cfg.shape_runner, "recipe", None) is not None
        and cfg.shape_runner.recipe.router_step is not None
    )
    if cfg.batch_quality_enabled and not _batch_routed:
        # Audit uniqueness must match the *write* posture, not only the sync
        # label. Resume upgrades insert→upsert while effective_sync stays
        # append — inventing a soft uniqueness check would last-row-wins.
        audit_sync = (
            "upsert" if cfg.write_mode == "upsert" else cfg.effective_sync
        )
        audit = run_integrity_audit(
            headers=batch.headers,
            rows=batch.rows,
            column_types=cfg.column_types,
            mappings=cfg.mappings,
            required_targets=cfg.pk_target_cols or [],
            primary_key=cfg.pk_source_col if cfg.pk_source_col in batch.headers else None,
            primary_key_columns=cfg.pk_audit_cols,
            validation_mode=cfg.validation_mode,
            dest_kind=cfg.dest_type,
            # Validate↔Run parity: Full append must not invent a uniqueness
            # hard-block after G9 unlocked Execute for the same sync mode.
            sync_mode=audit_sync,
        )
        if audit.issues:
            local_warnings.extend(audit.issues[:10])
        if audit.warnings:
            local_warnings.extend(audit.warnings[:10])
        if not audit.passed:
            raise ValueError(f"Batch {idx} failed data-quality audit: {'; '.join(audit.issues[:5])}")
        drift_warnings = cfg.drift_detector.check(audit.stats or {})
        if drift_warnings:
            if cfg.validation_mode == "maximum":
                raise ValueError(
                    f"Batch {idx} drift detected: {'; '.join(drift_warnings[:3])}"
                )
            local_warnings.extend(drift_warnings[:3])

    # Both bookmarks describe the page as the source handed it over: a recipe
    # may have dropped the row carrying the page's highest key, and
    # committing a lower bookmark would hand those rows over again.
    batch_max = None
    if cfg.incremental and cfg.cursor_source_col:
        batch_max = _raw_page_cursor(batch) or max_cursor_value(
            batch.rows, batch.headers, cfg.cursor_source_col, cfg.cursor_pk_source or None
        )
    batch_keyset = None
    # Persist a PK bookmark even on a snapshot scan so the next resume
    # seeks instead of draining the prefix again. Never invent a bookmark
    # from an arbitrary first column — that skips tied peers.
    if cfg.use_keyset or cfg.keyset_order_cols:
        batch_keyset = _raw_page_keyset(batch) or max_keyset_bookmark(
            batch.rows,
            batch.headers,
            cfg.keyset_order_cols
            or ([cfg.keyset_col] + ([cfg.keyset_tiebreak] if cfg.keyset_tiebreak else [])),
        )

    from services.preflight_source_kind import source_is_record_batch

    # Object-store and file extracts spell a blank numeric cell as "".
    # That blank is SQL NULL on a nullable typed column (R8-003). A
    # database integer cannot store "" either, but only when the source
    # carrier itself has no empty-string domain — that path stays in the
    # blank-cell contract and is not flipped on here.
    write_kwargs: dict[str, Any] = {
        # File uploads reaching this stream path must opt in by their
        # real kind — hardcoding "database" made a csv upload fall back
        # to the extract rule and reject blanks a file preflight had
        # already promised as SQL NULL (D1).
        "empty_cells_as_null": source_is_record_batch(
            getattr(cfg.source, "kind", "") or "database", cfg.src_type
        ),
    }
    if cfg.dest_type == "snowflake":
        write_kwargs["connection"] = _ensure_snowflake_conn(run)
        write_kwargs["close_connection"] = False
        write_kwargs["skip_session_setup"] = bool(tally.sf_conn_state["session_ready"])
    elif cfg.dest_type in ("postgresql", "redshift") and cfg.max_workers == 1:
        # One connection for the job. Later batches stay on this thread
        # (drive_ordered_batches). psycopg2 deadlocks once that handle
        # is used from the pool, which is how a 100k load stopped after
        # the pages that had already committed.
        write_kwargs["connection"] = _ensure_pg_conn(run)
        write_kwargs["close_connection"] = False
        write_kwargs["connection_holder"] = tally.pg_conn_state

    router_step = (
        cfg.shape_runner.recipe.router_step
        if cfg.shape_runner is not None and cfg.shape_runner.recipe is not None
        else None
    )
    _rconn = None
    if (
        router_step is not None
        and cfg.dest_type in ("postgresql", "redshift")
        and write_kwargs.get("connection") is not None
    ):
        # A routed run writes several tables; a failure must not leave
        # some routes committed and others not (RT-5). On the shared PG
        # connection, arm a held transaction for ANY sync mode — every
        # route's commits defer until finish_held_overwrite at stream end
        # (commit all or roll back all). The overwrite path already binds
        # one on truncate; begin() is idempotent.
        from connectors.write_resilience import (
            PgOverwriteTransaction,
            bind_overwrite_transaction,
            overwrite_transaction_for,
        )

        _rconn = write_kwargs["connection"]
        _rheld = overwrite_transaction_for(_rconn)
        if _rheld is None:
            _rheld = PgOverwriteTransaction()
            bind_overwrite_transaction(_rconn, _rheld)
        _rheld.begin(job_id=cfg.job_id or "")
        # Failed-Gate-8 restore guard: a replace-mode routed run into
        # non-empty route tables takes a row-level COPY of each occupied
        # table inside this transaction (CREATE TABLE bak (LIKE t
        # INCLUDING ALL) + INSERT SELECT). The live table object is never
        # renamed or replaced — the customer's DDL, indexes, grants and
        # dependent views stay bound to it (P0-G1). A write failure rolls
        # the copies back with the rows; a post-write proof failure lets
        # _settle_overwrite_backup DELETE+INSERT the prior rows back and
        # drop the copies (the txn commits before Gate-8, so the backup
        # is the only way to restore prior content).
        _rsync_now = str(cfg.effective_sync or "")
        if is_overwrite_sync(_rsync_now) and not tally.route_backup_state.get(
            "armed"
        ):
            tally.route_backup_state["armed"] = True
            from connectors.sql_identifiers import (
                quote_table_ref,
                split_qualified_table,
            )
            from connectors.table_manager import pg_route_backup_name

            _rprecount = tally.route_backup_state.get("precount") or {}
            _rbaks: dict[str, dict[str, str]] = {}
            _rtargets_map = dict(router_step.options.get("targets") or {})
            for _rr in router_step.options.get("routes") or []:
                _rrn = str(_rr.get("name") or "")
                if not _rrn or int(_rprecount.get(_rrn) or 0) <= 0:
                    continue
                _rrt = str(
                    _rtargets_map.get(_rrn) or f"{cfg.dest_table}__{_rrn}"
                )
                _rschema, _rtbl = split_qualified_table(
                    _rrt, str(cfg.dest_cfg.get("schema") or "public")
                )
                _rbak = pg_route_backup_name(_rtbl, cfg.job_id or "")
                try:
                    _live_ref = quote_table_ref(
                        _rtbl, _rschema, dialect="postgresql"
                    )
                    _bak_ref = quote_table_ref(
                        _rbak, _rschema, dialect="postgresql"
                    )
                    with _rconn.cursor() as _rcur:
                        # Backup existence is schema-qualified — a
                        # same-named table in another schema must not
                        # match (P0-G2).
                        _rcur.execute(
                            "SELECT 1 FROM information_schema.tables "
                            "WHERE table_schema = %s AND table_name = %s",
                            (_rschema, _rtbl),
                        )
                        if not _rcur.fetchone():
                            raise LookupError(
                                f"route table {_rschema}.{_rtbl} not found"
                            )
                        _rcur.execute(
                            f"CREATE TABLE {_bak_ref} "
                            f"(LIKE {_live_ref} INCLUDING ALL)"
                        )
                        _rcur.execute(
                            f"INSERT INTO {_bak_ref} SELECT * FROM {_live_ref}"
                        )
                    _rbaks[_rrn] = {
                        "table": _rtbl,
                        "backup": _rbak,
                        "schema": _rschema,
                    }
                except Exception as _rbak_exc:
                    _stream.logger.warning(
                        "Route %s backup copy skipped (%s); "
                        "post-write proof failure cannot restore it",
                        _rrn,
                        _rbak_exc,
                    )
            if _rbaks:
                tally.route_backup_state["backups"] = dict(_rbaks)
                _dextra = dict(getattr(cfg.destination, "extra", None) or {})
                _dextra["route_backups"] = dict(_rbaks)
                cfg.destination.extra = _dextra
    write_calls: list[tuple[str | None, str, Any]] = []
    if cfg.row_apply_plan is not None:
        if router_step is not None:
            raise ValueError(
                "router transform cannot write through a destination "
                "stored-procedure plan — route outputs are separate tables"
            )
        from .stream_dest_procedure import write_procedure_chunk

        records = [dict(zip(batch.headers, row)) for row in batch.rows]
        write_calls.append(
            (
                None,
                cfg.dest_table,
                partial(write_procedure_chunk, cfg.destination, records, cfg.row_apply_plan),
            )
        )
    else:
        from .stream_write import _write_batch

        splits = (
            _split_routed_batch(batch, cfg.shape_runner.recipe, cfg.dest_table)
            if router_step is not None
            else [(None, cfg.dest_table, batch.headers, batch.rows)]
        )
        if (
            router_step is not None
            and cfg.write_mode == "upsert"
            and _rconn is not None
            and cfg.pk_source_cols
        ):
            # Dedupe-by-partition (P1-B): a keyed row that changed route
            # must not keep its old copy — delete every batch/diverted
            # key from EVERY route table, inside this held transaction,
            # before the route's upsert lands its current copy.
            _purge_route_key_tuples(
                _rconn,
                router_step=router_step,
                recipe=cfg.shape_runner.recipe,
                dest_table=cfg.dest_table,
                dest_cfg=cfg.dest_cfg,
                batch=batch,
                pk_source_cols=cfg.pk_source_cols,
                pk_target_cols=cfg.pk_target_cols,
                pk_audit_cols=cfg.pk_audit_cols,
                diverted_keys=tally.shape_divert_key_tuples,
            )
        created_tables = tally.ddl_state.setdefault("created_tables", set())
        _rmap_decls = (
            {
                str(_rd.get("name") or ""): (_rd.get("map") or {})
                for _rd in (router_step.options.get("routes") or [])
            }
            if router_step is not None
            else {}
        )
        for _rname, _rtable, _rheaders, _rrows in splits:
            create_this = (
                _rtable not in created_tables
                if _rname is not None
                else not tally.ddl_state["created"]
            )
            if _rname is not None:
                # The route table declares only its own columns — the
                # ROUTE_COLUMN tag and other routes' map targets must not
                # reach the writer (a "mapped source not in headers" risk
                # contract would refuse the batch as a NULL-invent).
                _rheader_set = set(_rheaders)
                if _rmap_decls.get(_rname):
                    # A mapped route's headers are already the TARGET
                    # names, so source->target lookup against the shared
                    # mappings finds nothing — build identity mappings
                    # keyed by target name and carry the SOURCE column's
                    # type through the map so the route DDL keeps INT
                    # instead of degrading to TEXT (RT-8).
                    _rsrc_of = {
                        str(t): str(s)
                        for s, t in _rmap_decls[_rname].items()
                    }
                    _rmappings = [
                        {"source": h, "target": h, "confidence": 0.95}
                        for h in _rheaders
                    ]
                    _rtargets = set(_rheaders)
                    _rcol_types = {
                        h: cfg.column_types[_rsrc_of[h]]
                        for h in _rheaders
                        if _rsrc_of.get(h) in cfg.column_types
                    }
                else:
                    _rmappings = [
                        m for m in cfg.mappings
                        if str(m.get("source") or "") in _rheader_set
                    ]
                    _rtargets = {
                        str(m.get("target") or m.get("source") or "")
                        for m in _rmappings
                    }
                    _rcol_types = {
                        k: v for k, v in cfg.column_types.items() if k in _rtargets
                    }
                if True:
                    # Value-level proof per route: fingerprint this route's
                    # mapped rows exactly as the non-routed path does, so
                    # Gate-8 can compare each route table's read-back
                    # checksum against what this run wrote (RT accuracy).
                    # Always fingerprint for routes — a whole-source
                    # re-read cannot produce per-route digests.
                    try:
                        _r_mapped_fp, _ = map_rows_for_fingerprint(
                            headers=_rheaders,
                            data_rows=_rrows,
                            mappings=_rmappings,
                            target_cols=sorted(_rtargets),
                            column_types=_rcol_types,
                            error_policy=cfg.stream_error_policy,
                            dest_types=cfg.fingerprint_dest_types,
                            preserve_case=True,
                            dest_kind=cfg.dest_bind_dialect,
                            destination_pk_columns=list(cfg.pk_target_cols or []) or None,
                            empty_cells_as_null=bool(
                                write_kwargs.get("empty_cells_as_null")
                            ),
                        )
                        if _r_mapped_fp:
                            from services.fingerprint_accumulator import (
                                FingerprintAccumulator,
                            )

                            acc = tally.route_fp_acc.setdefault(
                                _rname, FingerprintAccumulator()
                            )
                            tally.route_fp_cols.setdefault(
                                _rname, sorted(_rtargets)
                            )
                            acc.add_many(
                                row_fingerprints(
                                    _r_mapped_fp,
                                    sorted(_rtargets),
                                    dest_db_type=cfg.dest_bind_dialect,
                                    dest_types=cfg.digest_dest_types,
                                )
                            )
                    except Exception as exc:
                        _stream.logger.warning(
                            "Route fingerprint skipped for %s: %s", _rname, exc
                        )
            else:
                _rmappings = cfg.mappings
                _rcol_types = cfg.column_types
            if _rname is not None and cfg.batch_quality_enabled:
                # Each route table is its own destination — audit the
                # route's rows against its own columns, never the mixed
                # batch (RT-3).
                try:
                    audit = run_integrity_audit(
                        headers=_rheaders,
                        rows=_rrows,
                        column_types=_rcol_types,
                        mappings=_rmappings,
                        required_targets=cfg.pk_target_cols or [],
                        primary_key=cfg.pk_source_col
                        if cfg.pk_source_col in _rheaders
                        else None,
                        primary_key_columns=[
                            c for c in (cfg.pk_audit_cols or []) if c in _rheaders
                        ],
                        validation_mode=cfg.validation_mode,
                        dest_kind=cfg.dest_type,
                        sync_mode=(
                            "upsert"
                            if cfg.write_mode == "upsert"
                            else cfg.effective_sync
                        ),
                    )
                    if audit.issues:
                        local_warnings.extend(
                            f"route {_rname}: {i}" for i in audit.issues[:10]
                        )
                    if audit.warnings:
                        local_warnings.extend(
                            f"route {_rname}: {w}" for w in audit.warnings[:10]
                        )
                    if not audit.passed:
                        raise ValueError(
                            f"Batch {idx} route {_rname} failed "
                            f"data-quality audit: "
                            f"{'; '.join(audit.issues[:5])}"
                        )
                    _rdet = cfg.route_drift_detectors.setdefault(
                        _rname, BatchDriftDetector()
                    )
                    drift_warnings = _rdet.check(audit.stats or {})
                    if drift_warnings:
                        if cfg.validation_mode == "maximum":
                            raise ValueError(
                                f"Batch {idx} route {_rname} drift "
                                f"detected: "
                                f"{'; '.join(drift_warnings[:3])}"
                            )
                        local_warnings.extend(
                            f"route {_rname}: {w}"
                            for w in drift_warnings[:3]
                        )
                except ValueError:
                    raise
                except Exception as exc:
                    _stream.logger.warning(
                        "Route %s integrity audit skipped: %s", _rname, exc
                    )
            write_calls.append(
                (
                    _rname,
                    _rtable,
                    partial(
                        _write_batch,
                        cfg.dest_type,
                        cfg.destination,
                        cfg.dest_cfg,
                        _rtable,
                        _rheaders,
                        _rrows,
                        _rmappings,
                        _rcol_types,
                        create_table=create_this,
                        on_checkpoint=None,
                        chunk_idx=idx,
                        total_chunks=_chunks_for_page(run, idx, batch),
                        rows_so_far=0,
                        write_mode=cfg.write_mode,
                        conflict_columns=cfg.pk_target_cols or None,
                        sync_mode=cfg.effective_sync,
                        backfill_new_fields=cfg.backfill_new_fields,
                        error_policy=cfg.stream_error_policy,
                        job_id=cfg.job_id,
                        skip_preflight=cfg.skip_preflight,
                        source_schema_catalog=cfg.source_schema_catalog,
                        **write_kwargs,
                    ),
                )
            )
    # Keyed census and the inline write-pass fingerprint are single-table
    # mechanisms — routed rows land in per-route tables, verified per
    # route at Gate-8 instead.
    if tally.keyed_census_acc is not None and router_step is None:
        observe_keyed_batch(
            tally.keyed_census_acc,
            headers=batch.headers,
            rows=batch.rows,
            mappings=cfg.mappings,
            key_columns=cfg.pk_target_cols,
            db_type=cfg.dest_type,
            cfg=cfg.dest_cfg,
            schema=schema_from_cfg(cfg.dest_type, cfg.dest_cfg),
            table_name=cfg.dest_table,
        )
    # Phase F1 — fingerprint the mapped rows from this chunk (same policy as
    # post-write re-read) so the write pass can own the source checksum.
    # Tombstones are already stripped: hashing deleted keys as writes would
    # fail Gate-8 after a correct hard DELETE.
    inline_fps: list[Any] = []
    if tally.inline_fingerprints_used and router_step is None:
        try:
            mapped_fp, _ = map_rows_for_fingerprint(
                headers=batch.headers,
                data_rows=batch.rows,
                mappings=cfg.mappings,
                target_cols=cfg.target_cols,
                column_types=cfg.column_types,
                error_policy=cfg.stream_error_policy,
                dest_types=cfg.fingerprint_dest_types,
                preserve_case=True,
                dest_kind=cfg.dest_bind_dialect,
                destination_pk_columns=list(cfg.pk_target_cols or []) or None,
                empty_cells_as_null=bool(write_kwargs.get("empty_cells_as_null")),
            )
            if mapped_fp and cfg.pk_target_cols:
                from services.primary_key import sync_collapses_in_file_duplicates

                if sync_collapses_in_file_duplicates(cfg.effective_sync):
                    from connectors.writer_common import dedupe_rows

                    mapped_fp = dedupe_rows(
                        list(mapped_fp), list(cfg.pk_target_cols), list(cfg.target_cols)
                    )
            if mapped_fp:
                inline_fps = row_fingerprints(
                    mapped_fp,
                    cfg.target_cols,
                    dest_db_type=cfg.dest_bind_dialect,
                    dest_types=cfg.digest_dest_types,
                )
        except Exception as exc:
            _stream.logger.warning(
                "Inline write-pass fingerprint skipped for chunk %s: %s", idx, exc
            )

    if cfg.job_id:
        # Not the throttled progress callback: that skips calls inside
        # its interval, and a cancel during a long batch was ignored
        # until the next flush. The in-flight statement still ends on
        # its lock wait; this stops the next batch.
        from services.cancel_check import raise_if_job_cancelled
        from services.mongodb_service import get_mongodb_service

        raise_if_job_cancelled(
            get_mongodb_service(), str(cfg.job_id), rows_written=tally.written
        )
    try:
        batch_written = 0
        last_checksum = ""
        chunk_summary: dict[str, Any] = {}
        routes_wire: dict[str, dict[str, Any]] = {}
        for _rname, _rtable, _wop in write_calls:
            _w, _cs, _ds = _stream.with_retry(
                _wop,
                budget=_stream.RetryBudget(
                    max_attempts=cfg.retry.max_attempts,
                    base_delay_seconds=cfg.retry.base_delay_seconds,
                    max_delay_seconds=cfg.retry.max_delay_seconds,
                    exponential_base=cfg.retry.exponential_base,
                    jitter=cfg.retry.jitter,
                ),
                replay_safety=cfg.replay_safety,
            )
            batch_written += int(_w or 0)
            if _cs:
                last_checksum = _cs
            chunk_summary = _stream._merge_chunk_write_summaries(chunk_summary, _ds)
            if _rname is not None:
                tally.ddl_state["created_tables"].add(_rtable)
                routes_wire[_rname] = {
                    "table": _rtable,
                    "rows": int(_w or 0),
                    # The route's rename map — downstream evidence probes
                    # (identity watermark) translate source pk names to
                    # the route table's actual columns through this.
                    "map": dict(_rmap_decls.get(_rname) or {}),
                }
        if routes_wire:
            chunk_summary["routes"] = routes_wire
        # Fold orphan-FK holdouts into the batch's own rejected ledger so
        # they persist to the quarantine DLQ and balance Gate-8
        # conservation (read = written + rejected + filtered).
        fk_orphans = getattr(batch, "fk_orphan_details", None) or []
        if fk_orphans:
            det = list(chunk_summary.get("rejected_details") or [])
            det.extend(fk_orphans)
            chunk_summary["rejected_details"] = det
            chunk_summary["rejected_rows"] = int(
                chunk_summary.get("rejected_rows") or 0
            ) + len(fk_orphans)
            chunk_summary["orphan_fk_rows"] = int(
                chunk_summary.get("orphan_fk_rows") or 0
            ) + len(fk_orphans)
    except WriteBatchBlocked as blocked:
        if tally.keyed_census_acc is not None:
            tally.keyed_census_acc.reverse_last_live_batch()
        # Persist this batch's quarantine before aborting the stream —
        # bare RuntimeError would drop mid-write FAIL_JOB details.
        details = list(blocked.rejected_details or [])
        if details and cfg.job_id:
            from services.quarantine_dlq import persist_rejected_rows

            persist_rejected_rows(
                job_id=str(cfg.job_id),
                rejected_details=details,
                source="stream_batch_abort",
                connector=str(
                    getattr(cfg.destination, "format", None)
                    or getattr(cfg.destination, "kind", None)
                    or ""
                ),
            )
        raise
    except Exception:
        if tally.keyed_census_acc is not None:
            tally.keyed_census_acc.reverse_last_live_batch()
        raise
    if tally.keyed_census_acc is not None:
        if int(batch_written or 0) == 0:
            tally.keyed_census_acc.reverse_last_live_batch()
        else:
            tally.keyed_census_acc.keep_last_live_batch()
    if cfg.dest_type == "snowflake":
        tally.sf_conn_state["session_ready"] = True
    return {
        "batch_written": batch_written,
        "last_checksum": last_checksum,
        "dest_summary": chunk_summary,
        "rejected": int(chunk_summary.get("rejected_rows", 0) or 0),
        "coerced_null": int(chunk_summary.get("coerced_null_rows", 0) or 0),
        "warnings": (chunk_summary.get("warnings") or [])[:10] + local_warnings,
        "batch_max": batch_max,
        "batch_keyset": batch_keyset,
        # Source rows this page consumed — the offset and the run's read
        # population count them, including the ones a recipe removed and a
        # Kafka poison message (read, then quarantined).
        "batch_rows": _raw_page_rows(batch) + len(kafka_rejects),
        "filter_page_removed": _raw_page_filtered(batch),
        "cursor_bounded_page": _raw_page_cursor_bounded(batch),
        "shape_page_removed": max(
            0,
            _raw_page_rows(batch)
            - _raw_page_filtered(batch)
            - _raw_page_cursor_bounded(batch)
            - len(batch.rows)
            # FK orphans are rejects (already in this batch's ledger), not
            # recipe removals — counting both made a correct run look lossy.
            - len(_fk_held_out(run, batch)),
        ),
        # Quarantine-diverted and still-buffered rows left the page for
        # named reasons — neither is a recipe removal (DD-3).
        "shape_page_diverted": int(
            getattr(batch, "raw_page_diverted", 0) or 0
        ),
        "shape_page_held": int(
            getattr(batch, "raw_page_held", 0) or 0
        ),
        "reconcile_sample_rows": sample_rows,
        "fingerprints": inline_fps,
        "overwrite_keys": (
            matrix_to_key_tuples(
                batch.headers, batch.rows, cfg.mappings, cfg.pk_target_cols
            )
            if tally.overwrite_keys_acc is not None
            else None
        ),
        "sdk_state_after": batch_sdk_state,
    }
