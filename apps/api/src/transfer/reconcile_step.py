"""Gate 8 reconciliation for the universal transfer engine."""

from __future__ import annotations

import logging
from typing import Any

from connectors.writer_common import (
    map_rows_for_fingerprint,
    resolve_target_columns,
    transform_error_policy_for_validation_mode,
)
from services.dest_precount import (
    OVERWRITE_SOURCE_KEYS_KEY,
    PRECOUNT_KEY,
    VECTOR_IDENTITY_ENGINES,
    records_to_key_tuples,
    stamp_artifact_census,
    stamp_keyset_census,
    stamp_scd2_census,
    stamp_vector_census,
)
from services.reconcile_coverage import (
    NO_OP_DEST_UNCHANGED,
    SOURCE_DIGEST_ENGINE_POPULATION,
    SOURCE_DIGEST_REMAPPED_ROWS,
    SOURCE_DIGEST_SOURCE_REREAD,
    SOURCE_DIGEST_WRITE_PASS,
    SOURCE_DIGEST_WRITER_ACK,
    WHOLE_TABLE_NOT_COMPARABLE,
    WRITTEN_BATCH_KEYS,
    is_no_op_report,
)
from services.destination_key_collision_probe import (
    destination_enforces_key,
    sync_mode_appends_without_key_resolution,
)
from services.row_conservation import CENSUS_KEY, live_records_for_digest
from services.reconciliation import (
    KEYED_READBACK_ENGINES,
    TargetSampleUnavailable,
    checksum_rows,
    read_target_sample,
    reconcile,
    sample_compare_rows,
    stamp_post_write_phase,
    verify_target,
)

from .adapters import records_to_matrix, resolve_connector_config
from .adapters_introspect import _introspect_table_schema_rich
from .models import EndpointConfig


def _finalize_reconcile(
    payload: dict[str, Any],
    *,
    dest_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Every post-write reconcile return path gets an explicit phase."""
    out = stamp_post_write_phase(payload)
    snap = None
    if isinstance(dest_summary, dict):
        snap = dest_summary.get("source_snapshot")
        raw_before = dest_summary.get(PRECOUNT_KEY)
        if out.get(PRECOUNT_KEY) is None and isinstance(raw_before, int):
            out[PRECOUNT_KEY] = raw_before
    if isinstance(snap, dict) and snap:
        out["source_snapshot"] = dict(snap)
    from services.reconciliation import attach_dest_readback

    out = attach_dest_readback(out)
    job_id = ""
    if isinstance(dest_summary, dict):
        job_id = str(dest_summary.get("job_id") or dest_summary.get("_id") or "")
    if job_id:
        try:
            from services.lineage_telemetry import emit_reconciliation, persist_event_on_job

            event = emit_reconciliation(
                run_id=str(out.get("run_id") or job_id),
                job_id=job_id,
                source_count=int(out.get("source_rows") or 0),
                target_count=int(out.get("target_rows") or 0),
                checksum_ok=out.get("checksum_match") if out.get("checksum_match") is not None else None,
            )
            persist_event_on_job(job_id, event)
        except Exception:
            pass
    return out


_ADVISORY_KEY_TOKENS = ("not enforced", "advisory", "informational")


def _destination_enforces_single_key(
    db_type: str,
    cfg: dict[str, Any],
    *,
    schema: str,
    table_name: str,
    key_column: str,
) -> bool:
    """True when the destination itself rejects a duplicate of ``key_column``.

    Only a constraint the destination enforces makes a key-scoped digest
    comparable to the source digest: it is what guarantees the read-back returns
    one row per written key. A key named by the stream contract, the merge
    request or Map's identity inference guarantees nothing on an append-only
    write — appending the same batch twice into a keyless table left two rows per
    key, so the keyed read-back covered 2x the batch and its digest could never
    equal the source's. That failed a correct append with two hex strings.

    Enforcement is read from the destination catalog, and a catalog that
    declares its keys advisory (Snowflake/BigQuery-class ``NOT ENFORCED``) does
    not enforce them. Anything unproven answers False, which keeps the append
    delta as the proof rather than inventing comparability.
    """
    if not key_column or not table_name:
        return False
    probe_cfg = dict(cfg or {})
    if schema:
        probe_cfg.setdefault("schema", schema)
    try:
        _types, _nulls, keys = _introspect_table_schema_rich(
            db_type, probe_cfg, table_name, [], strict_namespace=True
        )
    except Exception as exc:
        logging.getLogger(__name__).debug(
            "destination key enforcement unproven for %s.%s: %s",
            table_name,
            key_column,
            exc,
            exc_info=exc,
        )
        return False
    warnings = " ".join(str(w).lower() for w in (keys.get("warnings") or []))
    if any(token in warnings for token in _ADVISORY_KEY_TOKENS):
        return False
    return destination_enforces_key(
        key_column,
        destination_pk_columns=list(keys.get("primary_key_columns") or []),
        destination_unique_keys=list(keys.get("unique_keys") or []),
    )


def _dest_types_from_mappings(mappings: list[dict]) -> dict[str, str]:
    return {
        str(m.get("target") or ""): str(
            m.get("target_type") or m.get("inferredType") or ""
        )
        for m in mappings
        if m.get("target")
        and (m.get("target_type") or m.get("inferredType"))
    }


def _compute_source_checksum_from_spool(
    source_spool: Any,
    columns: list[str],
    mappings: list[dict],
    source_schema: dict[str, str] | None,
    target_cols: list[str] | None,
    *,
    dest_db_type: str = "",
    dest_types: dict[str, str] | None = None,
    validation_mode: str = "strict",
    destination_pk_columns: list[str] | None = None,
) -> tuple[str, str]:
    """Remap Gate-8 from the engine spool in bundles — never rebuild ``records``."""
    from connectors.engine_record_spill import iter_fingerprints_from_spool
    from services.fingerprint_accumulator import FingerprintAccumulator

    if target_cols is None:
        target_cols, _ = resolve_target_columns(
            mappings, source_schema or {}, preserve_case=True
        )
    acc = FingerprintAccumulator()
    policy = transform_error_policy_for_validation_mode(validation_mode)
    acc.add_many(
        iter_fingerprints_from_spool(
            source_spool,
            mappings,
            target_cols,
            headers=list(getattr(source_spool, "headers", None) or columns),
            column_types=source_schema or {},
            dest_db_type=dest_db_type or "",
            dest_types=dest_types or {},
            error_policy=policy,
            destination_pk_columns=destination_pk_columns,
        )
    )
    return acc.digest(), SOURCE_DIGEST_REMAPPED_ROWS


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
    source_spool: Any = None,
) -> tuple[str, str]:
    """Return ``(digest, provenance)`` for the source side of Gate-8.

    When source ``records`` or an engine ``source_spool`` are available, always
    remap and fingerprint them. Preferring the writer checksum first made
    Gate-8 circular: dest digest was compared to the writer's own ack, not to
    the remapped source population.

    The writer checksum remains the fallback when no records or spool were
    supplied, and the provenance is returned with it so the report can say so.
    That fallback is not a corner case: a streaming pass hands over no rows,
    which is exactly how the large tables move, and labelling those runs
    ``full_checksum`` claimed two independent digests had agreed when only one
    digest existed.
    """
    if source_spool is not None and getattr(source_spool, "row_count", 0):
        return _compute_source_checksum_from_spool(
            source_spool,
            columns,
            mappings,
            source_schema,
            target_cols,
            dest_db_type=dest_db_type,
            dest_types=dest_types,
            validation_mode=validation_mode,
            destination_pk_columns=destination_pk_columns,
        )
    if not records:
        return str(writer_checksum or ""), SOURCE_DIGEST_WRITER_ACK
    _, data_rows = records_to_matrix(records, columns)
    if target_cols is None:
        target_cols, _ = resolve_target_columns(
            mappings, source_schema or {}, preserve_case=True
        )
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
    return (
        checksum_rows(
            mapped_rows,
            target_cols,
            dest_db_type=dest_db_type,
            dest_types=dest_types,
        ),
        SOURCE_DIGEST_REMAPPED_ROWS,
    )


def _mapped_targets(mappings: list[dict], columns: list[str]) -> list[str]:
    """Return the ordered list of target column names used for reconciliation.

    Declared omissions are excluded: they have no destination carrier, and
    falling back to their source name asked the destination for a column that
    was never created, which failed the read-back and reported Gate-8 as
    unavailable on a write that had actually landed.
    """
    from services.mapping_constraints import write_mappings

    targets = list(dict.fromkeys(
        str(m.get("target") or m.get("source") or "")
        for m in write_mappings(mappings)
        if m.get("target") or m.get("source")
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


def _as_count(value: Any) -> int:
    try:
        return max(int(value), 0)
    except (TypeError, ValueError):
        return 0


def _ladder_declined(report: dict[str, Any], rows: int, budget: int) -> dict[str, Any]:
    """Record why L2/L4/L5 localization did not run, without weakening Gate-8.

    L1 and the L3 full-population checksum are streaming and still apply; only
    the in-memory localization layers are declined.
    """
    out = dict(report or {})
    out["verification_ladder"] = {
        "layers": {},
        "passed": bool(out.get("passed")),
        "assurance_level": str(out.get("assurance_level") or ""),
        "population_proof": False,
        "population_checksum_proof": bool(out.get("checksum_match")),
        "skipped": True,
        "reason": (
            f"Population of {rows} rows exceeds VERIFICATION_LADDER_MAX_ROWS={budget}; "
            "in-memory L2/L4/L5 localization declined before loading. "
            "Gate-8 L1 row balance and L3 full-population checksum still apply."
        ),
        "localization": {},
        "localization_summary": "",
    }
    return out


def _ladder_declined_for_shape(
    report: dict[str, Any], recipe_hash: str
) -> dict[str, Any]:
    """Decline source/destination cell equality for a shaped run, and say so.

    The rows left in the source are the pre-shape values; the destination holds
    the post-shape ones. Comparing them would report the operator's own declared
    change as corruption, so the ladder names the recipe instead of asserting an
    equality shaping made false. Gate-8 row balance and the destination re-read
    are unaffected.
    """
    out = dict(report or {})
    out["verification_ladder"] = {
        "layers": {},
        "passed": bool(out.get("passed")),
        "assurance_level": str(out.get("assurance_level") or ""),
        "population_proof": False,
        "population_checksum_proof": bool(out.get("checksum_match")),
        "skipped": True,
        "shape_recipe_hash": recipe_hash,
        "reason": (
            f"Transform recipe {recipe_hash} rewrote source-side values on the read, "
            "so the source table no longer holds what this run wrote; "
            "source→destination cell equality is declined rather than reported "
            "false. Gate-8 row balance and the destination re-read still apply."
        ),
        "localization": {},
        "localization_summary": "",
    }
    return out


def _maybe_engine_profile_ladder(
    report: dict[str, Any],
    *,
    endpoint: EndpointConfig,
    source_endpoint: EndpointConfig | None,
    records: list[dict],
    columns: list[str],
    dest_summary: dict[str, Any],
    mappings: list[dict],
    dest_type: str,
) -> dict[str, Any] | None:
    """Engine-side L2 for oversized same-engine SQL routes, at any scale.

    The in-memory ladder refuses above ``VERIFICATION_LADDER_MAX_ROWS`` and only
    L1/L3 survive — so on exactly the large tables where a migration's stakes are
    highest, the column-level checks that catch a silently nulled or truncated
    field stop running. When both ends are the same SQL engine family, the engine
    can still compute per-column NULL/min/max/sum in SQL over any number of rows,
    so that signal is restored rather than declined. Returns ``None`` for small
    populations (the richer in-memory ladder runs) and for any route it cannot
    profile, leaving the existing decline in charge.
    """
    if source_endpoint is None or getattr(source_endpoint, "kind", "") != "database":
        return None
    if dest_summary.get("shape_recipe_hash"):
        # Column aggregates over the unshaped source describe values this run
        # deliberately changed, so a rounded or filtered column would be reported
        # as drift. The shaped decline in the caller states the reason instead.
        return None
    from services.procedure_source import is_callable_source

    if is_callable_source(source_endpoint):
        # CALL/SELECT is not a physical table — profiling get_orders would
        # read a colliding relation and publish false Gate-8 proof.
        return None
    from services.sync_cursor import is_overwrite_sync

    # Whole-table aggregates are not the batch that an append/upsert wrote.
    # Profiling them would mix historical rows into Gate-8 L1/L2.
    sync = str(dest_summary.get("sync_mode") or "")
    if sync and not is_overwrite_sync(sync):
        return None
    from services.column_profile import engine_profile_ladder, profile_supported
    from services.verification_ladder import (
        MAX_LADDER_ROWS,
        attach_ladder_to_reconcile_report,
    )

    known = max(
        _as_count(report.get("source_rows")),
        _as_count(report.get("target_rows")),
        len(records or []),
    )
    if known <= MAX_LADDER_ROWS:
        return None

    from .connector_capabilities import resolve_driver_type

    source_type = resolve_driver_type(source_endpoint.format or "")
    # Same- or cross-engine, as long as both ends are SQL engines the profiler
    # knows how to render. Cross-engine narrows to value-based statistics itself.
    if not (profile_supported(source_type) and profile_supported(dest_type)):
        return None

    from services.mapping_constraints import is_intentional_omit

    pairs: list[tuple[str, str]] = []
    types: dict[str, str] = {}
    for m in mappings or []:
        if is_intentional_omit(m):
            continue
        src = str(m.get("source") or "").strip()
        tgt = str(m.get("target") or src).strip()
        if not src or not tgt:
            continue
        pairs.append((src, tgt))
        tt = str(m.get("target_type") or m.get("inferredType") or "").strip()
        if tt:
            types[tgt] = tt
    if not pairs:
        pairs = [(c, c) for c in (columns or []) if c]
    if not pairs:
        return None
    physical = dest_summary.get("column_types") or dest_summary.get("target_types")
    if isinstance(physical, dict):
        for k, v in physical.items():
            if v:
                types[str(k)] = str(v)

    src_cfg = resolve_connector_config(source_endpoint)
    dst_cfg = resolve_connector_config(endpoint)
    try:
        ladder = engine_profile_ladder(
            source_engine=source_type,
            source_cfg=src_cfg,
            source_schema=str(source_endpoint.schema or src_cfg.get("schema") or ""),
            source_table=str(
                source_endpoint.table
                or source_endpoint.collection
                or src_cfg.get("table")
                or ""
            ),
            dest_engine=dest_type,
            dest_cfg=dst_cfg,
            dest_schema=str(dst_cfg.get("schema") or dest_summary.get("schema") or ""),
            dest_table=str(
                endpoint.table or dest_summary.get("table") or dst_cfg.get("table") or ""
            ),
            pairs=pairs,
            types=types,
            source_rows=_as_count(report.get("source_rows")),
            target_rows=_as_count(report.get("target_rows")),
            rejected_rows=_as_count(report.get("rejected_rows")),
            coerced_null_rows=_as_count(report.get("coerced_null_rows")),
            rows_skipped=_as_count(report.get("rows_skipped")),
        )
    except Exception as exc:  # noqa: BLE001 — any failure declines to the existing skip
        logging.getLogger(__name__).info("engine profile ladder skipped: %s", exc)
        return None
    if ladder is None:
        return None
    return attach_ladder_to_reconcile_report(report, ladder)


def _maybe_attach_verification_ladder(
    report: dict[str, Any],
    *,
    endpoint: EndpointConfig,
    source_endpoint: EndpointConfig | None,
    records: list[dict],
    columns: list[str],
    dest_summary: dict[str, Any],
    mappings: list[dict],
    validation_mode: str,
) -> dict[str, Any]:
    """Property 5 — run L1–L5 when source+dest populations fit the row budget."""
    if is_no_op_report(report):
        # Nothing was read past the watermark, so there is no batch for the
        # ladder to verify: it would compare a zero-row write against a sink
        # that legitimately holds earlier rows, fail L1 conservation, and veto a
        # correct quiet poll. The destination count not moving is the proof.
        return report
    from services.verification_ladder import (
        MAX_LADDER_ROWS,
        PopulationTooLarge,
        attach_ladder_to_reconcile_report,
        read_postgres_rows,
        read_sqlite_rows,
        run_five_layer_verification,
    )

    from .connector_capabilities import resolve_driver_type

    dest_type = resolve_driver_type(endpoint.format)

    # Oversized same-engine SQL routes: the in-memory ladder below declines, but
    # the engine can still profile every column at full scale. Purely additive —
    # only runs when the population exceeds the in-memory budget.
    engine_profile = _maybe_engine_profile_ladder(
        report,
        endpoint=endpoint,
        source_endpoint=source_endpoint,
        records=records,
        columns=columns,
        dest_summary=dest_summary,
        mappings=mappings,
        dest_type=dest_type,
    )
    if engine_profile is not None:
        return engine_profile

    known = max(
        _as_count(report.get("source_rows")),
        _as_count(report.get("target_rows")),
        len(records or []),
    )
    if known > MAX_LADDER_ROWS:
        # Every oversized dest, not just PG/SQLite: a MySQL/MariaDB route that
        # could not profile must still name the decline instead of returning a
        # bare Gate-8 report with no ladder at all.
        return _ladder_declined(report, known, MAX_LADDER_ROWS)

    if dest_type not in {"sqlite", "postgresql", "redshift"}:
        return report
    target_cols = _mapped_targets(mappings, columns) if mappings else list(columns or [])
    if not target_cols:
        return report

    pk_cols: list[str] = []
    for key in ("primary_key_columns", "conflict_columns"):
        raw = dest_summary.get(key) or []
        if isinstance(raw, (list, tuple)):
            pk_cols = [str(x) for x in raw if x]
            if pk_cols:
                break
    pk_column = pk_cols[0] if pk_cols else ""
    # Identity maps often use `id` — accept when present on both sides.
    if not pk_column and "id" in {c.lower() for c in target_cols}:
        pk_column = next(c for c in target_cols if c.lower() == "id")

    dest_cfg = resolve_connector_config(endpoint)
    source_rows: list[dict] = [r for r in (records or []) if isinstance(r, dict)]
    target_rows: list[dict] = []

    # Prefer full SQL population over buffered records (streaming passes []).
    try:
        if dest_type == "sqlite":
            target_rows = read_sqlite_rows(
                database=str(dest_cfg.get("database") or ""),
                table=str(endpoint.table or dest_cfg.get("table") or ""),
                columns=target_cols,
                connection_string=str(dest_cfg.get("connection_string") or ""),
                host=str(dest_cfg.get("host") or ""),
            )
        else:
            target_rows = read_postgres_rows(
                host=str(dest_cfg.get("host") or ""),
                port=int(dest_cfg.get("port") or 5432),
                database=str(dest_cfg.get("database") or ""),
                username=str(dest_cfg.get("username") or ""),
                password=str(dest_cfg.get("password") or ""),
                schema=str(dest_cfg.get("schema") or "public"),
                table=str(endpoint.table or dest_cfg.get("table") or ""),
                columns=target_cols,
                connection_string=str(dest_cfg.get("connection_string") or ""),
                ssl=bool(dest_cfg.get("ssl", False)),
            )
    except PopulationTooLarge as exc:
        return _ladder_declined(report, exc.rows_read, exc.budget)
    except Exception as exc:
        logging.getLogger(__name__).debug("ladder dest load failed: %s", exc)
        return report

    # A shaping recipe rewrote source-side values on the read, so the rows still
    # in the source table are no longer what this run wrote. Re-reading them here
    # would compare pre-shape values against post-shape values and report every
    # shaped cell as a mismatch, so the cell ladder declines and says why — the
    # proof for a shaped run is the pinned recipe hash plus the destination
    # re-read, not a source/destination cell equality that shaping made false.
    shaped_run = bool(dest_summary.get("shape_recipe_hash"))
    if source_endpoint is not None and not source_rows and shaped_run:
        return _ladder_declined_for_shape(report, str(dest_summary["shape_recipe_hash"]))
    if source_endpoint is not None and not source_rows:
        from services.procedure_source import is_callable_source

        if not is_callable_source(source_endpoint):
            src_type = resolve_driver_type(source_endpoint.format)
            src_cfg = resolve_connector_config(source_endpoint)
            try:
                if src_type == "sqlite":
                    source_rows = read_sqlite_rows(
                        database=str(src_cfg.get("database") or ""),
                        table=str(source_endpoint.table or src_cfg.get("table") or ""),
                        columns=target_cols,
                        connection_string=str(src_cfg.get("connection_string") or ""),
                        host=str(src_cfg.get("host") or ""),
                    )
                elif src_type in {"postgresql", "redshift"}:
                    source_rows = read_postgres_rows(
                        host=str(src_cfg.get("host") or ""),
                        port=int(src_cfg.get("port") or 5432),
                        database=str(src_cfg.get("database") or ""),
                        username=str(src_cfg.get("username") or ""),
                        password=str(src_cfg.get("password") or ""),
                        schema=str(src_cfg.get("schema") or "public"),
                        table=str(source_endpoint.table or src_cfg.get("table") or ""),
                        columns=target_cols,
                        connection_string=str(src_cfg.get("connection_string") or ""),
                        ssl=bool(src_cfg.get("ssl", False)),
                    )
            except PopulationTooLarge as exc:
                return _ladder_declined(report, exc.rows_read, exc.budget)
            except Exception as exc:
                logging.getLogger(__name__).debug("ladder source load failed: %s", exc)

    if not source_rows or not target_rows:
        return report

    dest_types = {
        str(m.get("target") or ""): str(m.get("target_type") or m.get("inferredType") or "")
        for m in mappings
        if m.get("target") and (m.get("target_type") or m.get("inferredType"))
    }
    from services.sync_cursor import is_overwrite_sync

    sync_mode = str(
        dest_summary.get("sync_mode") or dest_summary.get("effective_sync_mode") or ""
    )
    allow_extra = (
        not is_overwrite_sync(sync_mode)
        and sync_mode.lower() not in {"full_refresh_mirror", "mirror", "scd2"}
    )
    raw_before = dest_summary.get(PRECOUNT_KEY)
    dest_before = int(raw_before) if isinstance(raw_before, int) else None
    from services.row_conservation import (
        KIND_KEYED,
        KeyCensus,
        conservation_kind,
    )

    kind = conservation_kind(sync_mode, dest_count_before=dest_before)
    census = KeyCensus.from_mapping(dest_summary.get(CENSUS_KEY))
    keyed = kind == KIND_KEYED
    ladder = run_five_layer_verification(
        source_rows=source_rows,
        target_rows=target_rows,
        columns=target_cols,
        pk_column=pk_column,
        source_row_count=int(report.get("source_rows") or len(source_rows)),
        target_row_count=int(report.get("target_rows") or len(target_rows)),
        rejected_rows=int(report.get("rejected_rows") or 0),
        coerced_null_rows=int(report.get("coerced_null_rows") or 0),
        rows_skipped=int(report.get("rows_skipped") or 0),
        source_checksum=str(report.get("source_checksum") or ""),
        target_checksum=str(report.get("target_checksum") or ""),
        dest_db_type=dest_type,
        dest_types=dest_types,
        allow_extra_rows=allow_extra,
        checksum_scope=str(report.get("checksum_scope") or ""),
        target_rows_before=dest_before,
        keyed_cardinality=keyed,
        keyed_expected_delta=census.expected_delta if keyed and census else None,
        # maximum: always run L4/L5. strict/balanced: localize only on L3 fail.
        always_localize=str(validation_mode or "").lower() == "maximum",
    )
    return attach_ladder_to_reconcile_report(report, ladder)


def _identity_watermark_evidence(
    *,
    db_type: str,
    cfg: dict[str, Any],
    schema: str,
    table: str,
    pk_cols: list[str],
) -> dict[str, Any]:
    """Generator state for the destination keys after the write.

    Row checksums cannot see this: a migration that carries explicit key values
    leaves Postgres and Oracle generators at their pre-migration value, so the
    first application insert after cutover collides on the primary key. Repair
    is forward-only and opt-out via ``identity_watermark_repair``.
    """
    from services.identity_watermark import (
        identity_watermark_supported,
        verify_identity_watermark,
    )

    if not (table and pk_cols):
        return {}
    if not identity_watermark_supported(db_type):
        return {
            "supported": False,
            "verified": False,
            "reason": f"generator state is not readable on '{db_type}'",
        }
    repair = cfg.get("identity_watermark_repair")
    evidence = verify_identity_watermark(
        db_type,
        cfg,
        schema=schema or "",
        table=str(table),
        columns=pk_cols,
        repair=True if repair is None else bool(repair),
    )
    evidence["supported"] = True
    return evidence


def _schema_state_evidence(
    *,
    source_endpoint: EndpointConfig | None,
    db_type: str,
    cfg: dict[str, Any],
    schema: str,
    table: str,
) -> dict[str, Any]:
    """Constraints, indexes, nullability and defaults, compared catalog to catalog.

    A run can move every row and still leave the client a table with no primary
    key, no unique constraint and no index. Both sides are read here on this
    module's own connections, never from writer bookkeeping.
    """
    from .connector_capabilities import resolve_driver_type

    if source_endpoint is None or source_endpoint.kind != "database" or not table:
        return {}
    src_cfg = resolve_connector_config(source_endpoint)
    src_type = resolve_driver_type(
        str(src_cfg.get("type") or source_endpoint.format or "")
    ).lower()
    src_table = str(source_endpoint.table or src_cfg.get("table") or "")
    if not src_table:
        return {}
    from services.procedure_source import is_callable_source

    if is_callable_source(source_endpoint) or is_callable_source(src_cfg):
        return {
            "skipped": True,
            "reason": "callable_source",
            "note": (
                "Physical catalog compare is not run against a CALL/SELECT stream name."
            ),
        }

    from services.dialect_profiles import schema_from_cfg
    from services.physical_state_diff import verify_physical_state

    return verify_physical_state(
        source_db_type=src_type,
        source_cfg=src_cfg,
        source_schema=str(schema_from_cfg(src_type, src_cfg) or ""),
        source_table=src_table,
        dest_db_type=db_type,
        dest_cfg=cfg,
        dest_schema=schema,
        dest_table=table,
    )


def _source_foreign_keys(schema_state: dict[str, Any]) -> list[dict[str, Any]]:
    """Relationships the source guaranteed, from its own catalog read."""
    rendered = ((schema_state.get("source") or {}).get("foreign_keys")) or []
    keys: list[dict[str, Any]] = []
    for item in rendered:
        parts = str(item).split("->")
        if len(parts) != 3:
            continue
        child, parent, parent_cols = parts
        keys.append(
            {
                "constrained_columns": [c for c in child.split("+") if c],
                "referred_table": parent,
                "referred_columns": [c for c in parent_cols.split("+") if c],
            }
        )
    return keys


def _referential_integrity_evidence(
    *,
    db_type: str,
    cfg: dict[str, Any],
    schema: str,
    table: str,
    schema_state: dict[str, Any],
) -> dict[str, Any]:
    """Orphan proof for every source relationship the destination does not enforce."""
    foreign_keys = _source_foreign_keys(schema_state)
    if not foreign_keys:
        return {}

    from services.destination_ri_probe import verify_destination_referential_integrity

    return verify_destination_referential_integrity(
        db_type,
        cfg,
        schema=schema,
        table=table,
        foreign_keys=foreign_keys,
    )


def _writer_supplied_engine_digests(
    dest_summary: dict[str, Any] | None,
) -> tuple[str, str, int] | None:
    """Both digests from a writer that computed them in the engine.

    Returns ``(source_checksum, target_checksum, target_rows)``. Only the pair is
    usable: a run that produced one of them and left the other to be recomputed
    here would be comparing a numeric engine digest against a Python hex digest.
    """
    summary = dest_summary or {}
    source = str(summary.get("engine_source_checksum") or "").strip()
    target = str(summary.get("engine_target_checksum") or "").strip()
    if not source or not target:
        return None
    rows = summary.get("rows_written")
    return source, target, int(rows or 0)


def _localize_checksum_mismatch(
    report: dict[str, Any],
    dest_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    """Name what the two hashes disagree about when no cell was found to differ.

    A strict mismatch that survives a clean key-aligned sample on a conserved
    population is not a corrupted cell — it is the two sides having been hashed
    on different bases or over different populations. Reporting only the pair of
    hashes left the operator with nothing to act on, so the classification and
    the facts behind it are stamped for the run panel:

    ``digest_basis`` — how each side was obtained (write-pass, independent
    re-read, writer ack) and, when known, the columns whose granularity the
    destination carrier decides.

    The verdict itself does not move: a mismatch still fails Gate-8.
    """
    if report.get("checksum_match") is not False:
        return report
    summary = dest_summary or {}
    sample = report.get("sample_compare") or {}
    compared = int((sample or {}).get("compared") or 0)
    sample_clean = bool(sample) and bool(sample.get("passed")) and compared > 0
    if not sample_clean:
        return report
    rows = int(sample.get("rows_compared") or 0)
    basis = {
        "source_digest": str(
            report.get("source_checksum_provenance")
            or summary.get("checksum_mode")
            or ""
        ),
        "source_scope": str(report.get("checksum_scope") or ""),
        "carrier_rounded_columns": list(report.get("carrier_rounded_columns") or []),
        "keyed_sample_rows_without_mismatch": rows or compared,
        "keyed_sample_cells_without_mismatch": compared,
    }
    scope = f"{rows:,} row(s) / {compared:,} cell(s)" if rows else f"{compared:,} cell(s)"
    report["mismatch_class"] = "comparison_basis_or_population_scope"
    report["digest_basis"] = basis
    report["message"] = (
        f"{str(report.get('message') or '').rstrip()} No differing cell was found "
        f"in {scope} of key-aligned data, so the two digests differ in how or "
        f"over what they were taken, not in a sampled value — source digest "
        f"{basis['source_digest'] or 'unknown'}"
        + (f", scope {basis['source_scope']}" if basis["source_scope"] else "")
        + ". Gate-8 still fails: an unexplained digest difference is not proof."
    )
    return report


def _attach_match_summary(
    report: dict[str, Any],
    dest_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    """Say what was compared, how much of it agreed, and what to do about it.

    A reconcile verdict of two hex strings is not proof an operator can use: it
    names no population, no percentage anyone can reproduce, and no next move.
    This stamps the denominators the run actually holds — source population,
    destination population, the rows this pass moved, and the keyed sample's row
    and cell counts — so a match percentage always says what it is a percentage
    *of*, plus an ordered remediation list whose entries map to real controls.

    Nothing here softens a verdict: a failed report stays failed, and a
    percentage taken from a sample is labelled sample evidence, never population
    proof.
    """
    summary = dest_summary or {}
    sample = report.get("sample_compare") or {}
    rows = int(sample.get("rows_compared") or 0)
    cells = int(sample.get("cells_compared") or sample.get("compared") or 0)
    differing = len(list(sample.get("mismatches") or []))
    match: dict[str, Any] = {
        "source_rows": report.get("source_rows"),
        "dest_rows": report.get("target_rows"),
        "dest_rows_before": summary.get(PRECOUNT_KEY),
        "rows_moved_this_run": summary.get("rows_written"),
        "rejected_rows": report.get("rejected_rows"),
        "sample_rows_compared": rows,
        "sample_cells_compared": cells,
        "sample_cells_differing": differing,
        "scope": str(report.get("checksum_scope") or ""),
    }
    if cells:
        agreeing = max(0, cells - differing)
        match["sample_match_percent"] = round(100.0 * agreeing / cells, 4)
        match["denominator"] = (
            f"{cells} cell(s) across {rows} key-aligned row(s) of the read-back "
            "sample — sample evidence, not population proof"
        )
    else:
        match["sample_match_percent"] = None
        match["denominator"] = "no key-aligned cells were comparable in this run"
    report["match_summary"] = match

    if report.get("passed"):
        return report
    actions: list[dict[str, str]] = []
    mismatches = [m for m in (sample.get("mismatches") or []) if isinstance(m, dict)]
    if mismatches:
        first = mismatches[0]
        actions.append({
            "action": "open_map",
            "label": f"Fix the mapping for {first.get('source')} → {first.get('target')}",
            "why": (
                f"{len(mismatches)} sampled cell(s) differ, e.g. source "
                f"{first.get('source_value')!r} vs destination "
                f"{first.get('target_value')!r} — a value that changed on write is "
                "a transform or type decision, not a digest artefact."
            ),
        })
    if report.get("mismatch_class") == "comparison_basis_or_population_scope":
        actions.append({
            "action": "overwrite_or_keyed_resync",
            "label": "Re-run this table with overwrite, or upsert on its key",
            "why": (
                "No sampled cell differs, so the digests disagree on basis or "
                "population, not on data. A full refresh or a keyed merge gives "
                "both sides one comparable population."
            ),
        })
    if int(report.get("rejected_rows") or 0) > 0:
        actions.append({
            "action": "replay_quarantine",
            "label": f"Replay {int(report['rejected_rows']):,} quarantined row(s)",
            "why": "Those rows are absent from the destination until they are replayed.",
        })
    if summary.get("resumed_from") is not None:
        actions.append({
            "action": "resume",
            "label": "Resume from the checkpoint",
            "why": "The pass stopped part-way; resuming moves only the tail.",
        })
    if actions:
        report["remediation"] = actions
    return report


def _engine_digest_enabled() -> bool:
    """Operator gate — default **off** until two gaps are closed.

    The mechanism is proven on its own (see ``test_engine_checksum.py``, and it
    is what caught the truncated-microseconds bug), but reading both populations
    back is not yet safe as the default here:

    * **Snapshot.** A PostgreSQL full refresh reads under REPEATABLE READ, so
      rows inserted while it runs are correctly outside the transfer. Digesting
      the source afterwards on a fresh connection sees them and reports a
      mismatch on a transfer that was right. The digest has to run inside the
      reader's snapshot, not after it.
    * **Scope.** Whole-table digests are meaningless for an append or upsert
      into a destination that already held rows; that case re-scopes the target
      to the written keys further down, and a whole-population source digest
      cannot be compared against it.

    Until both are handled, this stays behind ``DATAFLOW_ENGINE_DIGEST=1`` so it
    can be measured and exercised without deciding any operator's Gate-8 verdict.
    """
    from services.brand_env import getenv_brand

    return (getenv_brand("ENGINE_DIGEST", "0") or "0").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _engine_population_digests(
    *,
    source_endpoint: EndpointConfig | None,
    dest_cfg: dict[str, Any],
    dest_db_type: str,
    dest_schema: str,
    dest_table: str,
    mappings: list[dict],
    source_schema: dict[str, str] | None,
    dest_types: dict[str, str] | None,
) -> tuple[str, str, int] | None:
    """Digest both populations in their own engines, or ``None`` to fall back.

    Gate-8 compares the two sides to each other, so when they render values
    identically the comparison does not need Python to see a single row. That
    is worth reaching for: per-cell fingerprinting is the largest cost in a
    transfer by a wide margin.

    Returns ``None`` — leaving today's path untouched — whenever anything is not
    provably comparable: a different engine, a transform, a declared omission, a
    type that is only nearly the same, or any error while querying. Falling back
    costs time; guessing costs correctness.
    """
    if source_endpoint is None or source_endpoint.kind != "database":
        return None
    from services.engine_checksum import (
        comparable_column_pairs,
        engines_comparable,
        postgresql_engine_checksum,
    )

    from .connector_capabilities import resolve_driver_type

    source_driver = resolve_driver_type(source_endpoint.format or "")
    if not engines_comparable(source_driver, dest_db_type):
        return None
    pairs = comparable_column_pairs(mappings, source_schema, dest_types)
    if not pairs:
        return None

    from services.procedure_source import is_callable_source

    if is_callable_source(source_endpoint):
        # CALL/SELECT extract is not a physical table — engine checksum would
        # probe a fake relation named after the procedure. Row accounting stays
        # on dest COUNT / committed_offset.
        return None

    source_table = source_endpoint.table or source_endpoint.collection or ""
    if not source_table or not dest_table:
        return None

    from connectors.postgresql_conn import get_connection
    from connectors.sql_identifiers import quote_table_ref

    src_cfg = resolve_connector_config(source_endpoint)
    try:
        source_ref = quote_table_ref(
            source_table, source_endpoint.schema or "public", dialect="postgresql"
        )
        dest_ref = quote_table_ref(
            dest_table, dest_schema or "public", dialect="postgresql"
        )
    except Exception:
        return None

    def _digest(cfg: dict[str, Any], table_ref: str, columns: list[str]):
        conn = get_connection(
            host=cfg.get("host", ""),
            port=int(cfg.get("port") or 5432),
            database=cfg.get("database", ""),
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            connection_string=cfg.get("connection_string", ""),
            ssl=bool(cfg.get("ssl", False)),
        )
        try:
            with conn.cursor() as cur:
                return postgresql_engine_checksum(cur, table_ref, columns)
        finally:
            conn.close()

    try:
        source_digest = _digest(src_cfg, source_ref, [p[0] for p in pairs])
        dest_digest = _digest(dest_cfg, dest_ref, [p[1] for p in pairs])
    except Exception as exc:
        logging.getLogger(__name__).info(
            "Engine population digest unavailable, using row fingerprints: %s", exc
        )
        return None
    if source_digest is None or dest_digest is None:
        return None
    return source_digest.checksum, dest_digest.checksum, dest_digest.row_count


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
    source_endpoint: EndpointConfig | None = None,
    source_spool: Any = None,
) -> dict[str, Any]:
    """Verify row counts and checksums against the destination."""
    # Destination facts a row checksum cannot prove (generator watermarks today).
    physical_state: dict[str, Any] = {}
    # Where the source digest came from. Held in a box because the early-return
    # paths below run before it is known, and the report must never claim two
    # independent digests agreed when only the writer's was available.
    digest_provenance: dict[str, str] = {"source": "", "source_scope": ""}
    # Late-bound: populated once driver/schema/table are resolved. _finalize
    # is defined before those names exist; file-export returns must not stamp.
    vector_stamp_ctx: dict[str, Any] = {}
    keyset_stamp_ctx: dict[str, Any] = {}
    scd2_stamp_ctx: dict[str, Any] = {}
    # Late-bound with the resolved destination engine and physical types: which
    # columns Gate-8 could only prove at the carrier's granularity.
    carrier_ctx: dict[str, Any] = {}

    def _finalize(payload: dict[str, Any]) -> dict[str, Any]:
        if digest_provenance["source"] and "source_checksum_provenance" not in payload:
            payload = {
                **payload,
                "source_checksum_provenance": digest_provenance["source"],
            }
        if digest_provenance["source_scope"] and "source_checksum_scope" not in payload:
            payload = {
                **payload,
                "source_checksum_scope": digest_provenance["source_scope"],
            }
        # Property 3 — carry source snapshot id onto the reconcile report /
        # migration certificate surface.
        stamped = _finalize_reconcile(payload, dest_summary=dest_summary)
        msg = str(stamped.get("message") or "")
        assurance = str(stamped.get("assurance_level") or "")
        # Writer-ack / empty dest digest must never keep the reconcile() theatre
        # line "Row fidelity verified — source and target checksums match".
        if msg.lower().startswith("row fidelity verified") and assurance not in {
            "full_checksum",
        }:
            rows = stamped.get("target_rows") or stamped.get("source_rows") or 0
            dest_hash = str(stamped.get("target_checksum") or "").strip()
            if assurance == "write_pass_dest_readback" or dest_hash:
                stamped["message"] = (
                    f"Destination read-back matches the write-pass fingerprint "
                    f"({rows} row(s)). Source warehouse was not independently "
                    "re-read — not migration_proven."
                )
            else:
                stamped["message"] = (
                    f"Writer acknowledgment for {rows} row(s) — source digest was "
                    "the write-pass hash, not an independent dest read-back. "
                    "Not migration_proven."
                )
        if vector_stamp_ctx:
            stamped = stamp_vector_census(
                stamped,
                vector_stamp_ctx.get("cfg"),
                schema=str(vector_stamp_ctx.get("schema") or ""),
                table_name=str(vector_stamp_ctx.get("table_name") or ""),
                dest_engine=str(vector_stamp_ctx.get("engine") or ""),
            )
        if keyset_stamp_ctx:
            stamped = stamp_keyset_census(
                stamped,
                keyset_stamp_ctx.get("cfg"),
                schema=str(keyset_stamp_ctx.get("schema") or ""),
                table_name=str(keyset_stamp_ctx.get("table_name") or ""),
                dest_engine=str(keyset_stamp_ctx.get("engine") or ""),
                key_columns=list(keyset_stamp_ctx.get("key_columns") or []),
                keys=keyset_stamp_ctx.get("keys"),
            )
        leftover_n = dest_summary.get("leftover_deleted") if isinstance(dest_summary, dict) else None
        if isinstance(leftover_n, int) and leftover_n >= 0:
            stamped["leftover_deleted"] = leftover_n
        if scd2_stamp_ctx:
            stamped = stamp_scd2_census(
                stamped,
                scd2_stamp_ctx.get("cfg"),
                schema=str(scd2_stamp_ctx.get("schema") or ""),
                table_name=str(scd2_stamp_ctx.get("table_name") or ""),
                dest_engine=str(scd2_stamp_ctx.get("engine") or ""),
            )
        # Property 5 — attach L1–L5 ladder when both populations are available.
        try:
            stamped = _maybe_attach_verification_ladder(
                stamped,
                endpoint=endpoint,
                source_endpoint=source_endpoint,
                records=records,
                columns=columns,
                dest_summary=dest_summary,
                mappings=mappings or [],
                validation_mode=validation_mode,
            )
        except Exception as exc:
            logging.getLogger(__name__).warning(
                "verification ladder skipped: %s", exc, exc_info=exc
            )
        if physical_state:
            stamped["physical_state"] = dict(physical_state)
        rounded = list(carrier_ctx.get("rounded_columns") or [])
        if rounded:
            # Both digests were taken at the destination carrier's granularity,
            # so a match here proves every cell the destination *can* hold. The
            # fractional seconds it cannot hold are a declared narrowing, and
            # saying so is the difference between honest proof and a green that
            # over-claims.
            stamped["carrier_rounded_columns"] = rounded
            named = ", ".join(
                f"{c.get('column')} {c.get('source_type')} → {c.get('target_type')}"
                for c in rounded[:3]
            )
            more = f" (+{len(rounded) - 3} more)" if len(rounded) > 3 else ""
            note = (
                f" Instants on {len(rounded)} column(s) were compared at the "
                f"destination carrier's granularity — {named}{more} — because "
                "that column cannot store the source's fractional seconds; "
                "those dropped digits are a declared narrowing, not proven "
                "cell fidelity."
            )
            stamped["message"] = f"{str(stamped.get('message') or '').rstrip()}{note}"
        stamped = _localize_checksum_mismatch(stamped, dest_summary)
        stamped = _attach_match_summary(stamped, dest_summary)
        return stamped

    rejected_rows = int(dest_summary.get("rejected_rows", 0) or 0)
    coerced_null_rows = int(dest_summary.get("coerced_null_rows", 0) or 0)
    rows_skipped = int(dest_summary.get("rows_skipped", 0) or 0)
    # Rows an approved shaping recipe removed on the read. They were read — so
    # they belong to the source population this run counted — and they are
    # deliberately not at the destination, so conservation has to name them
    # instead of reading their absence as short delivery.
    rows_shaped_out = int(dest_summary.get("rows_shaped_out", 0) or 0)
    rows_expanded = int(dest_summary.get("rows_expanded", 0) or 0)
    # Rows a declared source filter removed on the read. The source population
    # this run counted includes them (the source paged and counted them), so the
    # filter has to be stated here for the same reason a recipe does.
    rows_source_filtered = int(dest_summary.get("rows_source_filtered", 0) or 0)
    # The streaming reader counts every row it removed on the read — a declared
    # filter's and a recipe's — for committed pages only, and cumulatively across
    # resumes. When it reports that, it is the authoritative removal total: the
    # recipe's own tally covers this pass alone.
    rows_removed_on_read = int(dest_summary.get("rows_removed_on_read", 0) or 0)
    if (
        "rows_source_filtered" not in dest_summary
        and rows_removed_on_read > rows_shaped_out + rows_source_filtered
    ):
        rows_source_filtered = rows_removed_on_read - rows_shaped_out
    # A resumed pass reads and writes only the tail of the population, while the
    # destination read-back is always full-table. Comparing the two directly
    # reports a mismatch on data that is correct, so the resumed slice must be
    # widened back to the whole population before anything is compared.
    resumed_from = _as_count(dest_summary.get("resumed_from"))
    resume_full_source_rows = _as_count(dest_summary.get("resume_full_source_rows"))
    # Coerced rows are KEPT (a cell became NULL); quarantine hold-outs are absent
    # from the destination. Skipped rows (e.g. stale CDC LSN) are not written.
    dropped_rows = max(rejected_rows - coerced_null_rows, 0)
    # Prefer independent source accounting when the caller provides it
    # (streaming paths should pass source_row_count from the read side).
    source_row_count = dest_summary.get("source_row_count")
    if isinstance(source_row_count, int) and source_row_count >= 0:
        source_rows = source_row_count
    elif records:
        source_rows = len(records)
    elif endpoint.kind != "database":
        # File/object exports have no cell-fidelity Gate-8; writer ack is enough
        # for an *unproven* operational pass below. Do not invent conservation.
        source_rows = int(rows_written or 0) + dropped_rows + rows_skipped
    else:
        # Never invent source_rows from writer ack on database destinations —
        # that circularly balances short reads. Fail closed until the reader
        # stamps a population count.
        return _finalize({
            "passed": False,
            "unproven": True,
            "migration_proven": False,
            "message": (
                "Source row count unmeasured — Gate-8 refuses conservation "
                "invented from writer acknowledgements alone."
            ),
            "source_rows": None,
            "target_rows": rows_written,
            "rejected_rows": rejected_rows,
            "coerced_null_rows": coerced_null_rows,
            "rows_skipped": rows_skipped,
            "details": {
                "reason": "source_row_count_unmeasured",
                "source_row_count_source": dest_summary.get("source_row_count_source"),
            },
        })
    if resume_full_source_rows:
        source_rows = resume_full_source_rows
    elif resumed_from:
        source_rows += resumed_from
    expected_written = max(
        source_rows
        + rows_expanded
        - dropped_rows
        - rows_skipped
        - rows_shaped_out
        - rows_source_filtered,
        0,
    )
    # Destinations with no read-back are accounted from writer counts, so rows a
    # previous pass already committed have to be added back or a correct resume
    # reads as short delivery.
    rows_written_accounted = rows_written + resumed_from

    if endpoint.kind != "database":
        # Object/file exports have no destination cell read-back. Writer checksum
        # proves bytes landed, not per-cell fidelity. Operational write may pass;
        # never stamp migration_proven / cell-fidelity Gate-8 green.
        # Independent artifact COUNT (re-open the file) is dest cardinality —
        # writer ``rows_written`` never closes conservation.
        checksum = str(writer_checksum or dest_summary.get("checksum") or "").strip()
        payload = stamp_artifact_census(
            {
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
                "rejected_rows": rejected_rows,
                "coerced_null_rows": coerced_null_rows,
                "rows_skipped": rows_skipped,
                "checksum": checksum,
            },
            dest_summary,
            fmt=endpoint.format,
        )
        counted = payload.get("artifact_row_count")
        if isinstance(counted, int) and counted >= 0:
            payload["message"] = (
                "File/object export wrote successfully — Gate-8 cell fidelity "
                "unproven (no destination cell read-back). Independent artifact "
                f"record count is {counted:,}. Writer acknowledgement is diagnostic."
            )
        return _finalize(payload)

    from .connector_capabilities import resolve_driver_type

    cfg = resolve_connector_config(endpoint)
    # Prefer canonical driver (amazon_s3→s3) while preserving catalog type on cfg.
    db_type = resolve_driver_type(
        str(cfg.get("type") or endpoint.format or "")
    ).lower()
    from services.dialect_profiles import schema_from_cfg

    schema = dest_summary.get("schema") or schema_from_cfg(db_type, cfg)
    table_name = dest_summary.get("table") or endpoint.table or endpoint.collection or ""
    if db_type in VECTOR_IDENTITY_ENGINES:
        vector_stamp_ctx.update(
            cfg=cfg,
            schema=schema,
            table_name=table_name,
            engine=db_type,
        )
    from services.sync_cursor import normalize_sync_mode

    if normalize_sync_mode(
        str(
            dest_summary.get("sync_mode")
            or dest_summary.get("effective_sync_mode")
            or ""
        ),
        default="",
    ) == "scd2":
        scd2_stamp_ctx.update(
            cfg=cfg,
            schema=schema,
            table_name=table_name,
            engine=db_type,
        )

    mapping_dicts = mappings or [{"source": col, "target": col} for col in columns]
    dest_types = _dest_types_from_mappings(mapping_dicts)
    # Prefer physical types stamped by the writer when present.
    physical = dest_summary.get("column_types") or dest_summary.get("target_types")
    if isinstance(physical, dict):
        for k, v in physical.items():
            if v:
                dest_types[str(k)] = str(v)
    # The plan's target_type is Map's intent; the carrier the rows landed in is
    # what the digests must be taken against. A pre-existing destination column
    # contradicts the plan (declared DATETIME(6), physical datetime) and hashing
    # the plan's precision failed correct loads with two opaque hashes.
    if not physical and table_name and endpoint.kind == "database":
        try:
            from services.dest_physical_types import (
                apply_physical_temporal_precision,
            )

            dest_types = apply_physical_temporal_precision(
                dest_types, db_type, cfg, table=str(table_name)
            )
        except Exception as exc:
            logging.getLogger(__name__).warning(
                "physical destination temporal precision unavailable: %s",
                exc,
                exc_info=exc,
            )
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

    try:
        from services.carrier_instant import carrier_rounded_columns

        carrier_ctx["rounded_columns"] = carrier_rounded_columns(
            mapping_dicts,
            source_schema=source_schema,
            dest_types=dest_types,
            dest_engine=db_type,
        )
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "carrier granularity disclosure skipped: %s", exc, exc_info=exc
        )

    target_cols = _mapped_targets(mapping_dicts, columns)
    pk_cols = list(
        dest_summary.get("primary_key_columns")
        or dest_summary.get("conflict_columns")
        or []
    )
    # Whether the *destination* stands behind this key — a declared PK/unique
    # constraint — as opposed to a key named by the stream contract or inferred
    # by Map. Naming a key is not enforcing it: a contract primary_key on an
    # append-only write leaves a keyless table free to hold the same key twice,
    # so it may align a sample but must never scope a digest. Resolved from the
    # destination catalog at the append branch below, where it is used.
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
    # Complete overwrite snapshot + dest PK: split MISSING_TARGET from
    # EXTRA_TARGET. Incremental CDC must not pass a batch as S (that would
    # invent leftover dest keys and look like inferred deletes).
    from services.sync_cursor import is_overwrite_sync

    key_tuples = None
    if (
        is_overwrite_sync(
            str(
                dest_summary.get("sync_mode")
                or dest_summary.get("effective_sync_mode")
                or ""
            )
        )
        and pk_cols
        and db_type
        not in {"pgvector", "pinecone", "qdrant", "weaviate", "milvus", "email"}
        and isinstance(source_rows, int)
        and not resumed_from
    ):
        if records and source_rows == len(records):
            key_tuples = records_to_key_tuples(
                records, [str(c) for c in pk_cols], mapping_dicts
            )
        else:
            stamped = dest_summary.get(OVERWRITE_SOURCE_KEYS_KEY)
            if isinstance(stamped, list) and len(stamped) == source_rows:
                try:
                    key_tuples = [tuple(t) for t in stamped]
                except TypeError:
                    key_tuples = None
        if key_tuples:
            # Complete overwrite snapshot MERGE: hard-DELETE dest keys not
            # in S *before* dest COUNT / checksum. Incremental CDC never
            # enters this gate. Measure-without-apply is DMS EXTRA_TARGET;
            # apply-without-this-gate is Airbyte issue #6383.
            try:
                from services.row_conservation import apply_inferred_leftover_deletes

                deleted = apply_inferred_leftover_deletes(
                    db_type=db_type,
                    cfg=cfg,
                    schema=str(schema or ""),
                    table_name=str(table_name or ""),
                    key_columns=[str(c) for c in pk_cols],
                    keys=key_tuples,
                    complete_snapshot=True,
                )
                if deleted:
                    dest_summary["leftover_deleted"] = int(deleted)
            except Exception as exc:
                logging.getLogger(__name__).warning(
                    "Overwrite leftover MERGE skipped: %s", exc, exc_info=exc
                )
            keyset_stamp_ctx.update(
                cfg=cfg,
                schema=schema,
                table_name=table_name,
                engine=db_type,
                key_columns=[str(c) for c in pk_cols],
                keys=key_tuples,
            )
    try:
        identity_state = _identity_watermark_evidence(
            db_type=db_type,
            cfg=cfg,
            schema=str(schema or ""),
            table=str(table_name or ""),
            pk_cols=[str(c) for c in pk_cols if c],
        )
        if identity_state:
            physical_state["identity_watermark"] = identity_state
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "identity watermark verification skipped: %s", exc, exc_info=exc
        )
        physical_state["identity_watermark"] = {
            "supported": True,
            "verified": False,
            "reason": f"probe failed: {exc}",
        }

    try:
        schema_state = _schema_state_evidence(
            source_endpoint=source_endpoint,
            db_type=db_type,
            cfg=cfg,
            schema=str(schema or ""),
            table=str(table_name or ""),
        )
        if schema_state:
            physical_state["schema_objects"] = schema_state
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "physical schema comparison skipped: %s", exc, exc_info=exc
        )
        schema_state = {}
        physical_state["schema_objects"] = {
            "verified": False,
            "reason": f"comparison failed: {exc}",
        }

    try:
        ri_state = _referential_integrity_evidence(
            db_type=db_type,
            cfg=cfg,
            schema=str(schema or ""),
            table=str(table_name or ""),
            schema_state=schema_state,
        )
        if ri_state:
            physical_state["referential_integrity"] = ri_state
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "destination referential integrity probe skipped: %s", exc, exc_info=exc
        )
        physical_state["referential_integrity"] = {
            "verified": False,
            "reason": f"probe failed: {exc}",
        }

    # The writer digest of a resumed pass covers the tail it wrote, not the
    # population. Recompute from the full source when the caller re-supplied it;
    # otherwise leave it empty and decline the comparison further down rather
    # than compare two different scopes.
    source_checksum_scope_note = ""
    if resumed_from and not (
        resume_full_source_rows and (records or source_spool is not None)
    ):
        source_checksum_scope_note = (
            f"Resumed after {resumed_from:,} previously committed row(s): this pass "
            "read only the remaining slice, so no source digest covering the whole "
            "population is available to compare against the full-table destination "
            "digest."
        )

    source_checksum_provenance = ""
    if source_checksum_scope_note:
        source_checksum = ""
    elif str(dest_summary.get("checksum_mode") or "") == "inline_write_pass" and writer_checksum:
        # Phase F1 fingerprints are remapped source rows hashed during the write,
        # not the destination writer's ack copied onto both sides.
        source_checksum = str(writer_checksum)
        source_checksum_provenance = SOURCE_DIGEST_WRITE_PASS
        digest_provenance["source"] = source_checksum_provenance
    elif (
        str(dest_summary.get("checksum_mode") or "") == "source_reread"
        and writer_checksum
    ):
        # Second warehouse scan after the write. Streaming hands records=[] so
        # _compute_source_checksum would otherwise stamp writer_ack on this
        # digest and refuse full_checksum — the hole that kept Snowflake→Postgres
        # at Trust 86 / writer-ack after an independent re-read.
        source_checksum = str(writer_checksum)
        source_checksum_provenance = SOURCE_DIGEST_SOURCE_REREAD
        digest_provenance["source"] = source_checksum_provenance
    else:
        digest_records = records
        if records and pk_cols and dest_summary.get(CENSUS_KEY):
            # The keyed upsert hard-DELETEd tombstoned keys, so the destination
            # holds the live population only. Hashing the tombstoned rows here
            # compares two different scopes and fails a correct run — same owner
            # the write path strips with.
            digest_records, tombstones_excluded = live_records_for_digest(
                records,
                key_columns=[str(c) for c in pk_cols if c],
                mappings=mapping_dicts,
            )
            if tombstones_excluded:
                digest_provenance["source_scope"] = (
                    f"live_population_excluding_{tombstones_excluded}_tombstones"
                )
        source_checksum, source_checksum_provenance = _compute_source_checksum(
            digest_records,
            columns,
            mapping_dicts,
            source_schema,
            # A resumed writer digest covers the tail only; recompute from the
            # full population the caller re-supplied.
            "" if resumed_from else writer_checksum,
            target_cols=target_cols,
            dest_db_type=db_type,
            dest_types=dest_types,
            validation_mode=validation_mode,
            destination_pk_columns=[str(c) for c in pk_cols if c] or None,
            source_spool=source_spool,
        )
        digest_provenance["source"] = source_checksum_provenance

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
            rows_shaped_out=rows_shaped_out,
            rows_source_filtered=rows_source_filtered,
            rows_expanded=rows_expanded,
        )
        return _finalize(report.to_dict())

    # Request a real read-back; if the verifier is unavailable we will detect
    # the negative row count and surface a softer "writer only" result.
    # The target digest is always full-population. Balanced mode used to cap it
    # at 5000 rows, which hashed an arbitrary 5000-row prefix and compared it
    # against the full source digest — every balanced transfer above 5000 rows
    # reported a checksum mismatch on data that was byte-identical. Validation
    # mode governs fail-closed severity (`strict_checksum`), never the scope of
    # the comparison; the digest streams through FingerprintAccumulator, which
    # spills to disk, so full scope stays memory-bounded at any row count.
    checksum_limit = 0

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
    # Pre-write destination cardinality, stamped by the write adapter. Without
    # it an append into a non-empty table has no cardinality proof at all.
    raw_before = dest_summary.get(PRECOUNT_KEY)
    rows_before = int(raw_before) if isinstance(raw_before, int) else None
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

    # Both digests computed by the engines when they will render identically —
    # the same population coverage at a fraction of the cost, since neither side
    # has to bring rows into Python to hash them.
    engine_digests = None
    # A server-to-server COPY never brought rows into Python, so it computed both
    # digests itself — the source one inside the same snapshot it copied. Those
    # are the only two comparable values for such a run: recomputing either side
    # here would compare two different algorithms and always disagree.
    paired = _writer_supplied_engine_digests(dest_summary)
    if paired is not None and not source_checksum_scope_note:
        engine_digests = paired
    elif _engine_digest_enabled() and not source_checksum_scope_note:
        engine_digests = _engine_population_digests(
            source_endpoint=source_endpoint,
            dest_cfg=cfg,
            dest_db_type=db_type,
            dest_schema=schema,
            dest_table=table_name,
            mappings=mapping_dicts,
            source_schema=source_schema,
            dest_types=dest_types,
        )

    if engine_digests is not None:
        source_checksum, target_checksum, target_rows = engine_digests
        # Both sides re-read independently, in full — the strongest source a
        # digest can have here, and in particular not the writer's own account.
        source_checksum_provenance = SOURCE_DIGEST_ENGINE_POPULATION
        digest_provenance["source"] = source_checksum_provenance
    else:
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

    if db_type == "pgvector":
        # Physical embedding COUNT(*) is chunk cardinality, not source-row
        # conservation. Do not run Gate-8 as reader == vector COUNT(*) —
        # 2 documents / 5 chunks would fail (or invent a surplus if stuffed
        # as dest population). Identity COUNT(DISTINCT source_id) is stamped
        # in _finalize. Embeddings are opaque: cell fidelity stays unproven.
        measured = isinstance(target_rows, int) and target_rows >= 0
        return _finalize({
            "passed": measured,
            "unproven": True,
            "skipped_readback": True,
            "migration_proven": False,
            "message": (
                "pgvector write completed — Gate-8 embedding cell fidelity "
                "unproven (opaque vectors). Independent identity is "
                "COUNT(DISTINCT source_id); physical vector COUNT(*) and "
                "writer chunk-upsert acknowledgement are diagnostic."
                if measured
                else (
                    "pgvector destination read-back unavailable — identity "
                    "COUNT(DISTINCT source_id) could not be compared. Writer "
                    "chunk-upsert acknowledgement is not destination proof."
                )
            ),
            "source_rows": source_rows,
            "target_rows": target_rows if measured else None,
            "source_checksum": source_checksum,
            "target_checksum": target_checksum if measured else "",
            "rejected_rows": rejected_rows,
            "coerced_null_rows": coerced_null_rows,
            "rows_skipped": rows_skipped,
        })

    if db_type in VECTOR_IDENTITY_ENGINES:
        # dest-only REST engines: no SQL chunk COUNT.
        # Never stuff writer upsert ack into target_rows — that is the
        # dest_only writer_ack lie. Identity is stamped in _finalize.
        return _finalize({
            "passed": True,
            "unproven": True,
            "skipped_readback": True,
            "migration_proven": False,
            "message": (
                f"{db_type} write completed — Gate-8 embedding cell fidelity "
                "unproven (opaque vectors). Independent identity is "
                "COUNT(DISTINCT source_id); collection rowCount / "
                "points_count and writer chunk-upsert acknowledgement "
                "are diagnostic."
            ),
            "source_rows": source_rows,
            "target_rows": None,
            "source_checksum": source_checksum,
            "target_checksum": "",
            "rejected_rows": rejected_rows,
            "coerced_null_rows": coerced_null_rows,
            "rows_skipped": rows_skipped,
        })

    strict_checksum = validation_mode in ("strict", "maximum")

    if source_checksum_scope_note and target_rows >= 0:
        # Resumed streaming pass: the destination digest covers the whole
        # population, the source digest could only cover the resumed tail. Prove
        # cardinality and say plainly that population fidelity is not proven —
        # never compare two different populations and call the difference
        # corruption.
        from services.reconciliation import ReconciliationReport

        expected_rows = max(source_rows - dropped_rows - rows_skipped, 0)
        balanced = target_rows == expected_rows or (
            allow_extra_early and target_rows >= expected_rows
        )
        return _finalize(
            ReconciliationReport(
                passed=balanced,
                source_rows=source_rows,
                target_rows=target_rows,
                source_checksum="",
                target_checksum=target_checksum,
                rejected_rows=rejected_rows,
                coerced_null_rows=coerced_null_rows,
                rows_skipped=rows_skipped,
                checksum_scope=WHOLE_TABLE_NOT_COMPARABLE,
                message=(
                    (
                        f"Row count verified after resume: {target_rows:,} row(s) on the "
                        f"destination for {source_rows:,} source row(s). "
                        if balanced
                        else (
                            f"Row count mismatch after resume: expected {expected_rows:,} "
                            f"row(s) on the destination, found {target_rows:,}. "
                        )
                    )
                    + source_checksum_scope_note
                    + " Re-run without resume for full_checksum population proof."
                ),
            ).to_dict()
        )

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
            return _finalize({
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
                return _finalize({
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
                return _finalize({
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
        # dest_only sinks (milvus, pinecone, …) have no independent SQL
        # identity read-back by design — fail-closed strict mode would ban
        # every production write. pgvector identity is stamped above; do not
        # accept writer-ack as dest population for that driver.
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
            and rows_written_accounted == expected_written
        ):
            return _finalize({
                "passed": True,
                "message": (
                    f"Gate-8 sample screening compared "
                    f"{int(sample_compare.get('compared') or 0)} "
                    f"key-aligned field(s) for '{db_type}' "
                    f"({rows_written:,} rows written"
                    + (f", {rejected_rows:,} rejected" if rejected_rows else "")
                    + ") — screening only, not population proof"
                ),
                "source_rows": source_rows,
                "target_rows": rows_written_accounted,
                "source_checksum": source_checksum,
                "target_checksum": "",
                "rejected_rows": rejected_rows,
                "coerced_null_rows": coerced_null_rows,
                "rows_skipped": rows_skipped,
                "sample_compare": sample_compare,
                "assurance_level": "sample_screening",
            })
        if strict_checksum and not dest_only:
            return _finalize({
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
        if rows_written_accounted == expected_written:
            # Warehouse/SQL dests advertise an independent COUNT/digest. A
            # missing verifier there is a Gate-8 failure, not a green writer-ack.
            # dest_only / no-read sinks (email, Redis, Kafka) still ack.
            _warehouse_readback = {
                "postgresql",
                "mysql",
                "snowflake",
                "bigquery",
                "sqlserver",
                "oracle",
                "sqlite",
                "generic_sql",
                "databricks",
                "mongodb",
            }
            dest_key = str(db_type or "").strip().lower()
            if dest_key in _warehouse_readback and not dest_only:
                return _finalize({
                    "passed": False,
                    "unproven": True,
                    "message": (
                        f"Writer acknowledged {rows_written:,} rows but Gate-8 has no "
                        f"independent destination read-back for '{db_type}'. "
                        "Not migration_proven."
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
            return _finalize({
                "passed": True,
                "message": (
                    f"Transfer verified by writer: {rows_written:,} rows written"
                    + (f", {rejected_rows:,} rejected" if rejected_rows else "")
                    + (f", {rows_skipped:,} skipped" if rows_skipped else "")
                    + " (read-back verifier not available for this destination)"
                ),
                "source_rows": source_rows,
                "target_rows": rows_written_accounted,
                "source_checksum": source_checksum,
                "target_checksum": "",
                "rejected_rows": rejected_rows,
                "coerced_null_rows": coerced_null_rows,
                "rows_skipped": rows_skipped,
                "sample_compare": sample_compare,
            })
        report = reconcile(
            source_rows=source_rows,
            target_rows=rows_written_accounted,
            source_checksum=source_checksum,
            target_checksum="",
            rejected_rows=rejected_rows,
            strict_checksum=False,
            coerced_null_rows=coerced_null_rows,
            rows_skipped=rows_skipped,
            rows_shaped_out=rows_shaped_out,
            rows_source_filtered=rows_source_filtered,
            rows_expanded=rows_expanded,
            sample_compare=sample_compare,
        )
        return _finalize(report.to_dict())

    # Data loss signal: the target table holds fewer rows than we just wrote.
    if target_rows < rows_written_accounted:
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
            rows_shaped_out=rows_shaped_out,
            rows_source_filtered=rows_source_filtered,
            rows_expanded=rows_expanded,
        )
        return _finalize(report.to_dict())

    # We have a verified read-back. Extra dest rows are legitimate for append /
    # upsert into a non-empty sink; overwrite/mirror/replace must not soft-pass
    # extras (Airbyte/Fivetran-class honesty: mode-aware reconcile).
    sync_mode = sync_mode_early
    allow_extra = not is_overwrite_sync(sync_mode)
    if sync_mode.lower() in {"full_refresh_mirror", "mirror", "scd2"}:
        allow_extra = False

    # An incremental poll that reads nothing has nothing to reconcile: no batch
    # was written, so there is no batch digest to compare, and the whole-table
    # digest of a sink that already held rows never equals an empty source
    # digest. Comparing them failed every quiet poll of every scheduled
    # incremental sync — a red run for the normal outcome, which buries the runs
    # that are actually broken. The proof of a no-op is that the destination did
    # not move.
    if (
        allow_extra
        and source_rows == 0
        and int(rows_written or 0) == 0
        and dropped_rows == 0
    ):
        if rows_before is None:
            return _finalize({
                "passed": False,
                "unproven": True,
                "message": (
                    "No new source rows since the last watermark — nothing written, "
                    f"but pre-write destination count was not measured "
                    f"(destination now holds {target_rows:,} row(s)). "
                    "Not migration_proven."
                ),
                "source_rows": 0,
                "target_rows": target_rows,
                "source_checksum": source_checksum,
                "target_checksum": target_checksum,
                "rejected_rows": rejected_rows,
                "coerced_null_rows": coerced_null_rows,
                "rows_skipped": rows_skipped,
                "assurance_level": "none",
            })
        if target_rows == rows_before:
            return _finalize({
                "passed": True,
                "message": (
                    "No new source rows since the last watermark — nothing written, "
                    f"destination unchanged at {target_rows:,} row(s)."
                ),
                "source_rows": 0,
                "target_rows": target_rows,
                "source_checksum": source_checksum,
                "target_checksum": target_checksum,
                "rejected_rows": rejected_rows,
                "coerced_null_rows": coerced_null_rows,
                "rows_skipped": rows_skipped,
                "assurance_level": NO_OP_DEST_UNCHANGED,
            })

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

        return _finalize(ReconciliationReport(
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
        ).to_dict())

    # Upsert/append into a larger sink: whole-table digests are not comparable to
    # the batch. Re-fingerprint destination WHERE pk IN (batch keys) while keeping
    # full-table cardinality for the operator report.
    expected_batch = max(source_rows - dropped_rows - rows_skipped, 0)
    keyed_scope = ""
    # Upsert/append/quarantine-replay into a non-empty table: whole-table digests
    # are not comparable. Keyed fingerprint of written_ids proves the batch for
    # balanced and strict alike (strict_checksum only governs fail-closed severity
    # inside reconcile(), not whether we may re-scope the target digest).
    # The keyed digest is only comparable to the source digest when the key list
    # covers the *whole* batch. `written_ids` is capped (writer stash / bounded
    # sample), so a 100-row append whose stash held 50 keys re-read 50 rows and
    # compared them to the 100-row source digest — a guaranteed mismatch
    # reported as "checksum mismatch (strict)" on a correct load. A partial key
    # list is a diagnostic sample, not a scope: fall through to the append delta.
    keys_cover_batch = bool(written_ids) and len(written_ids) >= expected_batch
    if (
        allow_extra
        and pk_column
        and keys_cover_batch
        and target_rows > expected_batch
        and source_checksum
        and target_checksum
        and str(db_type) in KEYED_READBACK_ENGINES
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
        # A key-scoped digest is only comparable when the key identifies *one*
        # destination row. An append (no merge, no enforced constraint) can put
        # the same key in twice: appending one batch twice left two rows per key,
        # the keyed read-back returned 2x the batch, and its digest could never
        # equal the source's — a correct append failed itself with two hex
        # strings. Merge writes own their conflict target, and a declared
        # PK/unique constraint rejects the second copy; an identity inferred from
        # Map guarantees neither, so an append keeps the delta as its identity.
        keys_identify_one_row = not sync_mode_appends_without_key_resolution(
            sync_mode
        ) or _destination_enforces_single_key(
            db_type,
            cfg,
            schema=schema,
            table_name=table_name,
            key_column=pk_column,
        )
        if keyed_checksum and keys_identify_one_row:
            target_checksum = keyed_checksum
            keyed_scope = WRITTEN_BATCH_KEYS

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
        rows_shaped_out=rows_shaped_out,
        rows_source_filtered=rows_source_filtered,
        rows_expanded=rows_expanded,
        target_rows_before=rows_before,
        checksum_scope=keyed_scope,
    )
    return _finalize(report.to_dict())
