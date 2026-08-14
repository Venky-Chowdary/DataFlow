"""Gate 8 reconciliation for the universal transfer engine."""

from __future__ import annotations

import logging
from typing import Any

from connectors.writer_common import (
    map_rows_for_fingerprint,
    resolve_target_columns,
    transform_error_policy_for_validation_mode,
)
from services.dest_precount import PRECOUNT_KEY, stamp_artifact_census
from services.reconcile_coverage import (
    SOURCE_DIGEST_ENGINE_POPULATION,
    SOURCE_DIGEST_REMAPPED_ROWS,
    SOURCE_DIGEST_WRITER_ACK,
    WRITTEN_BATCH_KEYS,
)
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
    if isinstance(snap, dict) and snap:
        out["source_snapshot"] = dict(snap)
    return out

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
) -> tuple[str, str]:
    """Return ``(digest, provenance)`` for the source side of Gate-8.

    When source ``records`` are available, always remap and fingerprint them.
    Preferring the writer checksum first made Gate-8 circular: dest digest was
    compared to the writer's own ack, not to the remapped source population.

    The writer checksum remains the fallback when no records were supplied, and
    the provenance is returned with it so the report can say so. That fallback
    is not a corner case: a streaming pass hands over no rows, which is exactly
    how the large tables move, and labelling those runs ``full_checksum`` claimed
    two independent digests had agreed when only one digest existed.
    """
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

    if source_endpoint is not None and not source_rows:
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
) -> dict[str, Any]:
    """Verify row counts and checksums against the destination."""
    # Destination facts a row checksum cannot prove (generator watermarks today).
    physical_state: dict[str, Any] = {}
    # Where the source digest came from. Held in a box because the early-return
    # paths below run before it is known, and the report must never claim two
    # independent digests agreed when only the writer's was available.
    digest_provenance: dict[str, str] = {"source": ""}

    def _finalize(payload: dict[str, Any]) -> dict[str, Any]:
        if digest_provenance["source"] and "source_checksum_provenance" not in payload:
            payload = {
                **payload,
                "source_checksum_provenance": digest_provenance["source"],
            }
        # Property 3 — carry source snapshot id onto the reconcile report /
        # migration certificate surface.
        stamped = _finalize_reconcile(payload, dest_summary=dest_summary)
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
        return stamped

    rejected_rows = int(dest_summary.get("rejected_rows", 0) or 0)
    coerced_null_rows = int(dest_summary.get("coerced_null_rows", 0) or 0)
    rows_skipped = int(dest_summary.get("rows_skipped", 0) or 0)
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
    expected_written = max(source_rows - dropped_rows - rows_skipped, 0)
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
    if resumed_from and not (resume_full_source_rows and records):
        source_checksum_scope_note = (
            f"Resumed after {resumed_from:,} previously committed row(s): this pass "
            "read only the remaining slice, so no source digest covering the whole "
            "population is available to compare against the full-table destination "
            "digest."
        )

    source_checksum_provenance = ""
    if source_checksum_scope_note:
        source_checksum = ""
    else:
        source_checksum, source_checksum_provenance = _compute_source_checksum(
            records,
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

    strict_checksum = validation_mode in ("strict", "maximum")

    if source_checksum_scope_note and target_rows >= 0:
        # Resumed streaming pass: the destination digest covers the whole
        # population, the source digest could only cover the resumed tail. Prove
        # cardinality and say plainly that population fidelity is not proven —
        # never compare two different populations and call the difference
        # corruption.
        from services.reconcile_coverage import WHOLE_TABLE_NOT_COMPARABLE
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
        and (rows_before is None or target_rows == rows_before)
    ):
        unchanged = (
            f"destination unchanged at {target_rows:,} row(s)"
            if rows_before is not None
            else f"destination holds {target_rows:,} row(s); pre-write count unknown"
        )
        return _finalize({
            "passed": True,
            "message": (
                "No new source rows since the last watermark — nothing written, "
                f"{unchanged}."
            ),
            "source_rows": 0,
            "target_rows": target_rows,
            "source_checksum": source_checksum,
            "target_checksum": target_checksum,
            "rejected_rows": rejected_rows,
            "coerced_null_rows": coerced_null_rows,
            "rows_skipped": rows_skipped,
            "assurance_level": "no_op_destination_unchanged",
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
    if (
        allow_extra
        and pk_column
        and written_ids
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
        if keyed_checksum:
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
        target_rows_before=rows_before,
        checksum_scope=keyed_scope,
    )
    return _finalize(report.to_dict())
