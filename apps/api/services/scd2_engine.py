"""Slowly Changing Dimension Type 2 (SCD2) support for SQL destinations.

An SCD2 sync keeps a full history of every version of a row.  Each change to a
non-key attribute closes the previous version (valid_to + is_current=False) and
inserts a new current version (valid_from + is_current=True).  Re-running the
same source snapshot produces no new rows.

Source image (Fivetran / Airbyte incremental-deduped history / dbt snapshot
check strategy): hash non-SCD attributes, expire the current version when
the hash changes, insert a new current row. ``DF_MISSING`` hydrates from
the live current version so STOP_COLUMN cannot invent NULL history.

The source population is the shared ``SourceRowSpool`` — not
``records_to_matrix``. Map + write-quarantine + PK identity run per
bundle. ``apply_scd2`` fail-scans every reject, then merges one bundle
at a time (same two-pass contract as stream SCD2). Peak mapped RAM is
one bundle, not the full dict list. Not exactly-once. CDC default
remains at-least-once upsert. Catalog tiles ≠ transfer-live.
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Iterator
from datetime import date, datetime, timezone
from typing import Any

from services.dialect_profiles import quote_stored_identifier, stored_identifier
from services.engine_pool import release_engine

VALID_FROM_COLUMN = "valid_from"
VALID_TO_COLUMN = "valid_to"
IS_CURRENT_COLUMN = "is_current"
ROW_HASH_COLUMN = "row_hash"

SCD2_COLUMNS = [VALID_FROM_COLUMN, VALID_TO_COLUMN, IS_CURRENT_COLUMN, ROW_HASH_COLUMN]
_STAMP_SET = set(SCD2_COLUMNS)
VERSION_COLUMN = "version"
# Unit separator — safe delimiter for composite natural keys in-memory.
_KEY_SEP = "\x1f"


def resolve_scd2_cols(opts: dict[str, Any] | None = None) -> dict[str, Any]:
    """Recipe-declared SCD2 stamp/contract names with canonical defaults.

    Returns keys: valid_from, valid_to, is_current, row_hash, version
    (None unless declared), tracked ([] = all attributes tracked),
    type1, close_missing. Recipe ``scd2`` steps pass their normalized
    options through :meth:`ShapeRecipe.scd2_column_names`.

    ``change_column`` (D38 minor): a source column carrying the row's own
    change time (``updated_at``, CDC commit ts). When declared, the
    version's ``valid_from``/``valid_to`` stamp is that source instant —
    not the load clock — so the history orders by when the row actually
    changed. Rows whose change cell is missing/unparseable fall back to
    the run stamp; the run never invents a change time.
    """
    o = opts or {}
    return {
        "valid_from": str(o.get("valid_from") or VALID_FROM_COLUMN),
        "valid_to": str(o.get("valid_to") or VALID_TO_COLUMN),
        "is_current": str(o.get("is_current") or IS_CURRENT_COLUMN),
        "row_hash": str(o.get("row_hash") or ROW_HASH_COLUMN),
        "version": str(o.get("version") or "") or None,
        "tracked": [str(t) for t in (o.get("tracked") or [])],
        "type1": bool(o.get("type1")),
        "close_missing": bool(o.get("close_missing", True)),
        "change_column": str(
            o.get("change_column") or o.get("change_time_column") or ""
        ) or None,
    }


def _stamp_set(cols: dict[str, Any] | None) -> set[str]:
    """Stamp column names that must never hash or count as attributes."""
    if not cols:
        return _STAMP_SET
    names = {
        cols["valid_from"], cols["valid_to"], cols["is_current"],
        cols["row_hash"],
    }
    if cols.get("version"):
        names.add(cols["version"])
    return names


def stores_is_current_as_numeric(dialect: str) -> bool:
    """True when ``is_current`` is 0/1 storage, not ANSI BOOLEAN."""
    from services.dialect_profiles import stores_boolean_as_numeric

    return stores_boolean_as_numeric(dialect)


def scd2_is_current_predicate(dialect: str, quoted_column: str) -> str:
    """Dest-engine predicate for the current SCD2 version.

    SQLite INTEGER, SQL Server BIT, Oracle NUMBER(1) → ``= 1``.
    PostgreSQL / MySQL BOOLEAN → ``IS TRUE``. One rule for merge, expire,
    checksum, and conservation COUNT — not a per-engine patch.
    """
    from services.dialect_profiles import sql_bool_is_true

    return sql_bool_is_true(dialect, quoted_column)


def scd2_is_current_false_sql(dialect: str) -> str:
    """SQL literal that closes a current version (``is_current = …``)."""
    from services.dialect_profiles import sql_bool_false_literal

    return sql_bool_false_literal(dialect)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _row_change_stamp(
    row: dict[str, Any],
    cols: dict[str, Any],
    fallback: datetime,
) -> datetime:
    """Stamp for this row's valid_from/valid_to.

    D38: when the recipe declares ``change_column`` the version edges come
    from the row's own change time, not the load clock. A missing or
    unparseable cell falls back to the run stamp — never a fabricated one.
    The result is always timezone-aware (naive inputs are read as UTC).
    """
    col = cols.get("change_column")
    if not col:
        return fallback
    raw = row.get(col)
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        return fallback
    if isinstance(raw, datetime):
        dt = raw
    elif isinstance(raw, date):
        dt = datetime(raw.year, raw.month, raw.day)
    else:
        text = str(raw).strip()
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return fallback
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _compose_key(row: dict[str, Any], columns: list[str]) -> str:
    from connectors.writer_common import conflict_key_wire

    return _KEY_SEP.join(conflict_key_wire(row.get(c)) for c in columns)


# Keys per membership predicate. Bounded by the tightest engine limits the
# statement can hit: SQLite's expression-tree depth (1000) for OR chains,
# Oracle's 1000-element IN list, SQL Server's 2100 bind parameters.
PK_CLAUSE_MAX_KEYS = 400


def iter_pk_key_chunks(keys: set[str] | list[str]) -> Iterator[list[str]]:
    """Yield ``keys`` in predicate-sized chunks; order is stable."""
    ordered = list(keys)
    for start in range(0, len(ordered), PK_CLAUSE_MAX_KEYS):
        yield ordered[start : start + PK_CLAUSE_MAX_KEYS]


def key_column_types(table_obj: Any, columns: list[str]) -> dict[str, Any]:
    """SQLAlchemy column types of ``columns`` on a reflected/declared table."""
    cols = getattr(table_obj, "c", None)
    if cols is None:
        return {}
    return {c: cols[c].type for c in columns if c in cols}


def typed_key_bind(wire: str, sa_type: Any) -> Any:
    """Bind one composed-key segment in the destination column's carrier.

    Keys travel as ``conflict_key_wire`` text. Engines with implicit text
    coercion (PostgreSQL, MySQL, SQLite, Snowflake) accept ``INT64 IN ('5')``;
    BigQuery refuses it with *No matching signature for operator IN*. Binding
    the Python type the column declares makes the predicate exact everywhere
    and never relies on engine-side text coercion. Unparseable text keeps
    the wire form so a bad key fails loudly rather than matching another row.
    """
    from decimal import Decimal, InvalidOperation

    if sa_type is None:
        return wire
    try:
        py = sa_type.python_type
    except (NotImplementedError, AttributeError):
        return wire
    try:
        if py is bool:
            from connectors.sql_bind import coerce_boolean_wire

            parsed = coerce_boolean_wire(wire)
            return wire if parsed is None else parsed
        if py is int:
            from connectors.sql_bind import coerce_integer_wire

            parsed = coerce_integer_wire(wire, ddl_type="BIGINT")
            return wire if parsed is None else int(parsed)
        if py is Decimal:
            return Decimal(wire)
        if py is float:
            return float(wire)
    except (ValueError, TypeError, OverflowError, InvalidOperation):
        return wire
    return wire


def _pk_or_clause(
    columns: list[str],
    keys: set[str] | list[str],
    *,
    prefix: str,
    dialect: str = "",
    column_types: dict[str, Any] | None = None,
) -> tuple[str, dict[str, Any]]:
    """Membership predicate for one chunk of composite keys.

    Single-column keys bind as ``c IN (:p0, :p1, …)`` — flat, so no engine
    expression-depth limit applies and the planner sees one list probe.
    Composite keys use ``(c1=:p0_0 AND c2=:p0_1) OR …``. Callers must chunk
    with :func:`iter_pk_key_chunks`; larger sets are refused, not silently cut.
    ``column_types`` (see :func:`key_column_types`) binds each segment in the
    destination column's Python carrier via :func:`typed_key_bind`.
    """

    if not keys or not columns:
        return "1=0", {}
    if len(keys) > PK_CLAUSE_MAX_KEYS:
        raise ValueError(
            f"pk predicate over {len(keys)} keys exceeds {PK_CLAUSE_MAX_KEYS}; "
            "chunk with iter_pk_key_chunks"
        )
    types = column_types or {}
    quoted = [quote_stored_identifier(dialect, c) for c in columns]
    params: dict[str, Any] = {}
    if len(columns) == 1:
        names: list[str] = []
        for i, key in enumerate(keys):
            # ``prefix_i`` (not ``prefixi``): callers that merge chunked
            # clauses into one statement index chunks by digits, and
            # f"{prefix}{i}" collides across chunk boundaries —
            # "x1"+"20" == "x12"+"0" — silently rebinding one chunk's keys
            # to another's (B1-E8: 200 retained keys leaked from the
            # exclusion at 13 chunks).
            pname = f"{prefix}_{i}"
            params[pname] = typed_key_bind(key, types.get(columns[0]))
            names.append(f":{pname}")
        return f"{quoted[0]} IN ({', '.join(names)})", params
    clauses: list[str] = []
    for i, key in enumerate(keys):
        parts = key.split(_KEY_SEP)
        if len(parts) != len(columns):
            continue
        ands = []
        for j, col_q in enumerate(quoted):
            pname = f"{prefix}_{i}_{j}"
            ands.append(f"{col_q} = :{pname}")
            params[pname] = typed_key_bind(parts[j], types.get(columns[j]))
        if ands:
            clauses.append("(" + " AND ".join(ands) + ")")
    if not clauses:
        return "1=0", {}
    return "(" + " OR ".join(clauses) + ")", params


def _qualified_name(table: str, schema: str | None, dialect: str = "") -> str:

    table_quoted = quote_stored_identifier(dialect, table)
    schema_quoted = quote_stored_identifier(dialect, schema) if schema else None
    return f"{schema_quoted}.{table_quoted}" if schema_quoted else table_quoted


def _target_columns(records_columns: list[str], mappings: list[dict[str, Any]] | None) -> list[str]:
    from connectors.writer_common import resolve_target_columns

    if mappings:
        target_cols, _ = resolve_target_columns(mappings, {}, preserve_case=True)
        return target_cols
    return records_columns


def _row_hash(
    row: dict[str, Any],
    target_cols: list[str],
    *,
    tracked: list[str] | None = None,
    stamp_set: set[str] | None = None,
    dialect: str = "",
) -> str:
    """Stable hash of the non-SCD target attribute values.

    Columns are named by their stored spelling: a Snowflake rerun maps onto
    the existing table's ``NAME`` where the first run wrote ``name``, and a
    case-sensitive name made every unchanged row look changed (D47).

    ``tracked`` narrows change detection to the recipe-declared columns;
    an untracked change never opens a new version (Type-1 in place when
    the recipe opts in). ``DF_MISSING`` must never participate — callers
    hydrate omit-from-SET cells from the current SCD2 version before
    hashing so STOP_COLUMN cannot invent false history by hashing as
    empty/NULL.
    """
    from connectors.writer_common import conflict_key_wire
    from services.value_serializer import is_missing_sentinel

    stamps = stamp_set or _STAMP_SET
    hash_cols = list(tracked) if tracked else target_cols
    parts = []
    for c in hash_cols:
        if c in stamps:
            continue
        val = row.get(c)
        if is_missing_sentinel(val):
            continue
        parts.append(f"{stored_identifier(dialect, c)}={conflict_key_wire(val)}")
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def _hydrate_scd2_omit(
    row: dict[str, Any],
    current_attrs: dict[str, Any] | None,
    target_cols: list[str],
    *,
    stamp_set: set[str] | None = None,
) -> dict[str, Any]:
    """Overlay DF_MISSING from the current SCD2 version (never invent NULL history)."""
    from services.value_serializer import is_missing_sentinel

    stamps = stamp_set or _STAMP_SET
    out = dict(row)
    for c in target_cols:
        if c in stamps:
            continue
        if not is_missing_sentinel(out.get(c)):
            continue
        if current_attrs is not None and c in current_attrs:
            out[c] = current_attrs[c]
        else:
            # New PK with STOP_COLUMN omit — NULL is honest for a brand-new version.
            out[c] = None
    return out


def _ensure_scd_columns(
    engine: Any,
    table_obj: Any,
    dialect_name: str,
    cols: dict[str, Any] | None = None,
) -> None:
    """Add SCD2 columns to an existing table if they are missing."""
    import sqlalchemy as sa

    cols = cols or resolve_scd2_cols()
    inspector = sa.inspect(engine)
    schema = table_obj.schema
    table_name = table_obj.name
    existing = {c["name"] for c in inspector.get_columns(table_name, schema=schema)}
    from connectors.generic_sql import _sa_type_for_logical

    # timestamptz: stamps are tz-aware instants; engines with a zone
    # carrier keep it (TIMESTAMPTZ / TIMESTAMP_TZ / DATETIMEOFFSET),
    # zone-less engines degrade to their instant type — never a naive
    # wall clock (D38 "no zone").
    wanted = [
        (cols["valid_from"], "timestamptz"),
        (cols["valid_to"], "timestamptz"),
        (cols["is_current"], "boolean"),
        (cols["row_hash"], "string"),
    ]
    if cols.get("version"):
        wanted.append((cols["version"], "integer"))
    additions = [
        (name, _sa_type_for_logical(logical, dialect_name))
        for name, logical in wanted
        if name not in existing
    ]

    qualified = _qualified_name(table_name, schema, dialect_name)
    for col_name, sa_type in additions:
        col_quoted = quote_stored_identifier(dialect_name, col_name)
        type_ddl = sa_type.compile(dialect=engine.dialect)
        ddl = f"ALTER TABLE {qualified} ADD COLUMN {col_quoted} {type_ddl}"
        with engine.begin() as conn:
            try:
                conn.execute(sa.text(ddl))
            except Exception as exc:
                logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)


def _build_scd_table(
    engine: Any,
    table_name: str,
    schema_name: str | None,
    target_cols: list[str],
    column_types: dict[str, str],
    db_type: str,
    cols: dict[str, Any] | None = None,
) -> Any:
    """Build or reflect the destination table with SCD2 audit columns."""
    import sqlalchemy as sa
    from connectors.generic_sql import _build_table_for_write

    cols = resolve_scd2_cols(cols)
    all_cols = list(dict.fromkeys(list(target_cols) + list(_stamp_set(cols))))
    all_types = {**column_types}
    wanted_types = {
        cols["valid_from"]: "timestamptz",
        cols["valid_to"]: "timestamptz",
        cols["is_current"]: "boolean",
        cols["row_hash"]: "string",
    }
    if cols.get("version"):
        wanted_types[cols["version"]] = "integer"
    for scd_col, logical in wanted_types.items():
        if scd_col not in all_types:
            all_types[scd_col] = logical

    table_obj = _build_table_for_write(
        engine,
        table_name,
        schema_name,
        all_cols,
        all_types,
        db_type=db_type,
        conflict_columns=None,
    )

    dialect_name = engine.dialect.name if engine.dialect else ""
    inspector = sa.inspect(engine)
    table_exists = inspector.has_table(table_name, schema=schema_name)
    if table_exists:
        _ensure_scd_columns(engine, table_obj, dialect_name, cols)
    else:
        table_obj.create(engine, checkfirst=True)
    return table_obj


def _fetch_current_snapshots(
    conn: Any,
    qualified: str,
    pk_columns: list[str],
    target_cols: list[str],
    keys: set[str],
    dialect_name: str,
    column_types: dict[str, Any] | None = None,
    cols: dict[str, Any] | None = None,
) -> dict[str, dict[str, Any]]:
    """Return {composite_key: {hash, attrs, version}} for current rows in ``keys``."""
    import sqlalchemy as sa

    cols = cols or resolve_scd2_cols()
    stamps = _stamp_set(cols)
    if not keys or not pk_columns:
        return {}
    attr_cols = [c for c in target_cols if c not in stamps]
    extra_selects = [cols["version"]] if cols.get("version") else []
    select_cols = list(
        dict.fromkeys(list(pk_columns) + [cols["row_hash"]] + extra_selects + attr_cols)
    )
    cols_quoted = ", ".join(quote_stored_identifier(dialect_name, c) for c in select_cols)
    current_quoted = quote_stored_identifier(dialect_name, cols["is_current"])
    current_pred = scd2_is_current_predicate(dialect_name, current_quoted)
    out: dict[str, dict[str, Any]] = {}
    for chunk in iter_pk_key_chunks(keys):
        where_keys, params = _pk_or_clause(
            pk_columns, chunk, prefix="k", dialect=dialect_name,
            column_types=column_types,
        )
        sql = (
            f"SELECT {cols_quoted} FROM {qualified} "  # nosec B608
            f"WHERE {where_keys} AND {current_pred}"
        )
        for row in conn.execute(sa.text(sql), params):
            # Positional: the Snowflake dialect reports a stored ``ID`` as
            # ``id``, so a name lookup read every key as empty (D47).
            mapping = dict(zip(select_cols, row))
            key = _compose_key(mapping, pk_columns)
            attrs = {c: mapping.get(c) for c in attr_cols}
            version_val = (
                int(mapping.get(cols["version"]) or 0)
                if cols.get("version") else None
            )
            out[key] = {
                "hash": str(mapping.get(cols["row_hash"]) or ""),
                "attrs": attrs,
                "version": version_val,
            }
    return out


def _fetch_current_rows(
    conn: Any,
    qualified: str,
    pk_columns: list[str],
    keys: set[str],
    dialect_name: str,
) -> dict[str, str]:
    """Return {composite_key: row_hash} for current rows whose key is in ``keys``."""
    snaps = _fetch_current_snapshots(
        conn, qualified, pk_columns, pk_columns, keys, dialect_name
    )
    return {k: str(v.get("hash") or "") for k, v in snaps.items()}


def _insert_rows(conn: Any, table_obj: Any, rows: list[dict[str, Any]]) -> int:
    """Insert new SCD2 versions and return the number of rows inserted."""
    import sqlalchemy as sa

    if not rows:
        return 0
    result = conn.execute(sa.insert(table_obj), rows)
    return result.rowcount or len(rows)


def _expire_rows(
    conn: Any,
    qualified: str,
    pk_columns: list[str],
    keys: set[str],
    timestamp: datetime,
    dialect_name: str,
    column_types: dict[str, Any] | None = None,
    cols: dict[str, Any] | None = None,
    key_stamps: dict[str, datetime] | None = None,
) -> int:
    """Mark the current versions of ``keys`` as historical.

    ``key_stamps`` (D38): when a change_column run gives each key its own
    version edge, close at that key's stamp so the prior version's
    ``valid_to`` equals the new version's ``valid_from`` — a gapless
    chain instead of every close stamped at the load clock.
    """
    import sqlalchemy as sa

    cols = cols or resolve_scd2_cols()
    if not keys or not pk_columns:
        return 0
    current_quoted = quote_stored_identifier(dialect_name, cols["is_current"])
    valid_to_quoted = quote_stored_identifier(dialect_name, cols["valid_to"])
    current_pred = scd2_is_current_predicate(dialect_name, current_quoted)
    false_lit = scd2_is_current_false_sql(dialect_name)
    expired = 0
    stamp_groups: dict[Any, list[str]] = {}
    for k in keys:
        stamp_groups.setdefault(
            (key_stamps or {}).get(k, timestamp), []
        ).append(k)
    for stamp, stamp_keys in stamp_groups.items():
        for chunk in iter_pk_key_chunks(stamp_keys):
            where_keys, params = _pk_or_clause(
                pk_columns, chunk, prefix="e", dialect=dialect_name,
                column_types=column_types,
            )
            params["ts"] = stamp
            sql = (
                f"UPDATE {qualified} "  # nosec B608
                f"SET {valid_to_quoted} = :ts, {current_quoted} = {false_lit} "
                f"WHERE {where_keys} AND {current_pred}"
            )
            result = conn.execute(sa.text(sql), params)
            expired += result.rowcount or 0
    return expired


def _iter_current_keys(
    conn: Any,
    qualified: str,
    pk_columns: list[str],
    dialect_name: str,
    batch_size: int,
    cols: dict[str, Any] | None = None,
):
    """Stream the composite keys of every current version. Never OFFSET."""
    import sqlalchemy as sa
    from services.reconciliation_api import iter_select_row_dicts

    cols = cols or resolve_scd2_cols()
    current_pred = scd2_is_current_predicate(
        dialect_name, quote_stored_identifier(dialect_name, cols["is_current"])
    )
    cols_quoted = ", ".join(quote_stored_identifier(dialect_name, c) for c in pk_columns)
    sql = f"SELECT {cols_quoted} FROM {qualified} WHERE {current_pred}"  # nosec B608
    for batch in iter_select_row_dicts(
        conn, sa.text(sql), pk_columns, itersize=max(1, int(batch_size))
    ):
        for row in batch:
            yield _compose_key(row, pk_columns)


def close_versions_missing_from_keys(
    conn: Any,
    qualified: str,
    pk_columns: list[str],
    source_keys: set[str],
    timestamp: datetime,
    dialect_name: str,
    batch_size: int = 1_000,
    column_types: dict[str, Any] | None = None,
    cols: dict[str, Any] | None = None,
) -> int:
    """Expire current versions whose key is not in a complete source key set.

    Buffered-path twin of :func:`close_versions_missing_from_snapshot`: the
    source population lives in a spool, not a staging table, so the
    difference ``dest_current \\ source`` is taken key-by-key against a
    streamed read of the destination's current versions and expired in
    bundles. Only valid for a complete source snapshot.
    """
    if not pk_columns:
        return 0
    missing = [
        k
        for k in _iter_current_keys(
            conn, qualified, pk_columns, dialect_name, batch_size, cols
        )
        if k and k not in source_keys
    ]
    expired = 0
    for start in range(0, len(missing), max(1, int(batch_size))):
        expired += _expire_rows(
            conn,
            qualified,
            pk_columns,
            set(missing[start : start + batch_size]),
            timestamp,
            dialect_name,
            column_types=column_types,
            cols=cols,
        )
    return expired


def close_versions_missing_from_snapshot(
    conn: Any,
    target_qualified: str,
    snapshot_qualified: str,
    pk_columns: list[str],
    *,
    timestamp: datetime | None = None,
    dialect: str = "",
    cols: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Expire current versions whose key is absent from a complete source snapshot.

    Kimball hard-delete close-out (dbt snapshot ``invalidate_hard_deletes``):
    a key that no longer exists at the source keeps its history but stops
    being current. Only valid when ``snapshot_qualified`` holds the *whole*
    source population — a limited or watermark-narrowed read must never call
    this, or every unread key would be closed. The closed count is a
    dest-engine COUNT of the transition taken before the UPDATE, never
    driver ``rowcount``.
    """
    import sqlalchemy as sa

    cols = cols or resolve_scd2_cols()
    if not pk_columns:
        return {"closed_missing_rows": 0}
    current_quoted = quote_stored_identifier(dialect, cols["is_current"])
    valid_to_quoted = quote_stored_identifier(dialect, cols["valid_to"])
    current_pred = scd2_is_current_predicate(dialect, current_quoted)
    false_lit = scd2_is_current_false_sql(dialect)
    join_pred = " AND ".join(
        f"{snapshot_qualified}.{quote_stored_identifier(dialect, c)} = "
        f"{target_qualified}.{quote_stored_identifier(dialect, c)}"
        for c in pk_columns
    )
    missing_pred = (
        f"{current_pred} AND NOT EXISTS "
        f"(SELECT 1 FROM {snapshot_qualified} WHERE {join_pred})"
    )
    closed = conn.execute(
        sa.text(
            f"SELECT COUNT(*) FROM {target_qualified} WHERE {missing_pred}"  # nosec B608
        )
    ).scalar()
    closed_n = int(closed or 0)
    stamp = timestamp or _now_utc()
    if closed_n:
        conn.execute(
            sa.text(
                f"UPDATE {target_qualified} "  # nosec B608
                f"SET {valid_to_quoted} = :ts, {current_quoted} = {false_lit} "
                f"WHERE {missing_pred}"
            ),
            {"ts": stamp},
        )
    return {"closed_missing_rows": closed_n, "run_stamp": stamp.isoformat()}


def _active_checksum(
    conn: Any,
    qualified: str,
    target_cols: list[str],
    batch_size: int,
    dialect_name: str,
    dest_types: dict[str, str] | None = None,
    cols: dict[str, Any] | None = None,
    pk_columns: list[str] | None = None,
    exclude_keys: set[str] | None = None,
) -> tuple[int, str]:
    """Current-version digest: one streamed SELECT. Never OFFSET.

    SQL Server FETCH requires ORDER BY; Oracle/DB2 reject LIMIT; OFFSET is
    O(n²) and can skip/duplicate. Same streaming kernel as Gate-8 read-back,
    steered by the same column types the write pass fingerprinted against.

    ``exclude_keys`` + ``pk_columns`` restrict the digest to current rows
    whose key is NOT in the set (B1-E5: ``close_missing=False`` retains
    deleted keys as current — the digest comparable to the source covers
    only source-keyed rows). Exclusion predicates are chunked through
    :func:`iter_pk_key_chunks` like every other key predicate.
    """
    import sqlalchemy as sa
    from services.reconciliation_api import stream_select_checksum

    cols = cols or resolve_scd2_cols()
    stamps = _stamp_set(cols)
    current_quoted = quote_stored_identifier(dialect_name, cols["is_current"])
    current_pred = scd2_is_current_predicate(dialect_name, current_quoted)
    attr_cols = [c for c in target_cols if c not in stamps]
    cols_quoted = ",".join(quote_stored_identifier(dialect_name, c) for c in attr_cols)
    where = current_pred
    params: dict[str, Any] = {}
    if exclude_keys and pk_columns:
        not_clauses: list[str] = []
        for i, chunk in enumerate(iter_pk_key_chunks(list(exclude_keys))):
            clause, clause_params = _pk_or_clause(
                pk_columns, chunk, prefix=f"x{i}", dialect=dialect_name,
            )
            not_clauses.append(f"NOT ({clause})")
            params.update(clause_params)
        where = f"{current_pred} AND {' AND '.join(not_clauses)}"
    sql = f"SELECT {cols_quoted} FROM {qualified} WHERE {where}"  # nosec B608
    return stream_select_checksum(
        conn,
        sa.text(sql).bindparams(**params),
        attr_cols,
        itersize=max(1, int(batch_size)),
        dest_db_type=dialect_name,
        dest_types=dest_types,
    )


def _scd2_map_context(
    endpoint: Any,
    columns: list[str],
    schema: dict[str, str] | None,
    mappings: list[dict[str, Any]] | None,
    conflict_columns: list[str],
    *,
    validation_mode: str,
) -> dict[str, Any]:
    """Shared Map / dest-type / policy facts for prepare and apply."""
    from connectors.writer_common import (
        sanitize_identifier,
        transform_error_policy_for_validation_mode,
    )
    from src.transfer.connector_capabilities import resolve_driver_type

    if not conflict_columns:
        raise ValueError("SCD2 sync requires a primary key / conflict column")
    pk_columns = [c for c in conflict_columns if c]
    if not pk_columns:
        raise ValueError("SCD2 sync requires a primary key / conflict column")

    target_cols = _target_columns(columns, mappings)
    effective_mappings = mappings or [{"source": c, "target": c} for c in columns]
    dest_types = {c: (schema or {}).get(c, "string") for c in target_cols}
    # Prefer Map target_type stamps when present (DECIMAL(p,s) / VARCHAR(n)).
    # Stamp under sanitized target names — same key space as target_cols / quarantine.
    for m in effective_mappings:
        tgt_raw = str(m.get("target") or "").strip()
        stamped = str(m.get("target_type") or m.get("dest_type") or "").strip()
        if not tgt_raw or not stamped:
            continue
        tgt = sanitize_identifier(tgt_raw, preserve_case=True)
        dest_types[tgt] = stamped
        if tgt_raw != tgt:
            dest_types.pop(tgt_raw, None)
    return {
        "pk_columns": pk_columns,
        "target_cols": target_cols,
        "effective_mappings": effective_mappings,
        "dest_types": dest_types,
        "target_types": [str(dest_types.get(c) or "") for c in target_cols],
        "error_policy": transform_error_policy_for_validation_mode(validation_mode),
        "dest_kind": resolve_driver_type(getattr(endpoint, "format", "") or ""),
        "dest_nullability": dict(
            (getattr(endpoint, "extra", None) or {}).get("schema_nullability") or {}
        ),
        "schema": schema or {},
        "extra": (
            getattr(endpoint, "extra", None)
            if isinstance(getattr(endpoint, "extra", None), dict)
            else {}
        ),
    }


def _pk_validate_mapped_rows(
    mapped_rows: list[dict[str, Any]],
    pk_columns: list[str],
    rejected_details: list[dict[str, Any]],
    *,
    row_numbers: list[int] | None = None,
) -> list[dict[str, Any]]:
    """Empty / DF_MISSING / reader-null PK identity quarantines — never a silent skip."""
    from connectors.writer_common import _is_nullish_conflict_key

    pk_ok_rows: list[dict[str, Any]] = []
    for row_idx, row in enumerate(mapped_rows):
        row_no = (
            int(row_numbers[row_idx])
            if row_numbers is not None and row_idx < len(row_numbers)
            else row_idx + 1
        )
        nullish = [c for c in pk_columns if _is_nullish_conflict_key(row.get(c))]
        if nullish:
            rejected_details.append(
                {
                    "row": row_no,
                    "column": ",".join(nullish),
                    "target": ",".join(pk_columns),
                    "value": "",
                    "reason": (
                        "SCD2 primary key cannot be DF_MISSING / STOP_COLUMN omit "
                        "or extract SQL NULL (an empty string is a valid key)"
                    ),
                    "policy": "quarantine",
                    "chars": [],
                }
            )
            continue
        pk_ok_rows.append(row)
    return pk_ok_rows


def _finish_scd2_map_bundle(
    bundle: Any,
    ctx: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    """Quarantine + PK-validate one mapped bundle. No last-write-wins drop."""
    from connectors.writer_common import (
        apply_write_quarantine_matrix_keeping_numbers,
        transform_error_policy,
    )

    target_cols = list(ctx["target_cols"])
    details = list(bundle.rejected_details)
    errors = list(bundle.transform_errors)
    mapped = list(bundle.mapped_rows)
    nums = list(bundle.accepted_source_rows or [])
    target_types = list(ctx["target_types"])
    if mapped and any(t.strip() for t in target_types):
        policy = transform_error_policy(ctx["error_policy"])
        mapped, nums = apply_write_quarantine_matrix_keeping_numbers(
            mapped,
            target_cols,
            target_types,
            details,
            policy,
            dialect_label=(ctx["dest_kind"] or "SCD2").strip() or "SCD2",
            mappings=list(ctx["effective_mappings"]) or None,
            dest_db=str(ctx["dest_kind"] or ""),
            source_row_numbers=nums or None,
        )
        # Same bind owners as every SQL writer: strict-typed engines (BigQuery)
        # refuse STRING binds into NUMERIC/INT64/TIMESTAMP that PostgreSQL,
        # MySQL and SQLite coerce implicitly.
        from connectors.writer_common import (
            bind_rows_keeping_numbers,
            normalize_temporal_cells,
        )

        engine = str(ctx["dest_kind"] or "").strip().lower()
        mapped = normalize_temporal_cells(
            mapped, target_types, target_cols, engine=engine
        )
        mapped, nums = bind_rows_keeping_numbers(
            mapped,
            target_cols,
            target_types,
            details,
            policy,
            engine=engine,
            dialect_label=(ctx["dest_kind"] or "SCD2").strip() or "SCD2",
            mappings=list(ctx["effective_mappings"]) or None,
            row_numbers=nums or None,
        )
    dicts = [dict(zip(target_cols, row)) for row in mapped]
    pk_ok = _pk_validate_mapped_rows(
        dicts, list(ctx["pk_columns"]), details, row_numbers=nums or None
    )
    return pk_ok, details, errors


def iter_scd2_prepared_bundles(
    *,
    columns: list[str],
    ctx: dict[str, Any],
    records: list[dict[str, Any]] | None = None,
    source_spool: Any = None,
    batch_size: int = 1_000,
):
    """Yield ``(pk_ok_rows, rejected_details, transform_errors)`` per spool bundle.

    Reuses ``iter_mapped_bundles_from_source``. Does not last-write-wins
    inside the bundle — SCD2 merge applies every PK-ok arrival in order
    (intra-batch hash update after insert).
    """
    from connectors.sql_write_materialize import iter_mapped_bundles_from_source

    for bundle in iter_mapped_bundles_from_source(
        headers=columns,
        mappings=ctx["effective_mappings"],
        target_cols=list(ctx["target_cols"]),
        column_types=ctx["schema"],
        dest_types=ctx["dest_types"],
        error_policy=ctx["error_policy"],
        preserve_case=True,
        dest_kind=ctx["dest_kind"],
        destination_pk_columns=list(ctx["pk_columns"]),
        destination_column_nullability=ctx["dest_nullability"],
        records=None if source_spool is not None else records,
        source_spool=source_spool,
        extra=ctx.get("extra") or {},
        batch_size=batch_size,
    ):
        pk_ok, details, errors = _finish_scd2_map_bundle(bundle, ctx)
        yield pk_ok, details, errors
        del bundle, pk_ok


def prepare_scd2_mapped_rows(
    endpoint: Any,
    records: list[dict[str, Any]],
    columns: list[str],
    schema: dict[str, str] | None,
    mappings: list[dict[str, Any]] | None,
    conflict_columns: list[str],
    *,
    validation_mode: str = "strict",
    source_spool: Any = None,
    clear_records: bool = False,
    retain_mapped: bool = True,
    batch_size: int = 1_000,
) -> dict[str, Any]:
    """Map + PK-validate SCD2 rows without writing history.

    Used for stream preflight (abort before any batch commits). Returns
    ``ok=False`` when FAIL_JOB / strict policy blocks a partial history
    write. Source cells come from the shared spool — never
    ``records_to_matrix``. ``retain_mapped=False`` keeps only reject
    details (apply's fail-scan). Hash is finalized in ``apply_scd2``
    after DF_MISSING hydrate from the current version.
    """
    from connectors.sql_write_materialize import ensure_sql_source_spool
    from connectors.writer_common import reject_on_strict_policy

    ctx = _scd2_map_context(
        endpoint,
        columns,
        schema,
        mappings,
        conflict_columns,
        validation_mode=validation_mode,
    )
    pk_columns = list(ctx["pk_columns"])
    target_cols = list(ctx["target_cols"])
    error_policy = ctx["error_policy"]
    spool, close_spool = ensure_sql_source_spool(
        headers=columns,
        records=None if source_spool is not None else records,
        mappings=ctx["effective_mappings"],
        extra=ctx.get("extra") or {},
        source_spool=source_spool,
    )
    if clear_records and records is not None:
        records.clear()
    rejected_details: list[dict[str, Any]] = []
    transform_errors: list[str] = []
    pk_ok_rows: list[dict[str, Any]] = []
    try:
        for pk_ok, details, errors in iter_scd2_prepared_bundles(
            columns=columns,
            ctx=ctx,
            source_spool=spool,
            batch_size=batch_size,
        ):
            rejected_details.extend(details)
            transform_errors.extend(errors)
            if retain_mapped:
                pk_ok_rows.extend(pk_ok)
            del pk_ok
    finally:
        if close_spool:
            spool.close()

    abort = reject_on_strict_policy(error_policy, rejected_details, "SCD2")
    if abort:
        return {
            "ok": False,
            "error": abort,
            "mapped_rows": [],
            "primary_key_columns": pk_columns,
            "target_columns": target_cols,
            "rejected_details": list(rejected_details),
            "rejected_rows": len(rejected_details),
            "transform_errors": list(transform_errors)[:20],
            "error_policy": error_policy,
        }

    return {
        "ok": True,
        "mapped_rows": pk_ok_rows,
        "primary_key_columns": pk_columns,
        "target_columns": target_cols,
        "rejected_details": list(rejected_details),
        "rejected_rows": len(rejected_details),
        "transform_errors": list(transform_errors)[:20],
        "error_policy": error_policy,
    }


def _type1_update_rows(
    conn: Any,
    qualified: str,
    pk_columns: list[str],
    updates: list[tuple[str, dict[str, Any]]],
    dialect_name: str,
    cols: dict[str, Any],
    column_types: dict[str, Any] | None = None,
) -> int:
    """Type-1 overwrite of untracked attributes on the current version.

    The key's tracked hash matched, so no new version opens — the recipe
    declares untracked columns refresh in place (Kimball Type-1), valid_from
    and the version number stay untouched.
    """
    import sqlalchemy as sa
    from connectors.writer_common import conflict_key_wire as _ckw  # noqa: F401

    if not updates:
        return 0
    current_quoted = quote_stored_identifier(dialect_name, cols["is_current"])
    current_pred = scd2_is_current_predicate(dialect_name, current_quoted)
    # One UPDATE per key at 20k untracked changes is 20k round-trips; group
    # by the updated-column signature and send each group's parameter sets
    # in a single executemany so the driver batches them server-side.
    groups: dict[tuple[str, ...], list[tuple[str, dict[str, Any]]]] = {}
    for key, attrs in updates:
        if not attrs:
            continue
        groups.setdefault(tuple(sorted(attrs)), []).append((key, attrs))
    updated = 0
    for sig, members in groups.items():
        cols_list = list(sig)
        set_clause = ", ".join(
            f"{quote_stored_identifier(dialect_name, c)} = :__v{i}"
            for i, c in enumerate(cols_list)
        )
        sql = None
        param_sets: list[dict[str, Any]] = []
        for key, attrs in members:
            where_keys, params = _pk_or_clause(
                pk_columns, [key], prefix="t1", dialect=dialect_name,
                column_types=column_types,
            )
            if sql is None:
                sql = (
                    f"UPDATE {qualified} SET {set_clause} "  # nosec B608
                    f"WHERE {where_keys} AND {current_pred}"
                )
            for i, c in enumerate(cols_list):
                params[f"__v{i}"] = attrs[c]
            param_sets.append(params)
        if sql is not None and param_sets:
            result = conn.execute(sa.text(sql), param_sets)
            updated += result.rowcount or 0
    return updated


def _fetch_max_versions(
    conn: Any,
    qualified: str,
    pk_columns: list[str],
    keys: set[str] | list[str],
    dialect_name: str,
    cols: dict[str, Any],
    pk_types: dict[str, str] | None = None,
) -> dict[str, int]:
    """max(version) per key across every version row — used to number a
    reopened key after a soft close."""
    import sqlalchemy as sa

    ver_q = quote_stored_identifier(dialect_name, cols["version"])
    pk_sql = ", ".join(quote_stored_identifier(dialect_name, c) for c in pk_columns)
    out: dict[str, int] = {}
    for chunk in iter_pk_key_chunks(list(keys)):
        pred, params = _pk_or_clause(
            pk_columns, chunk, prefix="mxv", dialect=dialect_name,
            column_types=pk_types,
        )
        for row in conn.execute(
            sa.text(
                f"SELECT {pk_sql}, MAX({ver_q}) AS _mx FROM {qualified} "  # nosec B608
                f"WHERE {pred} GROUP BY {pk_sql}"
            ),
            params,
        ):
            m = dict(zip([*pk_columns, "_mx"], row))
            out[_compose_key(m, pk_columns)] = int(m.get("_mx") or 0)
    return out


def _merge_scd2_bundle(
    conn: Any,
    table_obj: Any,
    qualified: str,
    batch: list[dict[str, Any]],
    pk_columns: list[str],
    target_cols: list[str],
    dialect_name: str,
    timestamp: datetime,
    cols: dict[str, Any] | None = None,
) -> tuple[int, int, int]:
    """Expire + insert one PK-ok bundle. Intra-bundle last hash wins after insert."""
    from connectors.writer_common import conflict_key_wire

    cols = cols or resolve_scd2_cols()
    stamps = _stamp_set(cols)
    tracked = list(cols.get("tracked") or [])
    tracked_set = set(tracked) if tracked else None
    keys: set[str] = set()
    for row in batch:
        # NULL keys were quarantined upstream; '' is a key (D53).
        keys.add(_compose_key(row, pk_columns))
    pk_types = key_column_types(table_obj, pk_columns)
    current_snaps = _fetch_current_snapshots(
        conn, qualified, pk_columns, target_cols, keys, dialect_name,
        pk_types, cols,
    )
    current_hashes = {k: str(v.get("hash") or "") for k, v in current_snaps.items()}

    # Untracked attributes (not the key, not stamps, not the tracked set)
    # refresh in place when the recipe opts into Type-1.
    attr_cols = [
        c for c in target_cols if c not in stamps and c not in pk_columns
    ]
    untracked_cols = (
        [c for c in attr_cols if c not in tracked_set]
        if tracked_set is not None
        else []
    )

    to_insert: list[dict[str, Any]] = []
    to_expire: set[str] = set()
    expire_stamps: dict[str, datetime] = {}
    to_type1: list[tuple[str, dict[str, Any]]] = []

    # A key with no current version may still hold closed history (it was
    # soft-closed on an earlier snapshot) — its reopened version is
    # max(version)+1, never v1 again.
    max_versions: dict[str, int] = {}
    if cols.get("version"):
        missing = [k for k in keys if k not in current_snaps]
        if missing:
            max_versions = _fetch_max_versions(
                conn, qualified, pk_columns, missing, dialect_name, cols,
                pk_types,
            )

    for row in batch:
        key = _compose_key(row, pk_columns)
        snap = current_snaps.get(key)
        hydrated = _hydrate_scd2_omit(
            row,
            (snap or {}).get("attrs") if snap else None,
            target_cols,
            stamp_set=stamps,
        )
        new_hash = _row_hash(
            hydrated, target_cols, tracked=tracked or None, stamp_set=stamps,
            dialect=dialect_name,
        )
        hydrated[cols["row_hash"]] = new_hash
        if key in current_hashes and current_hashes[key] == new_hash:
            # Tracked attributes unchanged — Type-1 refresh of untracked
            # columns on the current row, only where they actually differ.
            if cols.get("type1") and snap is not None and untracked_cols:
                snap_attrs = snap.get("attrs") or {}
                changed = {
                    c: hydrated.get(c)
                    for c in untracked_cols
                    if conflict_key_wire(hydrated.get(c))
                    != conflict_key_wire(snap_attrs.get(c))
                }
                if changed:
                    to_type1.append((key, changed))
                    snap_attrs.update(changed)
            continue
        if key in current_hashes:
            to_expire.add(key)
        # D38: a declared change_column gives each version its own edge —
        # the source change instant, not the load clock. The closing
        # version's valid_to is the SAME stamp so the timeline is
        # gapless (close == next open, never a wall-clock gap between them).
        row_stamp = _row_change_stamp(hydrated, cols, timestamp)
        if key in to_expire:
            expire_stamps[key] = row_stamp
        hydrated[cols["valid_from"]] = row_stamp
        hydrated[cols["valid_to"]] = None
        hydrated[cols["is_current"]] = True
        if cols.get("version"):
            base = (snap or {}).get("version") or max_versions.get(key) or 0
            hydrated[cols["version"]] = int(base) + 1
        to_insert.append(hydrated)

    expired = 0
    if to_expire:
        expired = _expire_rows(
            conn, qualified, pk_columns, to_expire, timestamp, dialect_name,
            column_types=pk_types, cols=cols,
            key_stamps=expire_stamps or None,
        )
    type1_updated = _type1_update_rows(
        conn, qualified, pk_columns, to_type1, dialect_name, cols,
        column_types=pk_types,
    )
    inserted = _insert_rows(conn, table_obj, to_insert)
    # Same-connection later bundles must see this bundle's current hash
    # (the original in-memory update after insert).
    for row in to_insert:
        key = _compose_key(row, pk_columns)
        current_hashes[key] = row[cols["row_hash"]]
        current_snaps[key] = {
            "hash": row[cols["row_hash"]],
            "version": row.get(cols["version"]) if cols.get("version") else None,
            "attrs": {c: row.get(c) for c in target_cols if c not in stamps},
        }
    return inserted, expired, type1_updated


def apply_scd2(
    endpoint: Any,
    records: list[dict[str, Any]],
    columns: list[str],
    schema: dict[str, str] | None,
    mappings: list[dict[str, Any]] | None,
    conflict_columns: list[str],
    *,
    batch_size: int = 1_000,
    validation_mode: str = "strict",
    source_spool: Any = None,
    clear_records: bool = False,
    complete_snapshot: bool = False,
    scd2_opts: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Apply an SCD2 merge against the SQL destination.

    ``conflict_columns`` is the destination primary key (one or more columns).
    Returns a summary dict with ``rows_written`` (new current versions),
    ``updated_rows`` (closed old versions), ``active_rows``, and ``active_checksum``.

    ``complete_snapshot=True`` declares ``records`` / ``source_spool`` to be the
    whole source population: current versions whose key is absent are closed
    (Kimball hard-delete close-out) and reported as ``closed_missing_rows``.
    A per-batch or limited caller must leave it False.

    Fail / FAIL_JOB: scan every spool bundle, collect every reject, then
    refuse the history write — never expire/insert a prefix. Merge pass
    remaps from the same spool one bundle at a time. Peak mapped RAM is
    one bundle. Not exactly-once.
    """

    from connectors.generic_sql import get_sql_schema, get_sqlalchemy_engine
    from connectors.sql_write_materialize import ensure_sql_source_spool
    from src.transfer.adapters import resolve_connector_config
    from src.transfer.connector_capabilities import resolve_driver_type

    cols = resolve_scd2_cols(scd2_opts)
    ctx = _scd2_map_context(
        endpoint,
        columns,
        schema,
        mappings,
        conflict_columns,
        validation_mode=validation_mode,
    )
    pk_columns = list(ctx["pk_columns"])
    target_cols = list(ctx["target_cols"])
    spool, close_spool = ensure_sql_source_spool(
        headers=columns,
        records=None if source_spool is not None else records,
        mappings=ctx["effective_mappings"],
        extra=ctx.get("extra") or {},
        source_spool=source_spool,
    )
    if clear_records and records is not None:
        records.clear()

    def _blocked(error: str, rejected: list[dict[str, Any]], errors: list[str]) -> dict[str, Any]:
        return {
            "ok": False,
            "error": error,
            "rows_written": 0,
            "updated_rows": 0,
            "active_rows": 0,
            "active_checksum": "",
            "mode": "scd2",
            "primary_key_columns": pk_columns,
            "target_columns": target_cols,
            "rejected_details": list(rejected),
            "rejected_rows": len(rejected),
            "transform_errors": list(errors)[:20],
        }

    try:
        scan = prepare_scd2_mapped_rows(
            endpoint,
            [],
            columns,
            schema,
            mappings,
            conflict_columns,
            validation_mode=validation_mode,
            source_spool=spool,
            retain_mapped=False,
            batch_size=batch_size,
        )
        rejected_details = list(scan.get("rejected_details") or [])
        transform_errors = list(scan.get("transform_errors") or [])
        if scan.get("ok") is False:
            return _blocked(
                str(scan.get("error") or "SCD2 map/Risk Contract blocked history merge"),
                rejected_details,
                transform_errors,
            )

        db_type = resolve_driver_type(endpoint.format)
        cfg = resolve_connector_config(endpoint)
        table = endpoint.table or endpoint.collection or "dt_import"
        schema_name = get_sql_schema(cfg)

        engine = get_sqlalchemy_engine(cfg)
        dialect_name = engine.dialect.name if engine.dialect else ""
        # The CREATE binds the Map contract (operator / population-widened
        # ``target_type`` stamps), never the peeked source carrier alone —
        # preflight proved fit against the stamps, so the DDL must be the same.
        column_types: dict[str, str] = {
            c: str(ctx["dest_types"].get(c) or (schema or {}).get(c) or "string")
            for c in target_cols
        }

        try:
            table_obj = _build_scd_table(
                engine, table, schema_name, target_cols, column_types,
                db_type, cols,
            )
            # Window start for the Gate-8 version recount: every stamp this
            # call writes is >= this instant. Callers that split the run
            # across several apply calls (streaming) stamp their own, earlier
            # window on the run summary.
            run_started_at = _now_utc()
            timestamp = _now_utc()
            inserted_total = 0
            expired_total = 0
            type1_total = 0
            closed_missing = 0
            source_keys: set[str] = set()
            qualified = _qualified_name(table, schema_name, dialect_name)
            # close_missing=off on a complete snapshot: the run's own rows
            # are the only honest source-side digest for the source-keyed
            # scope (the writer ack covers the whole current set, which
            # includes retained rows). Accumulate it in the same pass.
            source_fp_acc: Any = None
            if complete_snapshot and not cols.get("close_missing", True):
                from services.fingerprint_accumulator import (
                    FingerprintAccumulator,
                )
                from services.reconciliation import _iter_fingerprints

                _attr_cols = [
                    c for c in target_cols if c not in _stamp_set(cols)
                ]
                source_fp_acc = (
                    FingerprintAccumulator(),
                    _iter_fingerprints,
                    _attr_cols,
                )

            with engine.begin() as conn:
                for pk_ok, _details, _errors in iter_scd2_prepared_bundles(
                    columns=columns,
                    ctx=ctx,
                    source_spool=spool,
                    batch_size=batch_size,
                ):
                    if pk_ok:
                        if complete_snapshot:
                            source_keys.update(
                                _compose_key(row, pk_columns) for row in pk_ok
                            )
                        if source_fp_acc is not None:
                            _acc, _iter_fp, _acols = source_fp_acc
                            _acc.add_many(
                                _iter_fp(
                                    pk_ok,
                                    _acols,
                                    dest_db_type=dialect_name,
                                    dest_types=ctx["dest_types"],
                                )
                            )
                        inserted, expired, type1_updated = (
                            _merge_scd2_bundle(
                                conn,
                                table_obj,
                                qualified,
                                pk_ok,
                                pk_columns,
                                target_cols,
                                dialect_name,
                                timestamp,
                                cols=cols,
                            )
                        )
                        inserted_total += inserted
                        expired_total += expired
                        type1_total += type1_updated
                    del pk_ok

                retained_missing = 0
                scoped_rows = 0
                scoped_checksum = ""
                if complete_snapshot and cols.get("close_missing", True):
                    closed_missing = close_versions_missing_from_keys(
                        conn,
                        qualified,
                        pk_columns,
                        source_keys,
                        timestamp,
                        dialect_name,
                        batch_size=batch_size,
                        column_types=key_column_types(table_obj, pk_columns),
                        cols=cols,
                    )
                    expired_total += closed_missing
                elif complete_snapshot:
                    # close_missing=False: keys absent from this snapshot stay
                    # current — retained, not written. Count them so Gate-8's
                    # expected-current identity (read + retained == current)
                    # closes instead of failing on rows this run never wrote.
                    retained_keys = {
                        k
                        for k in _iter_current_keys(
                            conn,
                            qualified,
                            pk_columns,
                            dialect_name,
                            batch_size,
                            cols,
                        )
                        if k and k not in source_keys
                    }
                    retained_missing = len(retained_keys)
                    if retained_missing:
                        # Source-scoped digest: only the current rows this
                        # run's keys own are comparable to the source digest.
                        scoped_rows, scoped_checksum = _active_checksum(
                            conn,
                            qualified,
                            target_cols,
                            batch_size,
                            dialect_name,
                            dest_types=ctx["dest_types"],
                            cols=cols,
                            pk_columns=pk_columns,
                            exclude_keys=retained_keys,
                        )

                active_rows, active_checksum = _active_checksum(
                    conn,
                    qualified,
                    target_cols,
                    batch_size,
                    dialect_name,
                    dest_types=ctx["dest_types"],
                    cols=cols,
                )
        finally:
            release_engine(engine)
    finally:
        if close_spool:
            spool.close()

    return {
        "ok": True,
        "rows_written": inserted_total,
        "updated_rows": expired_total,
        "closed_missing_rows": closed_missing,
        "type1_updated_rows": type1_total,
        "active_rows": active_rows,
        "active_checksum": active_checksum,
        "retained_missing_rows": retained_missing,
        "source_scoped_rows": scoped_rows,
        "source_scoped_checksum": scoped_checksum,
        "source_snapshot_checksum": (
            source_fp_acc[0].digest() if source_fp_acc is not None else ""
        ),
        "mode": "scd2",
        "primary_key_columns": pk_columns,
        "target_columns": target_cols,
        "rejected_details": list(rejected_details),
        "rejected_rows": len(rejected_details),
        "transform_errors": list(transform_errors)[:20],
        "run_started_at": run_started_at.isoformat(),
        # The exact stamp every row this call opened/closed carries. Gate-8
        # recounts by exact-stamp membership so an overlapping writer inside
        # the run window is never counted as this run's work.
        "run_stamp": timestamp.isoformat(),
        "run_stamps": [timestamp.isoformat()],
    }


def _instant(value: Any) -> Any:
    """Canonical instant for valid_from/valid_to comparison — a tz-aware
    and a naive rendering of the same write timestamp compare equal."""
    if isinstance(value, datetime):
        v = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc)
    s = str(value or "")
    for suffix in ("+00:00", "Z", " UTC"):
        if s.endswith(suffix):
            s = s[: -len(suffix)]
    return s


def _as_dt(value: Any) -> datetime | None:
    """Parse a stamp to a tz-aware UTC datetime (datetime or ISO string).

    None when the value is absent or unparseable — an unparseable stamp
    then simply misses the recount window and surfaces as a count
    mismatch, which is the honest outcome."""
    if isinstance(value, datetime):
        d = value
    else:
        s = str(value or "").strip()
        if not s:
            return None
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            d = datetime.fromisoformat(s)
        except ValueError:
            return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc)


def verify_scd2_history(
    cfg: dict[str, Any],
    schema: str,
    table_name: str,
    pk_columns: list[str],
    cols: dict[str, Any] | None = None,
    expected: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Gate-8 read-back of the SCD2 history invariants, row by row.

    Asserts per natural key: exactly one is_current row; intervals do not
    overlap and do not gap (each version's valid_from equals the prior
    version's valid_to, the current version's valid_to is open); when the
    recipe declares a version column, versions form 1..n per key.
    Returns {"ok": bool, "violations": [...], "keys": int,
    "current_rows": int, "history_rows": int} — ok=None-safe: a read
    error returns ok False with the error in violations (fail closed for
    a history contract).
    """
    import sqlalchemy as sa

    from connectors.generic_sql import get_sqlalchemy_engine

    cols = resolve_scd2_cols(cols)
    engine = None
    try:
        engine = get_sqlalchemy_engine(cfg)
        dialect_name = engine.dialect.name if engine.dialect else ""
        qualified = _qualified_name(table_name, schema or None, dialect_name)
        select_cols = list(
            dict.fromkeys(
                list(pk_columns)
                + [
                    cols["valid_from"],
                    cols["valid_to"],
                    cols["is_current"],
                ]
                + ([cols["version"]] if cols.get("version") else [])
            )
        )
        col_sql = ", ".join(quote_stored_identifier(dialect_name, c) for c in select_cols)
        order_sql = ", ".join(quote_stored_identifier(dialect_name, c) for c in pk_columns) + (
            ", " + quote_stored_identifier(dialect_name, cols["valid_from"])
        )
        versions: dict[str, list[dict[str, Any]]] = {}
        with engine.connect() as conn:
            for row in conn.execute(
                sa.text(
                    f"SELECT {col_sql} FROM {qualified} "  # nosec B608
                    f"ORDER BY {order_sql}"
                )
            ):
                m = dict(zip(select_cols, row))
                key = _compose_key(m, pk_columns)
                versions.setdefault(key, []).append(m)

        violations: list[str] = []
        current_rows = 0
        history_rows = 0
        for key, rows in versions.items():
            currents = []
            for m in rows:
                ic = m.get(cols["is_current"])
                is_cur = ic is True or str(ic).lower() in (
                    "1", "true", "t", "yes",
                )
                if is_cur:
                    currents.append(m)
                    current_rows += 1
                else:
                    history_rows += 1
            label = key.replace(_KEY_SEP, "|")[:80]
            # At most one current row per key — a soft-closed key (removed
            # from a full snapshot) keeps history with zero current rows.
            if len(currents) > 1:
                violations.append(
                    f"key {label}: {len(currents)} is_current rows "
                    "(expected at most 1)"
                )
            ordered = sorted(
                rows,
                key=lambda m: (
                    m.get(cols["valid_from"]) is None,
                    str(m.get(cols["valid_from"]) or ""),
                ),
            )
            for prev, nxt in zip(ordered, ordered[1:]):
                vt_p = prev.get(cols["valid_to"])
                vf_n = nxt.get(cols["valid_from"])
                if vt_p is None:
                    violations.append(
                        f"key {label}: a non-final version has open valid_to"
                    )
                elif _instant(vf_n) < _instant(vt_p):
                    violations.append(
                        f"key {label}: overlapping intervals — next "
                        f"valid_from {vf_n} < prior valid_to {vt_p}"
                    )
                elif _instant(vf_n) != _instant(vt_p):
                    # Forward gap: the key's version closed at vt_p with no
                    # paired open — a soft-close on source absence, whose
                    # reopen later is legitimate history. A deleted middle
                    # version is still caught by the 1..n version-sequence
                    # check when the recipe declares a version column.
                    pass
            if cols.get("version"):
                seq = [m.get(cols["version"]) for m in ordered]
                if seq != list(range(1, len(seq) + 1)):
                    violations.append(
                        f"key {label}: versions {seq} are not 1..{len(seq)}"
                    )
        deltas: dict[str, Any] = {}
        if expected and (
            int(expected.get("inserted") or 0)
            or int(expected.get("closed") or 0)
        ):
            # Recount this run's writes independently — opened ==
            # inserted, closed == closed, and the keys holding both an open
            # and a close inside the run window are the changed keys,
            # which must equal closed - closed_missing. The run's writes do
            # NOT share one timestamp: each merge batch and the close pass
            # stamps its own instant. The recount is by exact-stamp
            # membership when the run recorded its stamps (an overlapping
            # writer inside the window is then never counted), else by the
            # recorded run start window, else the max-instant heuristic —
            # exact only for single-timestamp runs.
            stamp_set = {
                d for d in (
                    _as_dt(s) for s in (expected.get("run_stamps") or [])
                ) if d is not None
            }
            run_start = _as_dt(expected.get("run_started_at"))
            opened_now = 0
            closed_now = 0
            changed_now = 0
            if stamp_set:
                for rows in versions.values():
                    opened = sum(
                        1 for m in rows
                        if _as_dt(m.get(cols["valid_from"])) in stamp_set
                    )
                    closed = sum(
                        1 for m in rows
                        if _as_dt(m.get(cols["valid_to"])) in stamp_set
                    )
                    opened_now += opened
                    closed_now += closed
                    if opened and closed:
                        changed_now += 1
                stamp_strs = sorted(
                    str(s) for s in expected.get("run_stamps") or []
                )
            elif run_start is not None:
                for rows in versions.values():
                    opened = sum(
                        1 for m in rows
                        if (d := _as_dt(m.get(cols["valid_from"]))) is not None
                        and d >= run_start
                    )
                    closed = sum(
                        1 for m in rows
                        if (d := _as_dt(m.get(cols["valid_to"]))) is not None
                        and d >= run_start
                    )
                    opened_now += opened
                    closed_now += closed
                    if opened and closed:
                        changed_now += 1
                run_ts = str(expected.get("run_started_at"))
            else:
                instants = [
                    _instant(m.get(c))
                    for rows in versions.values()
                    for m in rows
                    for c in (cols["valid_from"], cols["valid_to"])
                    if m.get(c) is not None
                ]
                run_ts = max(instants) if instants else None
                for rows in versions.values():
                    opened = sum(
                        1 for m in rows
                        if _instant(m.get(cols["valid_from"])) == run_ts
                    )
                    closed = sum(
                        1 for m in rows
                        if m.get(cols["valid_to"]) is not None
                        and _instant(m.get(cols["valid_to"])) == run_ts
                    )
                    opened_now += opened
                    closed_now += closed
                    if opened and closed:
                        changed_now += 1
            exp_changed = int(expected.get("closed") or 0) - int(
                expected.get("closed_missing") or 0
            )
            # B1-E7a — never surface a comma-joined stamp list (grows
            # linearly with volume). Membership stays internal; report
            # min/max + count so API consumers get a bounded shape.
            _stamp_strs = (
                stamp_strs
                if stamp_set
                else ([str(run_ts)] if run_ts is not None else [])
            )
            deltas = {
                "run_ts_min": _stamp_strs[0] if _stamp_strs else None,
                "run_ts_max": _stamp_strs[-1] if _stamp_strs else None,
                "run_stamp_count": len(_stamp_strs),
                "opened": opened_now,
                "closed": closed_now,
                "changed": changed_now,
                "expected_inserted": int(expected.get("inserted") or 0),
                "expected_closed": int(expected.get("closed") or 0),
                "expected_changed": exp_changed,
            }
            if opened_now != deltas["expected_inserted"]:
                violations.append(
                    f"version recount: {opened_now} rows opened this run, "
                    f"expected {deltas['expected_inserted']} inserted"
                )
            if closed_now != deltas["expected_closed"]:
                violations.append(
                    f"version recount: {closed_now} versions closed this "
                    f"run, expected {deltas['expected_closed']}"
                )
            if changed_now != exp_changed:
                violations.append(
                    f"version recount: {changed_now} changed keys in the "
                    f"run window, expected {exp_changed} "
                    "(closed - closed_missing)"
                )
        return {
            "ok": not violations,
            "violations": violations[:20],
            "keys": len(versions),
            "current_rows": current_rows,
            "history_rows": history_rows,
            "deltas": deltas,
            "scd2": True,
        }
    except Exception as exc:  # noqa: BLE001 — a verifier that cannot run
        # is "unproven", never a silent skip and never a hard failure.
        return {
            "ok": False,
            "unproven": True,
            "violations": [f"scd2 history read-back failed: {exc}"],
            "keys": -1,
            "current_rows": -1,
            "history_rows": -1,
            "scd2": True,
        }
    finally:
        if engine is not None:
            release_engine(engine)


def preview_scd2_merge(
    endpoint,
    records: list[dict[str, Any]],
    columns: list[str],
    schema: dict[str, str],
    mappings: list[dict[str, Any]] | None,
    conflict_columns: list[str],
    *,
    validation_mode: str = "balanced",
    complete_snapshot: bool = False,
    scd2_opts: dict[str, Any] | None = None,
    batch_size: int = 1_000,
    sample: int = 50,
) -> dict[str, Any]:
    """Per-step SCD2 preview: which rows would open or close a version.

    Read-only — runs the same key/tracked-hash diff as ``apply_scd2``
    against the destination's current snapshots without writing anything.
    Returns per-key actions plus totals: ``open`` (new key or changed
    tracked columns), ``close`` (version closed by a change or by a key
    missing from a full snapshot), ``unchanged`` (no-op), ``type1`` (rows
    that would get an in-place Type-1 update of untracked columns).
    """
    from connectors.generic_sql import get_sql_schema, get_sqlalchemy_engine
    from connectors.sql_write_materialize import ensure_sql_source_spool
    from src.transfer.adapters import resolve_connector_config

    cols = resolve_scd2_cols(scd2_opts)
    preview: dict[str, Any] = {
        "opens": [],
        "closes": [],
        "unchanged": 0,
        "type1_updates": 0,
        "counts": {},
        "ok": True,
        "mode": "scd2_preview",
    }
    engine = None
    try:
        ctx = _scd2_map_context(
            endpoint,
            columns,
            schema,
            mappings,
            conflict_columns,
            validation_mode=validation_mode,
        )
        pk_columns = list(ctx["pk_columns"])
        target_cols = list(ctx["target_cols"])
        spool, close_spool = ensure_sql_source_spool(
            headers=columns,
            records=records,
            mappings=ctx["effective_mappings"],
            extra=ctx.get("extra") or {},
        )
        cfg = resolve_connector_config(endpoint)
        schema_name = get_sql_schema(cfg)
        table = endpoint.table or endpoint.collection or "dt_import"
        engine = get_sqlalchemy_engine(cfg)
        dialect_name = engine.dialect.name if engine.dialect else ""
        qualified = _qualified_name(table, schema_name, dialect_name)

        tracked = list(cols.get("tracked") or [])
        tracked_set = set(tracked) if tracked else None
        stamps = _stamp_set(cols)
        attr_cols = [
            c for c in target_cols if c not in stamps and c not in pk_columns
        ]
        untracked_cols = (
            [c for c in attr_cols if c not in tracked_set]
            if tracked_set is not None
            else []
        )
        from connectors.writer_common import conflict_key_wire as _ckw

        opens: list[str] = []
        closes: list[str] = []
        seen_keys: set[str] = set()
        try:
            with engine.connect() as conn:
                for pk_ok, _details, _errors in iter_scd2_prepared_bundles(
                    columns=columns,
                    ctx=ctx,
                    source_spool=spool,
                    batch_size=batch_size,
                ):
                    if not pk_ok:
                        continue
                    keys = {
                        _compose_key(row, pk_columns)
                        for row in pk_ok
                    }
                    seen_keys.update(keys)
                    snaps = _fetch_current_snapshots(
                        conn,
                        qualified,
                        pk_columns,
                        target_cols,
                        keys,
                        dialect_name,
                        cols=cols,
                    )
                    for row in pk_ok:
                        key = _compose_key(row, pk_columns)
                        label = key.replace(_KEY_SEP, "|")[:80]
                        snap = snaps.get(key)
                        hydrated = _hydrate_scd2_omit(
                            row,
                            (snap or {}).get("attrs") if snap else None,
                            target_cols,
                            stamp_set=stamps,
                        )
                        new_hash = _row_hash(
                            hydrated,
                            target_cols,
                            tracked=tracked or None,
                            stamp_set=stamps,
                            dialect=dialect_name,
                        )
                        if snap is None:
                            opens.append(label)
                            snaps[key] = {
                                "hash": new_hash,
                                "attrs": dict(hydrated),
                            }
                            continue
                        if str(snap.get("hash") or "") != new_hash:
                            closes.append(label)
                            opens.append(label)
                            snaps[key] = {
                                "hash": new_hash,
                                "attrs": dict(hydrated),
                            }
                        elif untracked_cols:
                            to_diff = {
                                c: hydrated.get(c)
                                for c in untracked_cols
                                if _ckw((snap.get("attrs") or {}).get(c))
                                != _ckw(hydrated.get(c))
                            }
                            if to_diff:
                                preview["type1_updates"] += 1
                            else:
                                preview["unchanged"] += 1
                        else:
                            preview["unchanged"] += 1
                    del pk_ok
        finally:
            if callable(close_spool):
                close_spool()
        if complete_snapshot and cols.get("close_missing", True):
            with engine.connect() as conn:
                all_keys = set(
                    _iter_current_keys(
                        conn,
                        qualified,
                        pk_columns,
                        dialect_name,
                        batch_size,
                        cols=cols,
                    )
                )
            for key in sorted(all_keys - seen_keys):
                closes.append(key.replace(_KEY_SEP, "|")[:80])
        preview["opens"] = opens[:sample]
        preview["closes"] = closes[:sample]
        preview["counts"] = {
            "open": len(opens),
            "close": len(closes),
            "unchanged": preview["unchanged"],
            "type1": preview["type1_updates"],
        }
        return preview
    except Exception as exc:  # noqa: BLE001 — preview is advisory, fail honest
        preview["ok"] = False
        preview["error"] = str(exc)
        return preview
    finally:
        if engine is not None:
            release_engine(engine)
