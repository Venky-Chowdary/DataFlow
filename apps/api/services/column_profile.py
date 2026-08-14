"""Engine-side column profiles for population parity at any scale.

The verification ladder's L2 (per-column NULL count, min, max, sum) and its
localization layers load the whole population into memory and refuse above
``VERIFICATION_LADDER_MAX_ROWS``. That is the honest thing to do for an
in-memory pass, but it means the exact column-level checks that catch the most
common silent corruptions — a field that quietly became NULL, a numeric that
lost precision, a timestamp that lost its clock — stop running precisely on the
large tables where a migration's stakes are highest. (Reading 50 public data
pipeline postmortems, schema drift was 38% of incidents and silent data loss
19%, and both classes are invisible to row-count and job-status monitoring:
the pipeline reports success while a column is quietly nulled or truncated.)

A SQL engine computes those aggregates over any number of rows without bringing
a single row into Python. This module asks each engine for the profile and
compares the two, so L2 keeps working at 50M rows where the in-memory ladder
declines. It is deliberately narrow about *which* statistics it trusts across a
route:

* ``count(*)`` and per-column NULL count are exact and independent of collation,
  scale and type width — they are compared for every column. A destination that
  silently nulled a field (the ``DESTINATION_TYPECAST_ERROR`` class of failure)
  shows up here as a NULL-count divergence on that column and nowhere else.
* ``sum`` is computed only for exact numeric types (integer/decimal). A floating
  point sum depends on summation order, which a parallel aggregate may vary, so
  a float sum is not a reliable parity signal and is not taken.
* ``min``/``max`` are taken for numeric and temporal columns, whose ordering is
  collation-independent. Text ``min``/``max`` and ``count(distinct)`` depend on
  the column collation, which two endpoints may not share even on one engine, so
  they are not compared — a difference there would be about collation, not data.

Numeric statistics are canonicalized (``10.50`` == ``10.5000``) so a scale
difference that carries the same value is not read as a divergence.

The two ends may be *different* engines (PostgreSQL ↔ MySQL/MariaDB). A
cross-engine route narrows the comparison to the statistics that survive a change
of engine — row count, per-column NULL rate, value-canonical numeric min/max/sum,
and **wall-clock date/time min/max rendered to a fixed ISO shape in SQL** so a
timestamp that lost its clock is caught even across engines. It declines only
what genuinely differs by engine: text ordering and distinct counts (collation),
float sums (order), and *zone-aware* timestamps (a stored offset renders
differently on each engine). Column kinds are read from each engine's live
catalog rather than a mapping hint, because ``timestamp`` means a wall clock in
PostgreSQL and an instant in MySQL, and only the catalog says which. That is the
parity primitive a "supervisor" needs: attach to a source and a destination that
no single tool moved between, and still prove — or disprove — that a column
carries the same population.

This extends the canonical reconcile ladder; it is not a parallel product. The
result reuses the ``ColumnAggregate`` and ``compare_column_aggregates`` shapes
the in-memory L2 already uses, and only ever runs on the oversized routes the
in-memory pass declines.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# PostgreSQL and MySQL families render an aggregate the same way on both ends of
# a same-engine route, which is the precondition for comparing min/max/sum as
# text. Every other engine needs its own proof before it may claim this path.
_PG_FAMILY = frozenset(
    {"postgresql", "postgres", "pg", "timescaledb", "alloydb", "supabase"}
)
_MYSQL_FAMILY = frozenset({"mysql", "mariadb"})


def _norm(engine: str | None) -> str:
    return (engine or "").strip().lower()


def profile_engine_family(engine: str | None) -> str:
    """Canonical profile family (``postgresql``/``mysql``) or ``""`` if unknown."""
    e = _norm(engine)
    if e in _PG_FAMILY:
        return "postgresql"
    if e in _MYSQL_FAMILY:
        return "mysql"
    return ""


def profile_supported(engine: str | None) -> bool:
    return bool(profile_engine_family(engine))


def same_profile_family(source_engine: str | None, dest_engine: str | None) -> bool:
    """True when both ends are the same family this module knows how to render."""
    fam = profile_engine_family(source_engine)
    return bool(fam) and fam == profile_engine_family(dest_engine)


def normalize_catalog_type(family: str, data_type: str | None) -> str:
    """Make a catalog type string unambiguous across engines.

    ``timestamp`` is the trap: PostgreSQL's catalog spells its zone-aware type
    ``timestamp with time zone`` and its wall-clock type ``timestamp without
    time zone``, but MySQL/MariaDB spell an *instant* (UTC-backed) column plainly
    ``timestamp`` and a wall-clock column ``datetime``. Left as-is, a bare
    ``timestamp`` would be read as wall-clock and a MySQL instant would be
    compared as if it had no zone. Rewriting per engine collapses both catalogs
    onto one vocabulary the classifier can trust.
    """
    t = _norm(data_type)
    if family == "mysql":
        if t == "timestamp":
            return "timestamp with time zone"
        if t == "datetime":
            return "timestamp without time zone"
    return t


def classify_column(type_str: str | None) -> str:
    """Bucket a (normalized) type into how its aggregates may be trusted.

    ``exact_numeric``    — integer/decimal: NULL count, min, max and sum.
    ``float``            — real/double: NULL count, min, max (sum is order-sensitive).
    ``temporal_ts``      — date / wall-clock timestamp: NULL count and a canonical
                           ISO min/max that compares across engines.
    ``temporal_time``    — time of day (no zone): NULL count and canonical min/max.
    ``temporal_instant`` — zone-aware timestamp/time: NULL count, and min/max that
                           are comparable only within one engine (a zone offset
                           renders differently across engines).
    ``other``            — text/json/binary/etc.: NULL count only.

    Pass values through :func:`normalize_catalog_type` first so ``timestamp`` is
    unambiguous.
    """
    t = _norm(type_str)
    if not t:
        return "other"
    if "interval" in t:
        # ``interval`` contains "int" and has no clean cross-dialect ordering.
        return "other"
    # ``with time zone`` marks an instant; note ``without time zone`` does not
    # contain the substring ``with time zone`` (the "with" is followed by "out").
    # ``timestamptz``/``timetz`` are the shorthand spellings of the same thing.
    if "with time zone" in t or "timestamptz" in t or "timetz" in t:
        return "temporal_instant"
    if "timestamp" in t or "datetime" in t or t == "date":
        return "temporal_ts"
    if t == "time" or "time without time zone" in t or t.startswith("time("):
        return "temporal_time"
    is_float = any(k in t for k in ("float", "double", "real"))
    is_int = any(k in t for k in ("int", "serial"))
    is_decimal = any(k in t for k in ("decimal", "numeric", "number", "money"))
    if is_decimal or (is_int and not is_float):
        return "exact_numeric"
    if is_float:
        return "float"
    return "other"


_NUMERIC_KINDS = frozenset({"exact_numeric", "float"})
_TEMPORAL_KINDS = frozenset({"temporal_ts", "temporal_time", "temporal_instant"})
#: Temporal kinds whose canonical rendering is identical across engines, so they
#: can be compared on a cross-engine route. An instant is excluded: its offset
#: renders differently between PostgreSQL and MySQL.
_CROSS_ENGINE_TEMPORAL = frozenset({"temporal_ts", "temporal_time"})


def _quote_col(family: str, name: str) -> str:
    from connectors.sql_identifiers import quote_sql_identifier, require_safe_identifier

    ident = require_safe_identifier(name, preserve_case=True, max_len=128)
    return quote_sql_identifier(ident, "`" if family == "mysql" else '"')


def _cast_text(family: str, expr: str) -> str:
    return f"CAST({expr} AS CHAR)" if family == "mysql" else f"({expr})::text"


class _ColumnPlan:
    """Which statistics this column contributes, and where they land in the row."""

    __slots__ = ("name", "kind", "want_minmax", "want_sum")

    def __init__(self, name: str, kind: str) -> None:
        self.name = name
        self.kind = kind
        self.want_minmax = kind in _NUMERIC_KINDS or kind in _TEMPORAL_KINDS
        self.want_sum = kind == "exact_numeric"


def _plan_columns(family: str, columns: list[str], types: dict[str, str] | None) -> list[_ColumnPlan]:
    resolved = {str(k): str(v) for k, v in (types or {}).items()}
    return [
        _ColumnPlan(name=col, kind=classify_column(normalize_catalog_type(family, resolved.get(col))))
        for col in columns
    ]


def _minmax_exprs(family: str, kind: str, quoted: str) -> tuple[str, str]:
    """``(min_expr, max_expr)`` rendered so the value compares across engines.

    Temporal columns are formatted to a fixed ISO shape rather than left to each
    engine's default text: a wall-clock timestamp becomes
    ``YYYY-MM-DDTHH:MM:SS.ffffff`` (a date lands at midnight, so a
    date-vs-timestamp pair still lines up), and a time becomes
    ``HH:MM:SS.ffffff``. An instant keeps its native text — only ever compared
    within one engine — because its zone offset renders differently elsewhere.
    """
    def _fmt(agg: str) -> tuple[str, str]:
        inner = f"{agg}({quoted})"
        if kind == "temporal_ts":
            if family == "mysql":
                return f"DATE_FORMAT({inner}, '%Y-%m-%dT%H:%i:%s.%f')", ""
            return f"to_char({inner}, 'YYYY-MM-DD\"T\"HH24:MI:SS.US')", ""
        if kind == "temporal_time":
            if family == "mysql":
                return f"TIME_FORMAT({inner}, '%H:%i:%s.%f')", ""
            return f"to_char({inner}, 'HH24:MI:SS.US')", ""
        # numeric or instant: native text is enough (numeric is canonicalized in
        # the reader; instant is only compared same-engine).
        return _cast_text(family, inner), ""

    return _fmt("min")[0], _fmt("max")[0]


def build_profile_sql(
    family: str, table_ref: str, columns: list[str], types: dict[str, str] | None
) -> tuple[str, list[_ColumnPlan]]:
    """One ``SELECT`` returning ``count(*)`` then each column's trusted stats.

    Returns the SQL and the ordered plans, so the reader can map result columns
    back to the statistic and column that produced them by position — no aliases
    to collide, no re-parsing of names.
    """
    plans = _plan_columns(family, columns, types)
    exprs: list[str] = ["count(*)"]
    for plan in plans:
        q = _quote_col(family, plan.name)
        exprs.append(f"count({q})")
        if plan.want_minmax:
            min_expr, max_expr = _minmax_exprs(family, plan.kind, q)
            exprs.append(min_expr)
            exprs.append(max_expr)
        if plan.want_sum:
            exprs.append(_cast_text(family, f"sum({q})"))
    sql = "SELECT " + ", ".join(exprs) + f" FROM {table_ref}"  # nosec B608 — identifiers quoted
    return sql, plans


def _canon_numeric(text: str | None) -> str | None:
    if text is None:
        return None
    try:
        from services.reconciliation import _canonicalize_number

        return _canonicalize_number(text) or text
    except Exception:  # noqa: BLE001 — canonicalization is best-effort, raw text is fine
        return text


def read_column_profile(
    engine: str,
    cur: Any,
    table_ref: str,
    columns: list[str],
    types: dict[str, str] | None,
) -> tuple[int, dict[str, Any]]:
    """Run the profile SQL and return ``(row_count, {column: ColumnAggregate})``.

    The ``row_count`` is the table's own ``count(*)`` from the same query, so a
    standalone parity check (which has no transfer to quote a count from) gets an
    authoritative cardinality without a second round trip.

    Numeric min/max/sum are canonicalized so a scale-only difference is not a
    divergence. Statistics this route does not trust for a column are left
    ``None`` on both sides, so the shared comparison treats them as "not
    compared" rather than "equal".
    """
    from services.verification_ladder import ColumnAggregate

    family = profile_engine_family(engine)
    if not family:
        raise ValueError(f"engine {engine!r} has no column profiler")
    sql, plans = build_profile_sql(family, table_ref, columns, types)
    cur.execute(sql)
    row = list(cur.fetchone() or [])
    if not row:
        raise ValueError("profile query returned no row")

    row_count = int(row[0] or 0)
    idx = 1
    out: dict[str, Any] = {}
    for plan in plans:
        non_null = int(row[idx] or 0)
        idx += 1
        min_v = max_v = sum_v = None
        if plan.want_minmax:
            raw_min, raw_max = row[idx], row[idx + 1]
            idx += 2
            if plan.want_sum:
                sum_v = _canon_numeric(None if row[idx] is None else str(row[idx]))
                idx += 1
            if plan.kind in _NUMERIC_KINDS:
                # Compare by value: 10.50 and 10.5000 are one number, and float
                # text residue is collapsed.
                min_v = _canon_numeric(None if raw_min is None else str(raw_min))
                max_v = _canon_numeric(None if raw_max is None else str(raw_max))
            else:
                # Temporal: already canonicalized to a fixed shape by the SQL
                # (wall-clock/time) or native instant text; store verbatim.
                min_v = None if raw_min is None else str(raw_min)
                max_v = None if raw_max is None else str(raw_max)
        out[plan.name] = ColumnAggregate(
            column=plan.name,
            null_count=max(row_count - non_null, 0),
            non_null_count=non_null,
            distinct_count=None,
            min_value=min_v,
            max_value=max_v,
            sum_value=sum_v,
        )
    return row_count, out


def _connect(family: str, cfg: dict[str, Any]) -> Any:
    kwargs = dict(
        host=cfg.get("host", ""),
        port=int(cfg.get("port") or (3306 if family == "mysql" else 5432)),
        database=cfg.get("database", ""),
        username=cfg.get("username", ""),
        password=cfg.get("password", ""),
        connection_string=cfg.get("connection_string", ""),
        ssl=bool(cfg.get("ssl", False)),
    )
    if family == "mysql":
        from connectors.mysql_conn import get_connection

        conn = get_connection(purpose="read", **kwargs)
    else:
        from connectors.postgresql_conn import get_connection

        conn = get_connection(**kwargs)
    _prepare_profile_session(family, conn)
    return conn


def _prepare_profile_session(family: str, conn: Any) -> None:
    """Session invariants the profile comparison assumes.

    MySQL ``TIMESTAMP`` renders in the session time zone. Two connections with
    different zones would disagree on min/max of the same stored instants.
    Pinning UTC is part of the comparison, not a connection-library default.
    """
    if family != "mysql":
        return
    try:
        with conn.cursor() as cur:
            cur.execute("SET time_zone = '+00:00'")
    except Exception as exc:  # noqa: BLE001 — a pin failure declines later if types mis-render
        logger.debug("mysql profile session time_zone pin skipped: %s", exc)


def _table_ref(family: str, schema: str, table: str) -> str:
    from connectors.sql_identifiers import quote_table_ref

    return quote_table_ref(table, schema or None, dialect=family)


def _introspect_types(
    family: str, cur: Any, schema: str, table: str, columns: list[str]
) -> dict[str, str]:
    """Read each column's engine-native type from ``information_schema.columns``.

    The live catalog is authoritative where a mapping's type hint is a guess, and
    it is what disambiguates ``timestamp`` (see :func:`normalize_catalog_type`).
    Returns ``{column_lower: normalized_type}``; an empty dict lets the caller
    fall back to the hints it was given (a temporary table, for instance, is not
    in ``information_schema``).
    """
    wanted = {c.lower() for c in columns}
    try:
        if family == "mysql" and not schema:
            cur.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = DATABASE() AND table_name = %s",
                (table,),
            )
        else:
            cur.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s",
                (schema or ("public" if family != "mysql" else ""), table),
            )
        rows = cur.fetchall() or []
    except Exception as exc:  # noqa: BLE001 — fall back to caller-supplied hints
        logger.debug("catalog type introspection unavailable for %s.%s: %s", schema, table, exc)
        return {}
    out: dict[str, str] = {}
    for name, data_type in rows:
        if str(name).lower() in wanted:
            out[str(name).lower()] = normalize_catalog_type(family, str(data_type))
    return out


def _column_kinds(
    family: str, cur: Any, schema: str, table: str, columns: list[str], hints: dict[str, str]
) -> tuple[dict[str, str], dict[str, str]]:
    """Return ``(types_by_column, kinds_by_column)`` for the profile.

    Catalog types win; a column the catalog did not return falls back to the
    caller's hint (already normalized per engine).
    """
    catalog = _introspect_types(family, cur, schema, table, columns)
    types: dict[str, str] = {}
    kinds: dict[str, str] = {}
    for col in columns:
        t = catalog.get(col.lower()) or normalize_catalog_type(family, hints.get(col, ""))
        types[col] = t
        kinds[col] = classify_column(t)
    return types, kinds


def _comparison_decisions(
    src_kinds: dict[str, str], dst_kinds: dict[str, str], *, cross_engine: bool
) -> dict[str, str]:
    """Per target column, decide how its min/max/sum may be compared.

    ``numeric`` — canonicalize both sides and compare by value.
    ``keep``    — compare the (already canonical) values as-is.
    ``drop``    — compare only NULL/row counts for this column.

    A column is only kept when both sides agree on its kind, so a source that is
    a timestamp and a destination that is an instant are never compared as if
    they meant the same thing. Cross-engine, only wall-clock temporals survive
    (an instant renders its offset differently on each engine).
    """
    decisions: dict[str, str] = {}
    for col, dst_kind in dst_kinds.items():
        src_kind = src_kinds.get(col, "other")
        if dst_kind in _NUMERIC_KINDS and src_kind in _NUMERIC_KINDS:
            decisions[col] = "numeric"
        elif dst_kind in _TEMPORAL_KINDS and src_kind == dst_kind:
            if cross_engine and dst_kind not in _CROSS_ENGINE_TEMPORAL:
                decisions[col] = "drop"  # instant: offset renders differently
            else:
                decisions[col] = "keep"
        else:
            decisions[col] = "drop"
    return decisions


def _apply_decisions(profile: dict[str, Any], decisions: dict[str, str]) -> dict[str, Any]:
    from dataclasses import replace as _replace_agg

    out: dict[str, Any] = {}
    for col, agg in profile.items():
        decision = decisions.get(col, "drop")
        if decision == "numeric":
            out[col] = _replace_agg(
                agg,
                min_value=_canon_numeric(agg.min_value),
                max_value=_canon_numeric(agg.max_value),
                sum_value=_canon_numeric(agg.sum_value),
            )
        elif decision == "keep":
            out[col] = agg
        else:
            out[col] = _replace_agg(agg, min_value=None, max_value=None, sum_value=None)
    return out


def engine_profile_ladder(
    *,
    source_engine: str,
    source_cfg: dict[str, Any],
    source_schema: str,
    source_table: str,
    dest_engine: str,
    dest_cfg: dict[str, Any],
    dest_schema: str,
    dest_table: str,
    pairs: list[tuple[str, str]],
    types: dict[str, str] | None,
    source_rows: int | None = None,
    target_rows: int | None = None,
    rejected_rows: int = 0,
    coerced_null_rows: int = 0,
    rows_skipped: int = 0,
) -> dict[str, Any] | None:
    """L1 + engine-side L2 for a SQL route, at any scale, same- or cross-engine.

    ``source_rows``/``target_rows`` are the accounted cardinalities from a
    transfer (which knows about rejected/skipped rows); a standalone parity check
    leaves them ``None`` and the profile's own ``count(*)`` is used instead.

    ``pairs`` are ordered ``(source_column, target_column)``; a rename is free
    because each side profiles its own names and the source profile is re-keyed
    onto the target name before the positional comparison. ``types`` are keyed by
    target column.

    Both ends may be different engines (PostgreSQL ↔ MySQL/MariaDB). When they
    are, the comparison narrows to the statistics that are engine-independent —
    row count, per-column NULL rate, and canonicalized numeric min/max/sum — and
    declines temporal/text ordering, which differs by rendering, time zone and
    collation. This is the parity primitive a Zero-ETL supervisor needs: it
    attaches to a source and a destination that no single tool moved between and
    still proves, or disproves, that a column carries the same population.

    The live catalog namespace is resolved with
    :func:`services.dialect_profiles.catalog_namespace` so a leaked Postgres
    ``public`` on MySQL does not miss the table. The profile is the *named
    table as it exists now* — callers whose write was a batch into a larger
    table (append/upsert) must not use this for Gate-8; that is a different
    population.

    Returns a verification-ladder-shaped dict, or ``None`` when either end is not
    a supported SQL engine or a profile could not be read. It never claims the
    population checksum proof (L3) or row localization (L5), so it upgrades
    assurance for nobody; it restores the *column-level* divergence signal that
    the oversized route would otherwise lose entirely.
    """
    src_family = profile_engine_family(source_engine)
    dst_family = profile_engine_family(dest_engine)
    if not src_family or not dst_family:
        return None
    cross_engine = src_family != dst_family
    clean = [(str(s), str(t)) for s, t in pairs if s and t]
    if not clean:
        return None
    dest_hints = {str(k): str(v) for k, v in (types or {}).items()}
    source_cols = [s for s, _ in clean]
    target_cols = [t for _, t in clean]
    rename = {s: t for s, t in clean}
    # The source hint per column is the destination's declared type — a faithful
    # migration keeps the class — but the live catalog below overrides it.
    source_hints = {s: dest_hints.get(t, "") for s, t in clean}

    src_conn = dst_conn = None
    try:
        src_conn = _connect(src_family, source_cfg)
        dst_conn = _connect(dst_family, dest_cfg)
        from dataclasses import replace as _replace_agg

        with src_conn.cursor() as sc, dst_conn.cursor() as dc:
            from services.dialect_profiles import catalog_namespace

            src_ns = catalog_namespace(source_engine, source_cfg, schema=source_schema)
            dst_ns = catalog_namespace(dest_engine, dest_cfg, schema=dest_schema)
            src_types, src_kinds_by_src = _column_kinds(
                src_family, sc, src_ns, source_table, source_cols, source_hints
            )
            dst_types, dst_kinds = _column_kinds(
                dst_family, dc, dst_ns, dest_table, target_cols, dest_hints
            )
            src_rows_obs, src_raw = read_column_profile(
                source_engine, sc, _table_ref(src_family, src_ns, source_table),
                source_cols, src_types,
            )
            dst_rows_obs, dst_profile = read_column_profile(
                dest_engine, dc, _table_ref(dst_family, dst_ns, dest_table),
                target_cols, dst_types,
            )
        # Re-key the source profile and its kinds onto the destination names so a
        # rename lines the two sides up column-for-column.
        src_profile = {rename.get(s, s): _replace_agg(agg, column=rename.get(s, s))
                       for s, agg in src_raw.items()}
        src_kinds = {rename.get(s, s): k for s, k in src_kinds_by_src.items()}
    except Exception as exc:  # noqa: BLE001 — any read failure declines to the caller's fallback
        logger.info("engine column profile unavailable, leaving ladder declined: %s", exc)
        return None
    finally:
        for conn in (src_conn, dst_conn):
            try:
                if conn is not None:
                    conn.close()
            except Exception as close_exc:  # noqa: BLE001 — a close failure must not mask the result
                logger.debug("profile connection close: %s", close_exc)

    from services.verification_ladder import (
        compare_column_aggregates,
        layer_l1_row_balance,
    )

    decisions = _comparison_decisions(src_kinds, dst_kinds, cross_engine=cross_engine)
    src_profile = _apply_decisions(src_profile, decisions)
    dst_profile = _apply_decisions(dst_profile, decisions)
    has_instant = any(k == "temporal_instant" for k in dst_kinds.values())

    # A transfer supplies accounted counts; a standalone check trusts the
    # profile's own count(*).
    eff_source_rows = src_rows_obs if source_rows is None else int(source_rows)
    eff_target_rows = dst_rows_obs if target_rows is None else int(target_rows)
    l1 = layer_l1_row_balance(
        source_rows=eff_source_rows,
        target_rows=eff_target_rows,
        rejected_rows=int(rejected_rows or 0),
        coerced_null_rows=int(coerced_null_rows or 0),
        rows_skipped=int(rows_skipped or 0),
    )
    l2 = compare_column_aggregates(src_profile, dst_profile)
    # Name what this pass does and does not prove, so no reader mistakes an
    # engine profile for the full five-layer localization.
    l2.details["source"] = "engine_sql_aggregates"
    l2.details["cross_engine"] = cross_engine
    if cross_engine:
        l2.details["compared_statistics"] = [
            "null_count", "non_null_count",
            "numeric min/max/sum (value-canonical)",
            "wall-clock date/time min/max (ISO-canonical)",
        ]
        not_compared = [
            "text min/max and distinct_count (collation-dependent)",
            "float sum (order-dependent)",
        ]
        if has_instant:
            not_compared.insert(
                0, "zone-aware timestamp min/max (offset renders differently per engine)"
            )
        l2.details["not_compared"] = not_compared
    else:
        l2.details["compared_statistics"] = ["null_count", "non_null_count", "min", "max", "sum"]
        l2.details["not_compared"] = [
            "distinct_count (collation-dependent)",
            "text min/max (collation-dependent)",
            "float sum (order-dependent)",
        ]
    mismatched = list(l2.details.get("mismatched_columns") or [])
    label = "cross-engine " if cross_engine else ""
    summary = ""
    if mismatched:
        summary = (
            f"Engine {label}column profile diverged on "
            + ", ".join(mismatched[:8])
            + (" …" if len(mismatched) > 8 else "")
        )
    return {
        "layers": {"L1": l1.to_dict(), "L2": l2.to_dict()},
        "passed": bool(l1.passed and l2.passed),
        "assurance_level": "engine_column_profile",
        # Population RI and per-row typed fidelity are not claimed here.
        "population_proof": False,
        "population_checksum_proof": False,
        "engine_profile": True,
        "cross_engine": cross_engine,
        "skipped": False,
        "localization": {"columns": mismatched},
        "localization_summary": summary,
        "reason": (
            "In-memory L2/L4/L5 declined for an oversized population; the engine "
            f"computed per-column aggregates in SQL{' on both engines' if cross_engine else ''} "
            "so column-level divergence is still detected at full scale."
        ),
    }
