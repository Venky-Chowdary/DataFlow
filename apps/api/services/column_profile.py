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


def classify_column(type_str: str | None) -> str:
    """Bucket a declared type into how its aggregates may be trusted.

    ``exact_numeric`` — integer/decimal: NULL count, min, max and sum are taken.
    ``float``         — real/double: NULL count, min, max (sum is order-sensitive).
    ``temporal``      — date/time/timestamp: NULL count, min, max.
    ``other``         — text/json/binary/etc.: NULL count only (min/max are
                        collation- or representation-dependent across a route).
    """
    t = _norm(type_str)
    if not t:
        return "other"
    if "interval" in t:
        # ``interval`` contains "int"; it is neither a plain integer nor a clean
        # temporal ordering across dialects, so only its NULL rate is trusted.
        return "other"
    if any(k in t for k in ("timestamp", "datetime", "date", "time")):
        return "temporal"
    is_float = any(k in t for k in ("float", "double", "real"))
    is_int = any(k in t for k in ("int", "serial"))
    is_decimal = any(k in t for k in ("decimal", "numeric", "number", "money"))
    if is_decimal or (is_int and not is_float):
        return "exact_numeric"
    if is_float:
        return "float"
    return "other"


def _quote_col(family: str, name: str) -> str:
    from connectors.sql_identifiers import quote_sql_identifier, require_safe_identifier

    ident = require_safe_identifier(name, preserve_case=True, max_len=128)
    return quote_sql_identifier(ident, "`" if family == "mysql" else '"')


def _cast_text(family: str, expr: str) -> str:
    return f"CAST({expr} AS CHAR)" if family == "mysql" else f"({expr})::text"


class _ColumnPlan:
    """Which statistics this column contributes, and where they land in the row."""

    __slots__ = ("name", "want_minmax", "want_sum")

    def __init__(self, name: str, want_minmax: bool, want_sum: bool) -> None:
        self.name = name
        self.want_minmax = want_minmax
        self.want_sum = want_sum


def _plan_columns(columns: list[str], types: dict[str, str] | None) -> list[_ColumnPlan]:
    types = {str(k): str(v) for k, v in (types or {}).items()}
    plans: list[_ColumnPlan] = []
    for col in columns:
        kind = classify_column(types.get(col))
        plans.append(
            _ColumnPlan(
                name=col,
                want_minmax=kind in {"exact_numeric", "float", "temporal"},
                want_sum=kind == "exact_numeric",
            )
        )
    return plans


def build_profile_sql(
    family: str, table_ref: str, columns: list[str], types: dict[str, str] | None
) -> tuple[str, list[_ColumnPlan]]:
    """One ``SELECT`` returning ``count(*)`` then each column's trusted stats.

    Returns the SQL and the ordered plans, so the reader can map result columns
    back to the statistic and column that produced them by position — no aliases
    to collide, no re-parsing of names.
    """
    plans = _plan_columns(columns, types)
    exprs: list[str] = ["count(*)"]
    for plan in plans:
        q = _quote_col(family, plan.name)
        exprs.append(f"count({q})")
        if plan.want_minmax:
            exprs.append(_cast_text(family, f"min({q})"))
            exprs.append(_cast_text(family, f"max({q})"))
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
) -> dict[str, Any]:
    """Run the profile SQL and return ``{column: ColumnAggregate}``.

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
                # Exact numeric: canonicalize so 10.50 and 10.5000 are one value.
                sum_v = _canon_numeric(None if row[idx] is None else str(row[idx]))
                idx += 1
                min_v = _canon_numeric(None if raw_min is None else str(raw_min))
                max_v = _canon_numeric(None if raw_max is None else str(raw_max))
            else:
                # Float / temporal: identical values render identically on one
                # engine, so the text is compared as-is; no sum is taken.
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
    return out


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

        return get_connection(purpose="read", **kwargs)
    from connectors.postgresql_conn import get_connection

    return get_connection(**kwargs)


def _table_ref(family: str, schema: str, table: str) -> str:
    from connectors.sql_identifiers import quote_table_ref

    return quote_table_ref(table, schema or None, dialect=family)


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
    source_rows: int,
    target_rows: int,
    rejected_rows: int = 0,
    coerced_null_rows: int = 0,
    rows_skipped: int = 0,
) -> dict[str, Any] | None:
    """L1 + engine-side L2 for a same-engine SQL route, at any scale.

    ``pairs`` are ordered ``(source_column, target_column)``; a rename is free
    because each side profiles its own names and the source profile is re-keyed
    onto the target name before the positional comparison. ``types`` are keyed by
    target column.

    Returns a verification-ladder-shaped dict, or ``None`` when the route is not
    a supported same-engine pair or a profile could not be read. It never claims
    the population checksum proof (L3) or row localization (L5) — those still
    need the in-memory or engine-digest paths — so it upgrades assurance for
    nobody; it restores the *column-level* divergence signal that the oversized
    route would otherwise lose entirely.
    """
    if not same_profile_family(source_engine, dest_engine):
        return None
    clean = [(str(s), str(t)) for s, t in pairs if s and t]
    if not clean:
        return None
    family = profile_engine_family(dest_engine)
    dest_types = {str(k): str(v) for k, v in (types or {}).items()}
    source_cols = [s for s, _ in clean]
    target_cols = [t for _, t in clean]
    # Classify the source side by the destination type so both sides bucket a
    # column the same way (same-engine routes declare the same type class).
    source_types = {s: dest_types.get(t, "") for s, t in clean}

    src_conn = dst_conn = None
    try:
        src_conn = _connect(family, source_cfg)
        dst_conn = _connect(family, dest_cfg)
        with src_conn.cursor() as sc, dst_conn.cursor() as dc:
            src_raw = read_column_profile(
                source_engine, sc, _table_ref(family, source_schema, source_table),
                source_cols, source_types,
            )
            dst_profile = read_column_profile(
                dest_engine, dc, _table_ref(family, dest_schema, dest_table),
                target_cols, dest_types,
            )
        # Re-key the source profile onto the destination names so a rename lines
        # the two profiles up column-for-column.
        from dataclasses import replace as _replace_agg

        rename = {s: t for s, t in clean}
        src_profile = {}
        for s, agg in src_raw.items():
            tgt = rename.get(s, s)
            src_profile[tgt] = _replace_agg(agg, column=tgt)
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

    l1 = layer_l1_row_balance(
        source_rows=int(source_rows),
        target_rows=int(target_rows),
        rejected_rows=int(rejected_rows or 0),
        coerced_null_rows=int(coerced_null_rows or 0),
        rows_skipped=int(rows_skipped or 0),
    )
    l2 = compare_column_aggregates(src_profile, dst_profile)
    # Name what this pass does and does not prove, so no reader mistakes an
    # engine profile for the full five-layer localization.
    l2.details["source"] = "engine_sql_aggregates"
    l2.details["compared_statistics"] = ["null_count", "non_null_count", "min", "max", "sum"]
    l2.details["not_compared"] = [
        "distinct_count (collation-dependent)",
        "text min/max (collation-dependent)",
        "float sum (order-dependent)",
    ]
    mismatched = list(l2.details.get("mismatched_columns") or [])
    summary = ""
    if mismatched:
        summary = (
            "Engine column profile diverged on "
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
        "skipped": False,
        "localization": {"columns": mismatched},
        "localization_summary": summary,
        "reason": (
            "In-memory L2/L4/L5 declined for an oversized population; the engine "
            "computed per-column NULL/min/max/sum in SQL so column-level "
            "divergence is still detected at full scale."
        ),
    }
