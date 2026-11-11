"""Millisecond narrowing onto a document-store instant, decided on values (D148).

BSON ``date`` and the Elasticsearch ``date`` field keep milliseconds. A source
column declaring finer precision (``DATETIME(6)``, ``TIMESTAMPTZ(6)``,
``DATETIME2(7)``) loses nothing when every stored value is a whole millisecond,
and silently loses digits when one is not. The declaration cannot tell which,
so the values decide, in two places with one rule:

* **Write time** (authoritative, every run: initial, incremental, scheduled).
  Each batch is checked as it is prepared, with integer arithmetic on the
  microsecond field. An unacknowledged column holding a value with sub-
  millisecond digits refuses the whole batch before anything of it lands,
  naming the columns and how many values were seen. Rows earlier batches of
  this run wrote are the run's failure/undo policy's, as for any mid-run
  refusal. With the acknowledgement (a signed continue-policy Migration Risk
  Contract on the column, the same one that clears the preflight collapse)
  the values are truncated as the carrier requires and counted for the proof.
* **Preflight** (early answer). A SQL source with pushdown counts, in one
  aggregate over the transfer's read scope (source filter, incremental cursor),
  the values whose microsecond part is not a multiple of 1000. Zero passes
  unsigned and is recorded; non-zero asks for the acknowledgement. Without
  pushdown (files, Kafka, document sources, a failed query) preflight passes
  provisionally *because* the write-time guard enforces the rule.
"""

from __future__ import annotations

import contextlib
import logging
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)

#: Gate id of the preflight answer.
GATE_ID = "g3_millisecond_fit"
#: Run-scoped attribute on the destination endpoint carrying the preflight
#: evidence to the run's proof. An attribute, not ``extra``: it must never be
#: persisted with the request and read back by a later run as its own proof.
EVIDENCE_ATTR = "_millisecond_fit_evidence"
#: Writer meta / destination-summary key: values truncated under the ack.
TRUNCATED_KEY = "ms_truncated_values"

METHOD_AGGREGATE = "source aggregate"
METHOD_PER_VALUE = "verified per value during load"

_US_PER_MS = 1000
#: Engines whose dialect expression below is proven live.
_AGGREGATE_ENGINES = frozenset({"postgresql", "mysql", "sqlserver"})
_EXAMPLE_CHARS = 40


@dataclass(frozen=True)
class NarrowedColumn:
    """One mapping whose datetime lands on a millisecond carrier."""

    source: str
    target: str
    source_type: str
    target_type: str
    acknowledged: bool


def narrowing_acknowledged(mapping: Mapping[str, Any] | None) -> bool:
    """The acknowledgement: a signed continue-policy Migration Risk Contract.

    The same contract that clears the preflight fidelity collapse. A legacy
    ``risk_acknowledged`` boolean alone never clears (charter), so it does not
    license a truncation here either.
    """
    from services.migration_risk_contract import mapping_has_clearing_risk_contract

    return bool(mapping) and mapping_has_clearing_risk_contract(dict(mapping or {}))


def narrowed_columns(
    mappings: Iterable[Mapping[str, Any]] | None,
    *,
    column_types: Mapping[str, str] | None,
    dest_types: Mapping[str, str] | None,
    dest_db: str,
    source_db: str = "",
) -> list[NarrowedColumn]:
    """Mappings whose source may hold finer than milliseconds on a ms carrier."""
    from services.document_instant import (
        DOCUMENT_INSTANT_FRACTIONAL_DIGITS,
        is_document_instant_token,
        source_fractional_digits,
        transform_narrows_to_calendar_day,
    )
    from services.source_engine_scope import bind_source_engine
    from services.type_system import LOGICAL_DATETIME, normalize_logical_type

    out: list[NarrowedColumn] = []
    seen: set[str] = set()
    for mapping in mappings or []:
        if not isinstance(mapping, Mapping):
            continue
        source = str(mapping.get("source") or "").strip()
        target = str(mapping.get("target") or source).strip()
        if not source or not target or target in seen:
            continue
        target_type = str(
            (dest_types or {}).get(target)
            or mapping.get("target_type")
            or mapping.get("inferredType")
            or ""
        ).strip()
        source_type = str(
            (column_types or {}).get(source) or mapping.get("source_type") or ""
        ).strip()
        if not is_document_instant_token(dest_db, target_type):
            continue
        if normalize_logical_type(source_type) != LOGICAL_DATETIME:
            continue
        if transform_narrows_to_calendar_day(str(mapping.get("transform") or "")):
            continue
        with bind_source_engine(source_db or ""):
            digits = source_fractional_digits(source_type)
        if digits is not None and digits <= DOCUMENT_INSTANT_FRACTIONAL_DIGITS:
            continue
        seen.add(target)
        out.append(
            NarrowedColumn(
                source=source,
                target=target,
                source_type=source_type,
                target_type=target_type,
                acknowledged=narrowing_acknowledged(mapping),
            )
        )
    return out


# ── write time ────────────────────────────────────────────────────────────


def has_sub_millisecond(value: Any) -> bool:
    """True when a cell carries a non-zero digit below the millisecond.

    Integer arithmetic on the microsecond field for a datetime (the reader's
    usual cell); text is judged on its own digits, so a seven-digit string is
    not first truncated to Python's six.
    """
    if value is None:
        return False
    if isinstance(value, datetime):
        return bool(value.microsecond % _US_PER_MS or getattr(value, "nanosecond", 0))
    if isinstance(value, str):
        from services.document_instant import cell_exceeds_document_instant

        return cell_exceeds_document_instant(value)
    return False


def floor_to_millisecond(value: Any) -> Any:
    """A datetime as a millisecond carrier stores it (floored, like BSON)."""
    if isinstance(value, datetime) and value.microsecond % _US_PER_MS:
        return value.replace(microsecond=value.microsecond - value.microsecond % _US_PER_MS)
    return value


@dataclass
class BatchGuard:
    """Outcome of checking one batch before it is bound."""

    refusal: str = ""
    #: Values with sub-millisecond digits per acknowledged column (to truncate).
    truncated: dict[str, int] = field(default_factory=dict)


def guard_batch(
    rows: Iterable[Any],
    target_cols: list[str],
    guarded: Iterable[str],
    *,
    acknowledged: Iterable[str],
    carrier_label: str,
) -> BatchGuard:
    """Check every guarded cell of a batch; refuse it before any write.

    ``guarded`` names the target columns bound to a millisecond carrier;
    ``acknowledged`` the ones whose narrowing a contract accepted.
    """
    wanted = {str(c) for c in guarded}
    idx = [(i, str(c)) for i, c in enumerate(target_cols or []) if str(c) in wanted]
    if not idx:
        return BatchGuard()
    counts: dict[str, int] = {}
    first: dict[str, tuple[int, Any]] = {}
    for row_no, row in enumerate(rows, start=1):
        for i, col in idx:
            if i < len(row) and has_sub_millisecond(row[i]):
                counts[col] = counts.get(col, 0) + 1
                first.setdefault(col, (row_no, row[i]))
    if not counts:
        return BatchGuard()
    acked = {str(c) for c in acknowledged}
    refused = {c: n for c, n in counts.items() if c not in acked}
    truncated = {c: n for c, n in counts.items() if c in acked}
    if not refused:
        return BatchGuard(truncated=truncated)
    return BatchGuard(refusal=refusal_message(refused, first, carrier_label), truncated=truncated)


def _values(n: int) -> str:
    return f"{n} value" if n == 1 else f"{n} values"


def refusal_message(
    counts: Mapping[str, int],
    first: Mapping[str, tuple[int, Any]],
    carrier_label: str,
) -> str:
    cols = sorted(counts)
    named = ", ".join(f"{c} ({_values(counts[c])})" for c in cols)
    examples = "; ".join(
        f"{c} row {first[c][0]}: {str(first[c][1])[:_EXAMPLE_CHARS]}" for c in cols if c in first
    )
    return (
        f"{carrier_label} keeps milliseconds and {_values(sum(counts.values()))} in this "
        f"batch carry finer digits: {named} (e.g. {examples}). Nothing from this batch "
        "was written — the narrowing is not acknowledged, so it is refused rather "
        "than truncated. To load these values truncated to milliseconds, accept the "
        f"millisecond-truncation risk on {', '.join(cols)} (sign a Migration Risk "
        "Contract with a continue policy); or map the column to a string to keep "
        "every digit."
    )


def truncation_warnings(truncated: Mapping[str, int], carrier_label: str) -> list[str]:
    return [
        f"{col}: {_values(n)} with sub-millisecond digits truncated to "
        f"{carrier_label} milliseconds under the accepted risk"
        for col, n in sorted(truncated.items())
        if n
    ]


def merge_truncations(prev: Any, incoming: Any) -> dict[str, Any]:
    """Run total of truncated values across batches (destination summary)."""
    total: dict[str, int] = {}
    for part in (prev, incoming):
        counts = part.get(TRUNCATED_KEY) if isinstance(part, dict) else None
        for col, n in (counts or {}).items():
            total[str(col)] = total.get(str(col), 0) + int(n or 0)
    return {TRUNCATED_KEY: total} if total else {}


# ── preflight ─────────────────────────────────────────────────────────────


def _sub_ms_predicate(engine: str, col_sql: str) -> str:
    if engine == "postgresql":
        return f"MOD(CAST(EXTRACT(MICROSECONDS FROM {col_sql}) AS BIGINT), 1000) <> 0"
    if engine == "mysql":
        return f"MOD(MICROSECOND({col_sql}), 1000) <> 0"
    # SQL Server: DATETIME2(7) / DATETIMEOFFSET carry 100 ns ticks.
    nanos = f"DATEPART(NANOSECOND, {col_sql})"
    return f"{nanos} / 1000000 * 1000000 <> {nanos}"


@dataclass(frozen=True)
class AggregateCount:
    checked: dict[str, int]
    sub_millisecond: dict[str, int]


#: Server-side bound on the preflight aggregate. A full-column scan of a large
#: source must not stall the plan (D321/D281): past this the query is cancelled
#: by the database, the gate passes provisionally and the write-time guard
#: checks every value instead.
AGGREGATE_TIMEOUT_ENV = "MS_FIT_AGGREGATE_SECONDS"
_AGGREGATE_TIMEOUT_DEFAULT = 20.0


def aggregate_timeout_seconds() -> float:
    from services.brand_env import getenv_brand

    raw = str(getenv_brand(AGGREGATE_TIMEOUT_ENV, "") or "").strip()
    try:
        value = float(raw) if raw else _AGGREGATE_TIMEOUT_DEFAULT
    except ValueError:
        value = _AGGREGATE_TIMEOUT_DEFAULT
    return value if value > 0 else _AGGREGATE_TIMEOUT_DEFAULT


@contextlib.contextmanager
def _bound_aggregate(conn: Any, engine: str) -> Iterator[None]:
    """Make the database cancel the aggregate after :func:`aggregate_timeout_seconds`.

    The connection is pooled, so a session-level setting is restored on the
    way out (PostgreSQL's ``SET LOCAL`` ends with the transaction). A dialect
    without a bound runs unbounded; the caller's exception path still falls
    back to the write-time guard.
    """
    seconds = aggregate_timeout_seconds()
    millis = max(1, int(seconds * 1000))
    restore: str | None = None
    raw: Any = None
    previous_timeout: Any = None
    if engine == "postgresql":
        conn.exec_driver_sql(f"SET LOCAL statement_timeout = {millis}")
    elif engine == "mysql":
        if getattr(conn.dialect, "is_mariadb", False):
            conn.exec_driver_sql(f"SET SESSION max_statement_time = {seconds:.3f}")
            restore = "SET SESSION max_statement_time = DEFAULT"
        else:
            conn.exec_driver_sql(f"SET SESSION max_execution_time = {millis}")
            restore = "SET SESSION max_execution_time = DEFAULT"
    elif engine == "sqlserver":
        raw = getattr(conn.connection, "dbapi_connection", None) or conn.connection
        if hasattr(raw, "timeout"):
            previous_timeout = raw.timeout
            raw.timeout = max(1, int(seconds))
        else:
            raw = None
    try:
        yield
    finally:
        try:
            if restore:
                conn.exec_driver_sql(restore)
            if raw is not None:
                raw.timeout = previous_timeout
        except Exception:  # noqa: BLE001 - a broken session is discarded by the pool
            logger.debug("millisecond-fit aggregate bound not restored", exc_info=True)


def source_sub_millisecond_counts(
    source: Any,
    columns: list[str],
    *,
    source_filter: Mapping[str, Any] | None = None,
    read_scope: Any = None,
) -> AggregateCount | None:
    """One aggregate over the read scope; ``None`` when pushdown is unavailable.

    A query that fails raises: the caller falls back to the write-time guard
    and says so, never passes silently.
    """
    from src.transfer.connector_capabilities import resolve_driver_type

    engine = resolve_driver_type(str(getattr(source, "format", "") or ""))
    table = str(getattr(source, "table", "") or "").strip()
    if engine not in _AGGREGATE_ENGINES or not table or not columns:
        return None
    from services.procedure_source import is_callable_source

    if is_callable_source(source):
        return None
    from connectors.sql_identifiers import require_safe_identifier
    from services.cdc_snapshot_filter import (
        compile_snapshot_filter_sql,
        normalize_snapshot_filter,
    )
    from services.dest_precount import warehouse_sql_engine, warehouse_sql_table_ref
    from src.transfer.adapters import resolve_connector_config

    cfg = resolve_connector_config(source)
    schema = str(getattr(source, "schema", "") or cfg.get("schema") or "")
    table_ref = warehouse_sql_table_ref(engine, cfg, schema, table)
    spec = normalize_snapshot_filter(dict(source_filter)) if source_filter else None
    with warehouse_sql_engine(engine, cfg) as sa_engine:
        dialect = sa_engine.dialect
        mark = "?" if dialect.paramstyle == "qmark" else "%s"
        quote = dialect.identifier_preparer.quote
        quote_char = dialect.identifier_preparer.initial_quote
        terms: list[str] = []
        for col in columns:
            q = quote(require_safe_identifier(col, preserve_case=True, max_len=128))
            terms.append(f"COUNT({q})")
            terms.append(f"SUM(CASE WHEN {_sub_ms_predicate(engine, q)} THEN 1 ELSE 0 END)")
        dialect_name = "sqlserver" if engine == "sqlserver" else engine
        where, params = compile_snapshot_filter_sql(
            spec, dialect=dialect_name, quote_char=quote_char, placeholder=mark
        )
        clauses = [where] if where else []
        if read_scope is not None and getattr(read_scope, "bounded", False):
            from services.keyset_pagination import (
                present_cursor_bookmark,
                split_cursor_bookmark,
            )

            bookmark = present_cursor_bookmark(read_scope.read_watermark)
            seed = split_cursor_bookmark(bookmark, has_tiebreak=True)[0] if bookmark else ""
            cursor = str(getattr(read_scope, "cursor_column", "") or "")
            if seed and cursor:
                # ``>=`` reads the tie group the run trims too: a superset can
                # only over-count, never pass a value the run will write.
                qc = quote(require_safe_identifier(cursor, preserve_case=True, max_len=128))
                clauses.append(f"{qc} >= {mark}")
                params.append(seed)
        sql = f"SELECT {', '.join(terms)} FROM {table_ref}"  # nosec B608 - quoted identifiers
        if clauses:
            sql += " WHERE " + " AND ".join(f"({c})" for c in clauses)
        with sa_engine.connect() as conn:
            with _bound_aggregate(conn, engine):
                row = conn.exec_driver_sql(sql, tuple(params)).fetchone()
    values = [int(v or 0) for v in (row or [])]
    checked = {c: values[2 * i] if 2 * i < len(values) else 0 for i, c in enumerate(columns)}
    sub_ms = {
        c: values[2 * i + 1] if 2 * i + 1 < len(values) else 0 for i, c in enumerate(columns)
    }
    return AggregateCount(checked=checked, sub_millisecond=sub_ms)


def _aggregate_applicable(request: Any) -> bool:
    """The aggregate counts the rows the run reads only without row shaping."""
    if int(getattr(request, "limit", 0) or 0) > 0:
        return False
    if getattr(request, "shape_recipe", None):
        return False
    from services.sync_cursor import normalize_sync_mode

    return normalize_sync_mode(str(getattr(request, "sync_mode", "") or ""), default="") != "cdc"


def _label(col: NarrowedColumn, dest_db: str) -> str:
    from services.carrier_instant import temporal_carrier_display_label

    return (
        f"{col.target} {col.source_type} → "
        f"{temporal_carrier_display_label(col.target_type, dest_db)}"
    )


def millisecond_fit_gate(
    request: Any,
    mappings: list[dict[str, Any]] | None,
    column_types: Mapping[str, str] | None,
    dest_types: Mapping[str, str] | None,
) -> dict[str, Any] | None:
    """Preflight answer for the millisecond narrowing; ``None`` when none applies.

    The evidence is also left on the destination endpoint (``EVIDENCE_ATTR``)
    so the run's proof can say what was checked.
    """
    from preflight.models import GateStatus

    dest = getattr(request, "destination", None)
    src = getattr(request, "source", None)
    dest_db = str(getattr(dest, "format", "") or "").lower()
    source_db = str(getattr(src, "format", "") or "").lower()
    cols = narrowed_columns(
        mappings,
        column_types=column_types,
        dest_types=dest_types,
        dest_db=dest_db,
        source_db=source_db,
    )
    pending = [c for c in cols if not c.acknowledged]
    if not pending:
        return None
    evidence: dict[str, Any] = {
        "columns": [c.target for c in pending],
        "method": METHOD_PER_VALUE,
    }
    counts: AggregateCount | None = None
    if _aggregate_applicable(request):
        try:
            from services.preflight_cursor_gate import read_scope_for_transfer_request

            counts = source_sub_millisecond_counts(
                src,
                [c.source for c in pending],
                source_filter=getattr(request, "source_filter", None) or None,
                read_scope=read_scope_for_transfer_request(request),
            )
        except Exception as exc:  # noqa: BLE001 - fall back to the write-time guard
            logger.warning("millisecond-fit source aggregate failed: %s", exc)
            evidence["aggregate_error"] = str(exc)[:300]
    names = ", ".join(c.target for c in pending)
    status = GateStatus.PASS.value
    if counts is None:
        message = (
            f"Millisecond narrowing on {names}: {METHOD_PER_VALUE} — a value with "
            "sub-millisecond digits refuses its batch before it is written"
        )
    else:
        checked = sum(counts.checked.get(c.source, 0) for c in pending)
        offending = {
            c.target: counts.sub_millisecond.get(c.source, 0)
            for c in pending
            if counts.sub_millisecond.get(c.source, 0)
        }
        evidence.update(
            method=METHOD_AGGREGATE,
            checked_values=checked,
            checked_by_column={c.target: counts.checked.get(c.source, 0) for c in pending},
            sub_millisecond_values=offending,
        )
        if offending:
            status = GateStatus.BLOCK.value
            labels = ", ".join(_label(c, dest_db) for c in pending if c.target in offending)
            message = (
                f"Millisecond narrowing would truncate "
                f"{_values(sum(offending.values()))} with sub-millisecond digits: "
                + ", ".join(f"{col} ({_values(n)})" for col, n in offending.items())
                + f" — {labels}; checked {_values(checked)} ({METHOD_AGGREGATE}). "
                "Accept the millisecond-truncation risk on "
                f"{', '.join(offending)} (Migration Risk Contract with a continue "
                "policy), or map the column to a string."
            )
        else:
            message = (
                f"Checked {_values(checked)} in columns {names}, all fit milliseconds "
                f"(method: {METHOD_AGGREGATE})"
            )
    evidence["message"] = message
    if dest is not None:
        setattr(dest, EVIDENCE_ATTR, dict(evidence))
    return {
        "id": GATE_ID,
        "status": status,
        "message": message,
        "duration_ms": 0,
        "details": {**evidence, "columns": [c.target for c in pending]},
    }


# ── proof ─────────────────────────────────────────────────────────────────


def millisecond_fit_proof(
    mappings: list[dict[str, Any]] | None,
    *,
    source_schema: Mapping[str, str] | None,
    dest_types: Mapping[str, str] | None,
    dest_engine: str,
    source_db: str = "",
    preflight: Mapping[str, Any] | None = None,
    truncated: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    """What a finished run proved about its millisecond narrowing.

    Reached only by a run every batch of which passed the write-time guard: an
    unacknowledged column therefore lost nothing; an acknowledged one lost the
    digits of the values counted in ``truncated``.
    """
    cols = narrowed_columns(
        mappings,
        column_types=source_schema,
        dest_types=dest_types,
        dest_db=dest_engine,
        source_db=source_db,
    )
    if not cols:
        return None
    counts = {str(k): int(v or 0) for k, v in (truncated or {}).items()}
    pre = dict(preflight or {})
    fit = [c.target for c in cols if not counts.get(c.target)]
    proof: dict[str, Any] = {
        "columns": [c.target for c in cols],
        "fit_columns": fit,
        "truncated_values": {c.target: counts[c.target] for c in cols if counts.get(c.target)},
        "method": pre.get("method") or METHOD_PER_VALUE,
    }
    if "checked_values" in pre:
        proof["checked_values"] = int(pre.get("checked_values") or 0)
    aggregated = (
        set(pre.get("columns") or [])
        if proof["method"] == METHOD_AGGREGATE and "checked_values" in proof
        else set()
    )
    by_aggregate = [c for c in fit if c in aggregated]
    per_value = [c for c in fit if c not in aggregated]
    parts: list[str] = []
    if by_aggregate:
        parts.append(
            f"checked {_values(proof['checked_values'])} in columns "
            f"{', '.join(by_aggregate)}, all fit milliseconds (method: {METHOD_AGGREGATE})"
        )
    if per_value:
        parts.append(
            f"every value in columns {', '.join(per_value)} fit milliseconds "
            f"({METHOD_PER_VALUE})"
        )
    for col, n in proof["truncated_values"].items():
        parts.append(f"{col}: {_values(n)} truncated to milliseconds under the accepted risk")
    proof["message"] = "Millisecond narrowing: " + "; ".join(parts) + "."
    return proof
