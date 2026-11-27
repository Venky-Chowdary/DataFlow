"""Reading the destination's copies of an append's watermark tie group (D335).

The question is "which stored rows were written at the stored watermark?".
It is asked at the destination's own precision, with nothing wider:

* a temporal cursor is matched on the **carrier instant** - the source value
  quantized the way the destination column stores it (PG/MySQL/SQL Server
  round, SQL Server DATETIME to 1/300 s, BigQuery's writer truncates, Oracle
  DATE rounded or cut) - through a bind each engine converts exactly: typed
  datetimes, ``CAST(text AS type)`` on SQL Server (pymssql sends a datetime
  with milliseconds only), ``TO_TIMESTAMP[_TZ](text, fmt)`` on Oracle (a
  datetime bind is a DATE there);
* a text cursor (SQLite, VARCHAR) is matched on the spellings that instant
  can have (``T`` or space, fraction digits, offset forms);
* a keyed append adds every key column (OR of ANDs, chunked) and pages the
  answer by keyset; a keyless append streams it. Nothing is capped.

Errors are split. :class:`BoundaryStructural` - the destination has no such
column, the key is not on the page, the cursor type cannot be compared - will
fail the same way on every run, so it names the remedy. Anything else
(timeouts, lost connections) is transient: the run holds the tie group back
and the next run retries it.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from decimal import ROUND_FLOOR, ROUND_HALF_UP, Decimal
from typing import Any, Iterator

_KEY_CHUNK = 200
_PAGE_ROWS = 5_000
_STATEMENT_TIMEOUT_MS = 120_000
_SQL_FAMILIES = frozenset({"postgresql", "mysql", "sqlserver", "oracle", "sqlite"})
_EPOCH = datetime(1970, 1, 1)
_TEXT_TS = re.compile(
    r"^(\d{4}-\d{2}-\d{2})(?:[T ](\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?)?\s*(Z|z|[+-]\d{2}(?::?\d{2})?)?$"
)
_OFFSET = re.compile(r"\s*(Z|z|[+-]\d{2}(?::?\d{2})?)$")
_MSSQL = frozenset({"sqlserver", "mssql", "azure_sql", "azuresql"})

STRUCTURAL_REMEDY = (
    "Declare a primary key or dedupe key on this route (a keyed append is "
    "matched by key), or record the per-route acknowledgement "
    "'accept possible boundary duplicates' (destination option "
    "accept_boundary_duplicates with an actor and a reason)."
)


class BoundaryUnmeasured(RuntimeError):
    """The destination did not answer this time (transient): retry next run."""


class BoundaryStructural(BoundaryUnmeasured):
    """The destination can never answer as configured: fail with the remedy."""


# --- instants --------------------------------------------------------------------


def instant(value: Any) -> tuple[Decimal, bool] | None:
    """``(exact seconds since 1970 - UTC when zoned, zone-bearing)`` or None."""
    if value is None:
        return None
    if isinstance(value, datetime):
        aware = value.tzinfo is not None
        base = value.astimezone(timezone.utc).replace(tzinfo=None) if aware else value
        whole = base.replace(microsecond=0)
        seconds = Decimal((whole - _EPOCH) // timedelta(seconds=1))
        return seconds + Decimal(value.microsecond).scaleb(-6), aware
    from services.value_serializer import present_cell_text

    m = _TEXT_TS.match(str(present_cell_text(value) or "").strip())
    if not m:
        return None
    day, hh, mi, ss, frac, off = m.groups()
    base = datetime.fromisoformat(f"{day}T{hh or '00'}:{mi or '00'}:{ss or '00'}")
    seconds = Decimal((base - _EPOCH) // timedelta(seconds=1))
    if frac:
        seconds += Decimal("0." + frac)
    if off and off not in ("Z", "z"):
        sign = -1 if off[0] == "-" else 1
        digits = off[1:].replace(":", "")
        seconds -= sign * (int(digits[:2]) * 3600 + int(digits[2:4] or 0) * 60)
    return seconds, bool(off)


def civil(value: Any) -> Any:
    """The wall-clock digits of a zoned value (an NTZ carrier keeps those)."""
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    from services.value_serializer import present_cell_text

    return _OFFSET.sub("", str(present_cell_text(value) or ""))


def carrier_values(seconds: Decimal, ddl: str, dialect: str) -> list[Decimal]:
    """The value(s) the destination column stores for ``seconds``."""
    from connectors.sql_temporal import sql_base_type
    from services.carrier_instant import carrier_instant_digits

    base = sql_base_type(ddl or "") if ddl else ""
    fam = str(dialect or "").lower()
    if base == "SMALLDATETIME":
        return [(seconds / 60).quantize(Decimal(1), ROUND_HALF_UP) * 60]
    if base == "DATETIME" and fam in _MSSQL:
        ticks = (seconds * 300).quantize(Decimal(1), ROUND_HALF_UP)
        return [(ticks / 300).quantize(Decimal("0.001"), ROUND_HALF_UP)]
    if base == "DATE" and fam.startswith("oracle"):
        return [seconds.quantize(Decimal(1), ROUND_HALF_UP), seconds.quantize(Decimal(1), ROUND_FLOOR)]
    digits = carrier_instant_digits(ddl, engine=fam) if ddl else None
    if digits is None:
        return [seconds]
    step = Decimal(1).scaleb(-int(digits))
    if fam.startswith("bigquery") or fam == "bq":
        # The BigQuery writer parses to a Python datetime: digits past the
        # microsecond are cut, never rounded (warehouse_temporal).
        return [seconds.quantize(step, ROUND_FLOOR)]
    return [seconds.quantize(step, ROUND_HALF_UP)]


def same_cursor(src: Any, dest: Any, *, ddl: str, dialect: str) -> bool:
    """True when ``dest`` is the stored form of the source cursor value ``src``."""
    a, b = instant(src), instant(dest)
    if a is None or b is None:
        from connectors.writer_common import row_fingerprints

        fps = [
            fp
            for _k, fp in row_fingerprints(
                [(src,), (dest,)], ["c"], dest_db_type=dialect, dest_types={"c": ddl}
            )
        ]
        return len(fps) == 2 and fps[0] == fps[1]
    if a[1] != b[1]:
        a = instant(civil(src)) if a[1] else a
        b = instant(civil(dest)) if b[1] else b
        if a is None or b is None:
            return False
    return b[0] == a[0] or b[0] in carrier_values(a[0], ddl, dialect)


# --- the stored-instant predicate ---------------------------------------------------


def _iso(seconds: Decimal, digits: int = 6) -> str:
    whole = int(seconds.to_integral_value(ROUND_FLOOR))
    frac = seconds - whole
    text = (_EPOCH + timedelta(seconds=whole)).isoformat(sep=" ")
    if digits <= 0:
        return text
    return text + "." + str(int((frac * 10**digits).to_integral_value(ROUND_FLOOR))).zfill(digits)


def _text_spellings(watermark: str) -> list[str]:
    """Every spelling a text cursor may store this instant in."""
    m = _TEXT_TS.match(str(watermark).strip())
    if not m:
        return [str(watermark)]
    day, hh, mi, ss, frac, off = m.groups()
    if hh is None:
        return [day, f"{day} 00:00:00", f"{day}T00:00:00"]
    stripped = (frac or "").rstrip("0")
    fracs = {"", stripped} if not stripped else {stripped}
    fracs |= {frac or ""}
    for n in (3, 6, 7, 9):
        if len(stripped) <= n:
            fracs.add(stripped.ljust(n, "0"))
    offs = {off or ""}
    if off in (None, "", "Z", "z", "+00:00", "+0000", "+00"):
        offs |= {"", "Z", "+00:00"} if off else {""}
    out = []
    for sep in ("T", " "):
        for f in sorted(fracs):
            for o in sorted(offs):
                out.append(f"{day}{sep}{hh}:{mi}:{ss}" + (f".{f}" if f else "") + o)
    return sorted(set(out))


def _column_ddl(col: Any, engine: Any) -> str:
    try:
        return str(col.type.compile(dialect=engine.dialect))
    except Exception:  # noqa: BLE001 - an uncompilable type names itself
        return type(col.type).__name__


def stored_instant_predicate(col: Any, watermark: str, dialect: str, engine: Any) -> Any:
    """``col`` holds the stored form of ``watermark`` (exact, at carrier precision)."""
    import sqlalchemy as sa

    from connectors.cursor_bind import cursor_kind, sa_cursor_bind

    from .engine_family import family_or_kind

    fam = family_or_kind(dialect)
    kind = cursor_kind(getattr(col, "type", None))
    if fam == "sqlite" or kind == "text" or isinstance(getattr(col, "type", None), sa.String):
        return col.in_(_text_spellings(watermark))
    if kind in ("integer", "decimal", "float"):
        return col == sa_cursor_bind(col, str(watermark), dialect=dialect)
    inst = instant(watermark)
    if kind not in ("datetime", "datetime_tz", "date") or inst is None:
        raise BoundaryStructural(
            f"the cursor column's type {col.type!r} cannot be compared with the watermark"
        )
    zoned = kind == "datetime_tz"
    seconds = inst[0] if (zoned or not inst[1]) else instant(civil(watermark))[0]
    ddl = _column_ddl(col, engine)
    values = carrier_values(seconds, ddl, fam)
    from connectors.sql_temporal import sql_base_type
    from services.carrier_instant import carrier_instant_digits

    if kind == "date":
        lo = hi = seconds.quantize(Decimal(1), ROUND_FLOOR)
        digits, pad = 0, Decimal(0)
    elif sql_base_type(ddl) == "DATETIME" and fam in _MSSQL:
        lo, hi, digits, pad = min(values), max(values), 3, Decimal("0.002")
    else:
        stored = carrier_instant_digits(ddl, engine=fam)
        digits = 6 if stored is None else min(int(stored), 6)
        lo, hi = min(values), max(values)
        # Beyond the microsecond no driver binds: the stored digit is matched
        # at the microsecond it falls in.
        pad = Decimal(0) if stored is None or stored <= 6 else Decimal("0.000001")
    lo_t, hi_t = _iso(lo - pad, digits), _iso(hi + pad if pad else hi, digits)
    if zoned:
        lo_t, hi_t = lo_t + "+00:00", hi_t + "+00:00"
    if fam == "oracle":
        fn = "TO_TIMESTAMP_TZ" if zoned else "TO_TIMESTAMP"
        fmt = "YYYY-MM-DD HH24:MI:SS.FF9" + (" TZH:TZM" if zoned else "")

        def _ora(text: str) -> Any:
            text = text.replace("+00:00", " +00:00") if zoned else text
            if "." not in text.split(" ")[1]:
                text = text.replace(" +", ".0 +") if zoned else text + ".0"
            return getattr(sa.func, fn.lower())(sa.bindparam(None, text, type_=sa.String), fmt)

        return sa.and_(col >= _ora(lo_t), col <= _ora(hi_t))
    if fam == "sqlserver":
        return sa.and_(
            col >= sa.cast(sa.bindparam(None, lo_t, type_=sa.String), col.type),
            col <= sa.cast(sa.bindparam(None, hi_t, type_=sa.String), col.type),
        )
    return sa.and_(
        col >= sa_cursor_bind(col, lo_t, dialect=dialect),
        col <= sa_cursor_bind(col, hi_t, dialect=dialect),
    )


# --- reading ------------------------------------------------------------------------


def _key_terms(table_cols: dict[str, Any], key_cols: list[str], keys: list[dict], dialect: str) -> Any:
    import sqlalchemy as sa

    from connectors.cursor_bind import sa_cursor_bind

    terms = []
    for key in keys:
        parts = []
        for c in key_cols:
            col = table_cols[c.lower()]
            v = key.get(c)
            parts.append(col.is_(None) if v is None else col == sa_cursor_bind(col, str(v), dialect=dialect))
        terms.append(sa.and_(*parts))
    return sa.or_(*terms)


def _after(cols: list[Any], last: tuple) -> Any:
    """Keyset continuation ``(c1, c2, ...) > last`` spelled for every engine."""
    import sqlalchemy as sa

    ors = []
    for i, col in enumerate(cols):
        eqs = [cols[j] == last[j] for j in range(i)]
        ors.append(sa.and_(*eqs, col > last[i]))
    return sa.or_(*ors)


def _bound_session(conn: Any, fam: str) -> None:
    """A per-statement time budget on PostgreSQL, scoped to this transaction (R10).

    Other engines keep their driver read timeout; nothing is set on a pooled
    session that outlives this read.
    """
    if fam == "postgresql":
        conn.exec_driver_sql("SET LOCAL statement_timeout = %d" % _STATEMENT_TIMEOUT_MS)


def read_candidates(
    *, dialect: str, dest_cfg: dict, table: str, cursor_col: str, watermark: str,
    key_cols: list[str], keys: list[dict] | None, columns: list[str],
) -> Iterator[dict[str, Any]]:
    """Destination rows stored at the watermark (lower-cased names), every one."""
    from .engine_family import family_or_kind

    if family_or_kind(dialect) not in _SQL_FAMILIES:
        yield from _sample_rows(dialect=dialect, dest_cfg=dest_cfg, table=table, cursor_col=cursor_col,
                                watermark=watermark, key_cols=key_cols, keys=keys, columns=columns)
        return
    import sqlalchemy as sa

    from connectors.generic_sql import _engine, _reflect_table, _schema_name
    from services.engine_pool import release_engine

    cfg = dict(dest_cfg)
    cfg.setdefault("type", dialect)
    engine = _engine(cfg)
    try:
        try:
            table_obj = _reflect_table(engine, table, _schema_name(cfg))
        except sa.exc.NoSuchTableError:
            return
        by_name = {str(c.name).casefold(): c for c in table_obj.c}
        need = [cursor_col, *(key_cols if keys is not None else [])]
        missing = [c for c in need if c.casefold() not in by_name]
        if missing:
            raise BoundaryStructural("the destination table has no column " + ", ".join(missing))
        names = list(dict.fromkeys([c for c in columns if c.casefold() in by_name] + need))
        cols = {c.lower(): by_name[c.casefold()] for c in names}
        fam = family_or_kind(dialect)
        where = stored_instant_predicate(cols[cursor_col.lower()], str(watermark), fam, engine)
        select = [cols[c.lower()] for c in names]
        with engine.connect() as conn:
            _bound_session(conn, fam)
            if keys is None:
                result = conn.execution_options(stream_results=True).execute(sa.select(*select).where(where))
                while True:
                    rows = result.fetchmany(_PAGE_ROWS)
                    if not rows:
                        break
                    for row in rows:
                        yield {c.lower(): v for c, v in zip(names, row)}
                return
            order = [cols[c.lower()] for c in key_cols]
            for i in range(0, len(keys), _KEY_CHUNK):
                base = sa.and_(where, _key_terms(cols, key_cols, keys[i : i + _KEY_CHUNK], fam))
                last = None
                while True:
                    cond = base if last is None else sa.and_(base, _after(order, last))
                    rows = conn.execute(
                        sa.select(*select).where(cond).order_by(*order).limit(_PAGE_ROWS)
                    ).fetchall()
                    for row in rows:
                        yield {c.lower(): v for c, v in zip(names, row)}
                    if len(rows) < _PAGE_ROWS:
                        break
                    last = tuple(row[names.index(c)] for c in key_cols)
    finally:
        release_engine(engine)


def _sample_rows(*, dialect, dest_cfg, table, cursor_col, watermark, key_cols, keys, columns):
    """Document and file destinations: the sample reader, filtered here."""
    from services.target_sample import read_target_sample

    if keys is None:
        sort_key, values = cursor_col, _text_spellings(str(watermark))
    else:
        sort_key = key_cols[0]
        values = list(dict.fromkeys(str(k.get(sort_key)) for k in keys if k.get(sort_key) is not None))
    rows = read_target_sample(
        dialect, dest_cfg, schema=str(dest_cfg.get("schema") or "public"), table_name=table,
        columns=list(dict.fromkeys([*columns, cursor_col, *key_cols])), limit=10_000_000,
        sort_key=sort_key, key_values=values,
    )
    for r in rows or []:
        low = {str(k).lower(): v for k, v in r.items()}
        if same_cursor(watermark, low.get(cursor_col.lower()), ddl="", dialect=dialect):
            yield low


def live_columns(*, dialect: str, dest_cfg: dict, table: str) -> set[str] | None:
    """Lower-cased columns the destination table holds now (None: not a SQL table)."""
    from .engine_family import family_or_kind

    if family_or_kind(dialect) not in _SQL_FAMILIES:
        return None
    import sqlalchemy as sa

    from connectors.generic_sql import _engine, _reflect_table, _schema_name
    from services.engine_pool import release_engine

    cfg = dict(dest_cfg)
    cfg.setdefault("type", dialect)
    engine = _engine(cfg)
    try:
        try:
            table_obj = _reflect_table(engine, table, _schema_name(cfg))
        except sa.exc.NoSuchTableError:
            return set()
        return {str(c.name).lower() for c in table_obj.c}
    finally:
        release_engine(engine)
