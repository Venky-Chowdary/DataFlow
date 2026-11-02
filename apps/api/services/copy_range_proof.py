"""Proof that an occupied primary-key range already holds *this* source range.

The engine copies (X -> PostgreSQL / MySQL / SQL Server / Oracle) resume a
crashed load by skipping a key range whose destination ``COUNT(*)`` equals
the source range's count. A count is not identity: appending N new-key rows
into a key-declared table that already held N rows (a single unbounded range
covers the whole table) "matched", every range was skipped, nothing was
written, and the run failed its conservation proof — or, for a range that
happened to hold the same number of *other* keys, a resume could claim rows
it never wrote.

:func:`range_keys_match` streams the range's keys from both sides and
compares order-independent multiset digests (two independent 64-bit
accumulators plus the count), so a skip happens only when the destination
range holds exactly the source range's keys. A genuine resume of a partly
done run — whose completed ranges hold the very keys of the source — still
skips them.
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import re
import uuid
from decimal import Decimal
from typing import Any, Iterator

_MASK = (1 << 64) - 1
_BATCH = 10_000
_CANONICAL_INT = re.compile(r"-?(?:0|[1-9][0-9]*)")


def _key_token(value: Any) -> bytes:
    """Canonical bytes for one key cell, the same on every engine."""
    if value is None:
        return b"\x00"
    if isinstance(value, bool):
        return b"i:" + (b"1" if value else b"0")
    if isinstance(value, int):
        return b"i:" + str(value).encode()
    if isinstance(value, float):
        if value.is_integer():
            return b"i:" + str(int(value)).encode()
        return b"f:" + repr(value).encode()
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return b"i:" + str(int(value)).encode()
        return b"d:" + str(value.normalize()).encode()
    if isinstance(value, (bytes, bytearray, memoryview)):
        return b"b:" + bytes(value).hex().encode()
    if isinstance(value, uuid.UUID):
        return b"s:" + str(value).lower().encode()
    if isinstance(value, (_dt.datetime, _dt.date, _dt.time)):
        return b"t:" + value.isoformat().encode()
    text = str(value)
    # Numeric text (an Oracle NUMBER read as a string) is the same integer key.
    if _CANONICAL_INT.fullmatch(text):
        return b"i:" + text.encode()
    return b"s:" + text.encode("utf-8", "surrogatepass")


def _digest(rows: Iterator[Any]) -> tuple[int, int, int]:
    count = 0
    acc_a = 0
    acc_b = 0
    for value in rows:
        h = hashlib.blake2b(_key_token(value), digest_size=16).digest()
        acc_a = (acc_a + int.from_bytes(h[:8], "big")) & _MASK
        acc_b = (acc_b + int.from_bytes(h[8:], "big")) & _MASK
        count += 1
    return count, acc_a, acc_b


def _stream(cur: Any) -> Iterator[Any]:
    while True:
        chunk = cur.fetchmany(_BATCH)
        if not chunk:
            return
        for row in chunk:
            yield row[0]


def _range_sql(
    dialect: str, cur: Any, table_ref: str, ident: str, part: dict[str, Any]
) -> tuple[str, list[Any]]:
    lo, hi = part.get("lo"), part.get("hi")
    null_shard = bool(part.get("null_shard"))
    if dialect == "postgresql":
        from services.copy_pg_mysql import _pg_quoted_literal, pk_range_predicate

        if null_shard:
            pred = f"{ident} IS NULL"
        else:
            pred = pk_range_predicate(
                ident,
                _pg_quoted_literal(cur, lo) if lo is not None else None,
                _pg_quoted_literal(cur, hi) if hi is not None else None,
            )
        where = f" WHERE {pred}" if pred else ""
        return f"SELECT {ident} FROM {table_ref}{where}", []  # nosec B608
    if dialect == "oracle":
        from services.copy_oracle_oracle import oracle_pk_range_clause

        clause, params = oracle_pk_range_clause(ident, lo, hi, null_shard=null_shard)
    elif dialect in {"mysql", "sqlserver"}:
        from services.copy_pg_mysql import mysql_pk_range_clause

        clause, params = mysql_pk_range_clause(ident, lo, hi, null_shard=null_shard)
    else:
        raise ValueError(f"no range key reader for {dialect!r}")
    return f"SELECT {ident} FROM {table_ref} WHERE {clause}", params  # nosec B608


def range_key_digest(
    dialect: str, cur: Any, table_ref: str, ident: str, part: dict[str, Any]
) -> tuple[int, int, int]:
    """(count, digest_a, digest_b) of the keys in one range on one side."""
    sql, params = _range_sql(dialect, cur, table_ref, ident, part)
    if params:
        cur.execute(sql, params)
    else:
        cur.execute(sql)
    return _digest(_stream(cur))


def range_keys_match(
    *,
    src_dialect: str,
    src_cur: Any,
    src_ref: str,
    src_ident: str,
    dst_dialect: str,
    dst_cur: Any,
    dst_ref: str,
    dst_ident: str,
    part: dict[str, Any],
) -> bool:
    """True only when the destination range holds exactly the source range's keys.

    Any read failure answers False: the range is then treated as occupied by
    rows this run did not write (append declines the fast path; overwrite
    reloads it), never skipped on a count alone.
    """
    try:
        src = range_key_digest(src_dialect, src_cur, src_ref, src_ident, part)
        dst = range_key_digest(dst_dialect, dst_cur, dst_ref, dst_ident, part)
    except Exception:  # noqa: BLE001 - unproven is not a skip
        return False
    return src == dst
