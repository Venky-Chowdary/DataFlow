"""D186 — an Oracle ``TIMESTAMP WITH LOCAL TIME ZONE`` NULL must read back as NULL.

D167 renders LTZ as ``TO_CHAR(SYS_EXTRACT_UTC(col), mask) || '+00:00'``. Oracle
concatenation treats NULL as the empty string, so a NULL cell became the text
``'+00:00'``, the writer could not bind it as a timestamp and the whole table
rolled back. The read must be NULL-guarded.

Oracle is not available here. ``_oracle_eval`` is a minimal evaluator that
implements the Oracle semantics that matter (``||`` treats NULL as '' and a
string-typed NULL stays NULL through ``TO_CHAR``/``SYS_EXTRACT_UTC``); whether
the real server agrees is for a run against a live Oracle to confirm.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import oracle

from connectors.generic_sql import _tz_safe_projection
from connectors.oracle_tstz import oracle_tz_to_char, parse_oracle_tstz_text

_TOKEN = re.compile(
    r"\s*(?:(?P<str>'(?:[^']|'')*')|(?P<op>\|\||\(|\)|,)|(?P<word>[A-Za-z_][A-Za-z_0-9.\"]*|\"[^\"]+\"))"
)


def _oracle_eval(expr: str, value: datetime | None) -> Any:
    """Evaluate the read expression with Oracle NULL semantics for column ``"C"``/``c``."""
    tokens = [m for m in _TOKEN.finditer(expr)]
    assert "".join(m.group(0) for m in tokens).strip() == expr.strip(), expr
    toks = [(m.lastgroup, m.group(m.lastgroup)) for m in tokens]
    pos = 0

    def peek() -> tuple[str, str] | tuple[None, None]:
        return toks[pos] if pos < len(toks) else (None, None)

    def take() -> tuple[str, str]:
        nonlocal pos
        pos += 1
        return toks[pos - 1]

    def kw(word: str) -> bool:
        kind, text = peek()
        return kind == "word" and text.upper() == word

    def concat() -> Any:
        left = primary()
        while peek() == ("op", "||"):
            take()
            right = primary()
            # Oracle: concatenating with NULL yields the other operand.
            left = (left or "") + (right or "") if (left is not None or right is not None) else None
        return left

    def primary() -> Any:
        kind, text = take()
        if kind == "str":
            return text[1:-1].replace("''", "'")
        if kind == "op" and text == "(":
            inner = concat()
            assert take() == ("op", ")")
            return inner
        upper = text.upper()
        if upper == "CASE":
            result, matched = None, False
            while kw("WHEN"):
                take()
                parens = peek() == ("op", "(")
                if parens:
                    take()
                take()  # the column
                assert kw("IS")
                take()
                negate = kw("NOT")
                if negate:
                    take()
                assert take()[1].upper() == "NULL"
                if parens:
                    assert take() == ("op", ")")
                assert take()[1].upper() == "THEN"
                branch = concat_or_null()
                cond = (value is None) != negate
                if cond and not matched:
                    result, matched = branch, True
            if kw("ELSE"):
                take()
                branch = concat_or_null()
                if not matched:
                    result = branch
            assert take()[1].upper() == "END"
            return result
        if upper in {"TO_CHAR", "SYS_EXTRACT_UTC"}:
            assert take() == ("op", "(")
            arg = concat()
            fmt = None
            if peek() == ("op", ","):
                take()
                fmt = concat()
            assert take() == ("op", ")")
            if arg is None:
                return None
            if upper == "SYS_EXTRACT_UTC":
                return arg
            assert "TZH" not in (fmt or "")
            return arg.strftime("%Y-%m-%dT%H:%M:%S.%f")
        if upper == "NULL":
            return None
        # a column reference
        return value

    def concat_or_null() -> Any:
        return concat()

    result = concat()
    assert pos == len(toks), (pos, toks)
    return result


WALL = datetime(2024, 1, 1, 8, 31, 0, 123456)


def test_ltz_expression_keeps_a_value_as_utc_text_with_offset():
    sql = oracle_tz_to_char('"C"', "ltz")
    assert _oracle_eval(sql, WALL) == "2024-01-01T08:31:00.123456+00:00"
    assert parse_oracle_tstz_text(_oracle_eval(sql, WALL)).utcoffset().total_seconds() == 0


def test_ltz_expression_keeps_null_as_null():
    sql = oracle_tz_to_char('"C"', "ltz")
    assert _oracle_eval(sql, None) is None, sql


def test_unguarded_concat_is_what_oracle_turns_into_plus_zero_text():
    """Pins the evaluator to the defect: the D167 shape yields '+00:00' for NULL."""
    d167 = "TO_CHAR(SYS_EXTRACT_UTC(\"C\"), 'YYYY-MM-DD\"T\"HH24:MI:SS.FF6') || '+00:00'"
    assert _oracle_eval(d167, None) == "+00:00"


def test_tstz_expression_keeps_null_as_null():
    assert _oracle_eval(oracle_tz_to_char('"C"', "tz"), None) is None


def test_typed_projection_ltz_is_null_guarded():
    table = sa.Table(
        "t", sa.MetaData(), sa.Column("ltz", oracle.TIMESTAMP(local_timezone=True))
    )
    projected = _tz_safe_projection({"type": "oracle"}, list(table.c))
    sql = str(
        sa.select(*projected).compile(
            dialect=oracle.dialect(), compile_kwargs={"literal_binds": True}
        )
    )
    expr = re.search(r"SELECT (.*) AS ltz", sql, re.S).group(1).replace("t.ltz", '"C"')
    assert _oracle_eval(expr, None) is None, sql
    assert _oracle_eval(expr, WALL) == "2024-01-01T08:31:00.123456+00:00"
