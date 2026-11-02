"""Keyset resume uses present_cell_text, not ``if cursor_after`` / ``str()``.

``if cursor_after`` dropped integer ``0`` (first page forever). Reader-null
sentinels are truthy and became ``WHERE col > '__DF_SQL_NULL__'``.
``str(True)`` invented ``True`` so dest ``true`` missed the resume.
Incremental empty-string watermarks stay a different polarity.
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.salesforce import soql_literal  # noqa: E402
from services.keyset_pagination import (  # noqa: E402
    KEYSET_SEP,
    present_cursor_bookmark,
)
from services.value_serializer import (  # noqa: E402
    DF_MISSING_SENTINEL,
    SQL_NULL_SENTINEL,
    Missing,
)

_ABSENT = (None, "", "   ", SQL_NULL_SENTINEL, "__df_ddb_null__", Missing, DF_MISSING_SENTINEL)


def test_present_cursor_bookmark_absent_is_first_page():
    for wire in _ABSENT:
        assert present_cursor_bookmark(wire) is None, wire


def test_present_cursor_bookmark_zero_and_true_stay():
    assert present_cursor_bookmark(0) == "0"
    assert present_cursor_bookmark(False) == "false"
    assert present_cursor_bookmark(True) == "true"
    assert present_cursor_bookmark("true") == "true"
    assert present_cursor_bookmark(True) != "True"
    assert present_cursor_bookmark(Decimal("1E+2")) == "100"


def test_present_cursor_bookmark_keeps_composite():
    raw = f"2024-01-01{KEYSET_SEP}true"
    assert present_cursor_bookmark(raw) == raw


def test_soql_literal_absent_is_none():
    for wire in _ABSENT:
        assert soql_literal(wire) is None, wire


def test_soql_literal_dest_wire_and_escape():
    assert soql_literal(0) == "'0'"
    assert soql_literal(True) == "'true'"
    assert soql_literal("true") == "'true'"
    assert soql_literal("a' OR Name != '") == "'a\\' OR Name != \\''"
    assert soql_literal("001A") == "'001A'"


def test_salesforce_sentinel_bookmark_is_first_page():
    from tests.test_salesforce_keyset_pagination import _run

    _, queries = _run(limit=200, cursor_column="Id", cursor_after=SQL_NULL_SENTINEL)
    assert queries
    assert "WHERE" not in queries[0]


def test_salesforce_zero_and_true_seek_on_dest_wire():
    from tests.test_salesforce_keyset_pagination import _run

    _, q0 = _run(limit=10, cursor_column="Id", cursor_after=0)
    assert "WHERE Id > '0'" in q0[0]
    _, qt = _run(limit=10, cursor_column="Id", cursor_after=True)
    assert "WHERE Id > 'true'" in qt[0]
    assert "WHERE Id > 'True'" not in qt[0]
