"""The writer backstop catches every undecoded marker (review F5).

``refuse_unbound_marker`` skipped every ``str`` subclass, so a marked reader
cell (``WireCell``) holding the NULL marker — the shape of an undecoded NULL —
was bound as text, and the PostgreSQL COPY paths (``_copy_rows`` and the
``_copy_rows_temp`` staging used by upsert/merge) had no guard at all.

* bind paths refuse a plain or marked marker and any cell still carrying the
  escape prefix; a decoded user literal (``WireLiteral``) still binds;
* COPY reads a NULL marker as SQL NULL by design (``\\N``), so it refuses only a
  cell that still carries the escape prefix (an escape nobody decoded).
"""

from __future__ import annotations

import pytest

from services.null_marker_guard import NullMarkerLeak, refuse_unbound_marker
from services.value_serializer import ESCAPED_WIRE_PREFIX, SQL_NULL_SENTINEL, WireCell, WireLiteral


@pytest.mark.parametrize(
    "cell",
    [
        WireCell(SQL_NULL_SENTINEL),
        WireCell("__DF_MISSING__"),
        WireCell(ESCAPED_WIRE_PREFIX + SQL_NULL_SENTINEL),
        SQL_NULL_SENTINEL,
        ESCAPED_WIRE_PREFIX + "x",
    ],
)
def test_bind_refuses_an_undecoded_marker(cell):
    with pytest.raises(NullMarkerLeak):
        refuse_unbound_marker([("a", cell)], ["k", "v"], table="t")


def test_bind_keeps_values_and_decoded_literals():
    refuse_unbound_marker([
        ("a", None, WireLiteral(SQL_NULL_SENTINEL), WireLiteral(ESCAPED_WIRE_PREFIX + "x"), WireCell("plain")),
    ])


class _Cur:
    def __init__(self):
        self.copied = None

    def copy_expert(self, _sql, buf):
        self.copied = buf.read()


@pytest.mark.parametrize("fn", ["_copy_rows", "_copy_rows_temp"])
def test_copy_refuses_an_escape_nobody_decoded(fn):
    from connectors import postgresql_writer as pw

    cur = _Cur()
    args = (cur, "public", "t") if fn == "_copy_rows" else (cur, "t")
    with pytest.raises(NullMarkerLeak):
        getattr(pw, fn)(*args, ["k", "v"], [("a", ESCAPED_WIRE_PREFIX + SQL_NULL_SENTINEL)])
    assert cur.copied is None


@pytest.mark.parametrize("fn", ["_copy_rows", "_copy_rows_temp"])
def test_copy_writes_null_marker_as_null_and_a_literal_as_text(fn):
    from connectors import postgresql_writer as pw

    cur = _Cur()
    args = (cur, "public", "t") if fn == "_copy_rows" else (cur, "t")
    getattr(pw, fn)(*args, ["k", "v", "w"], [("a", WireCell(SQL_NULL_SENTINEL), WireLiteral(SQL_NULL_SENTINEL))])
    assert cur.copied == f"a\t\\N\t{SQL_NULL_SENTINEL}\n"
