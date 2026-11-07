"""C08 regression: Postgres rich types travel as engine text (D50d/D79/D80).

psycopg2's native decoders lose or rewrite PG-native spellings:
interval → timedelta (``1 mon 1 day`` decays to ``744:00:00``), real → float64
(``0.33333334`` invents digits), varbit ``B'1'`` hits the binary route and
quarantines, ranges render ``(None, 5)``, inet/macaddr re-render differently.
Every one is fixed by projecting ``col::text`` so the wire carries the
engine's own canonical text (same pattern JSON already used).
"""
from __future__ import annotations

import pytest


def _render(node):
    wrapped = getattr(node, "_wrapped", None)
    if wrapped is not None:
        return "".join(_render(part) for part in wrapped)
    if isinstance(node, str):
        return node
    raw = getattr(node, "string", "") or ""
    try:
        from psycopg2 import sql as _sql

        if isinstance(node, _sql.Identifier):
            return f'"{raw}"'
    except Exception:
        pass
    return raw or str(node)


_CATALOG = [
    ("id", "integer", "int4"),
    ("doc", "jsonb", "jsonb"),
    ("dur", "interval", "interval"),
    ("r", "real", "float4"),
    ("ip", "inet", "inet"),
    ("bits", "bit varying", "varbit"),
    ("rng", "numrange", "numrange"),
    ("ch", "character", "bpchar"),
]


class _Cur:
    def __init__(self):
        self._rows: list = list(_CATALOG)

    def execute(self, q, params=None):
        text = _render(q)
        if "information_schema" in text:
            self._rows = list(_CATALOG)
        else:
            self._rows = [r[0] for r in _CATALOG]

    def fetchall(self):
        rows, self._rows = self._rows, []
        return rows


def test_text_carry_column_names_covers_lossy_families():
    from connectors.postgresql_reader import _text_carry_column_names

    names = _text_carry_column_names(_Cur(), "public", "t")
    assert {"doc", "dur", "r", "ip", "bits", "rng"} <= names
    # char(5) travels as str already — padding is the destination's contract,
    # not a decode loss, so it stays off the projection list.
    assert "ch" not in names and "id" not in names


def test_select_list_projects_text_carry_columns():
    from connectors.postgresql_reader import _select_list

    sel = _select_list(_Cur(), "public", "t", None, "")
    text = _render(sel)
    for col in ("doc", "dur", "r", "ip", "bits", "rng"):
        assert f"{col}::text" in text, col
    assert "ch::text" not in text


@pytest.mark.parametrize("engine", ["pg", "sqlite"])
def test_pg_interval_roundtrip(engine: str):
    """'1 day 03:00:04.5' and '1 mon 1 day 00:00:01.5' travel verbatim."""
    from connectors.sql_bind import coerce_interval_wire

    ddl = "INTERVAL" if engine == "pg" else "TEXT"
    for wire in ("1 day 03:00:04.5", "1 mon 1 day 00:00:01.5"):
        bound = coerce_interval_wire(wire, ddl_type=ddl, engine=engine)
        assert str(bound) == wire


def test_varbit_any_length():
    """B'1' is a legal varbit — a bit string, not binary (D79)."""
    from connectors.sql_bind import coerce_bitstring_wire

    assert coerce_bitstring_wire("B'1'", varying=True) == "1"
    assert coerce_bitstring_wire("1", varying=True) == "1"
    assert coerce_bitstring_wire("1011001", width=8, varying=True) == "1011001"


def test_range_unbounded_text():
    """(,5) stays unbounded — never (None, 5) (D80)."""
    from connectors.sql_bind import coerce_range_wire

    assert coerce_range_wire("(,5)") == "(,5)"
    assert coerce_range_wire("[1,)") == "[1,)"
    assert coerce_range_wire("empty") == "empty"


def test_real_float32_exact_pg():
    """REAL carries its float32 spelling — no float64 digit invent."""
    from connectors.postgresql_reader import _text_carry_column_names

    names = _text_carry_column_names(_Cur(), "public", "t")
    assert "r" in names


def test_inet_char_pg_to_pg():
    from connectors.postgresql_reader import _text_carry_column_names

    names = _text_carry_column_names(_Cur(), "public", "t")
    assert "ip" in names
