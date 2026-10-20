"""X-72/X-73: PG INTERVAL must travel as engine text — never a driver
``timedelta`` that flattens ``1 year 2 mons`` into ~428 days of hours — and
a signed ``-HH:MM:SS`` wire is a negative interval, not a refused payload.
"""

from __future__ import annotations


class _FakeCur:
    """Minimal cursor stub for information_schema-driven readers."""

    def __init__(self, columns: list[tuple]):
        self._columns = columns

    def execute(self, query, params=None):
        self._last_query = query

    def fetchall(self):
        return self._columns

    def fetchone(self):
        return self._columns[0] if self._columns else None


def test_interval_columns_project_as_engine_text():
    from connectors.postgresql_reader import _engine_text_column_names

    cur = _FakeCur(
        [
            ("id", "integer", "int4"),
            ("dur", "interval", "interval"),
            ("payload", "jsonb", "jsonb"),
        ]
    )
    names = _engine_text_column_names(cur, "public", "t")
    assert names == {"dur", "payload"}


def test_select_list_casts_interval_columns_to_text():
    from connectors.postgresql_reader import _select_list

    cols = [
        ("id", "integer", "int4"),
        ("dur", "interval", "interval"),
    ]
    col_sql = _select_list(
        _FakeCur(cols), "public", "t", ["id", "dur"], identity=""
    )
    assert col_sql is not None

    def _strings(node):
        seq = getattr(node, "seq", None)
        if seq is None:
            yield getattr(node, "string", str(node))
            return
        for part in seq:
            yield from _strings(part)

    rendered = " ".join(_strings(col_sql))
    assert "::text" in rendered


def test_signed_interval_wire_is_interval_not_refusal():
    from services.schema_inference import is_interval_wire

    assert is_interval_wire("-00:00:01")
    assert is_interval_wire("+25:00:00")
    assert is_interval_wire("100:00:00")
    # A plain in-range clock stays ambiguous → not interval.
    assert not is_interval_wire("10:30:00")


def test_pg_component_interval_wire_passes_bind():
    from connectors.sql_bind import coerce_interval_wire

    wire = "1 year 2 mons 3 days 04:05:06.789"
    bound = coerce_interval_wire(wire, ddl_type="INTERVAL", engine="duckdb")
    assert bound == wire
    assert coerce_interval_wire(
        "-00:00:01", ddl_type="INTERVAL", engine="duckdb"
    ) == "-00:00:01"
