"""C07 regression: JSON fidelity cluster (D74, D75, D73).

- D74: a high-precision JSON number must not degrade to a JSON *string* on the
  wire — ``{"pi": 3.1415…}`` stored as ``{"pi": "3.1415…"}`` retypes the leaf.
- D75: jsonb ``null`` must stay distinct from SQL NULL on every population
  read — psycopg2 decodes jsonb natively, collapsing ``'null'::jsonb`` to
  ``None``.
- D73: JSON compare is semantic — ``3.0`` ≡ ``3`` after a Snowflake VARIANT
  round-trip, and key order/whitespace never mismatches; ``3.0001`` still
  differs.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

PI_EXACT = "3.14159265358979323846264338327950288"


def _digits_only(text: str) -> str:
    return text.replace(" ", "")


@pytest.mark.parametrize("engine", ["sqlite", "postgres_bind", "postgres_copy"])
def test_json_number_precision_preserved(engine: str):
    """A Decimal leaf inside a document stays a JSON number, full digits."""
    doc = {"pi": Decimal(PI_EXACT)}
    if engine == "sqlite":
        from connectors.sqlite_writer import _to_sqlite_value

        text = _to_sqlite_value(doc, "JSON")
    elif engine == "postgres_bind":
        from connectors.sql_bind import coerce_json_wire

        text = coerce_json_wire(doc, as_text=True)
    else:
        from connectors.postgresql_writer import _copy_text_value

        text = _copy_text_value(doc)
    assert isinstance(text, str)
    assert f'"pi":{PI_EXACT}' in _digits_only(text)
    assert '"pi": "' not in text and '"pi":"' not in _digits_only(text)


def test_json_document_text_from_cell_to_string():
    """cell_to_string on a decoded document keeps numbers numeric."""
    from services.value_serializer import cell_to_string

    text = cell_to_string({"pi": Decimal(PI_EXACT), "n": Decimal("3.0")})
    assert '"pi":3.14159265358979323846264338327950288' in _digits_only(text)
    assert '"n":3.0' in _digits_only(text)


def test_jsonb_null_vs_sql_null_pg(monkeypatch):
    """The ladder population read projects jsonb as engine text.

    Without ``col::text`` psycopg2 hands back Python ``None`` for
    ``'null'::jsonb`` — the fingerprint then sees SQL NULL and a written JSON
    null either disappears or false-fails (D75).
    """
    import connectors.postgresql_reader as pg_reader
    from services import verification_ladder

    queries: list[str] = []

    def _render(node):
        """Offline psycopg2 Composable renderer — Identifier/SQL only."""
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

    class _Cur:
        def __init__(self):
            self.itersize = 0
            self.description = None
            self._cat_rows: list = []
            self._data_rows: list = [(1, "null")]

        def execute(self, q, params=None):
            text = _render(q)
            queries.append(text)
            if "information_schema" in text:
                self._cat_rows = [("id", "int4", "int4"), ("doc", "jsonb", "jsonb")]
            else:
                self.description = [("id",), ("doc",)]

        def fetchall(self):
            return self._cat_rows

        def fetchmany(self, n):
            rows, self._data_rows = self._data_rows, []
            return rows

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    class _Conn:
        def cursor(self, name=None):
            return _Cur()

        def close(self):
            pass

    monkeypatch.setattr(
        "connectors.postgresql_conn.get_connection", lambda **_: _Conn()
    )
    monkeypatch.setattr(
        pg_reader, "_json_column_names", lambda cur, schema, table: {"doc"}
    )

    rows = verification_ladder.read_postgres_rows(
        host="h",
        port=5432,
        database="d",
        username="u",
        password="p",
        schema="public",
        table="t",
    )
    data_sql = [q for q in queries if "FROM" in q and "information_schema" not in q]
    assert data_sql, "no data SELECT issued"
    assert "CASE WHEN" in data_sql[-1] and "doc::text" in data_sql[-1]
    assert rows == [{"id": "1", "doc": "null"}]


def test_json_semantic_compare_trailing_zero_snowflake():
    """Snowflake VARIANT reads ``3.0`` back as ``3`` — same JSON number (D73)."""
    from services.reconciliation import normalize_cell

    assert normalize_cell('{"n": 3.0}', ddl_type="VARIANT", engine="snowflake") == (
        normalize_cell('{"n": 3}', ddl_type="VARIANT", engine="snowflake")
    )
    assert normalize_cell('{"n": 3.0}') != normalize_cell('{"n": 3.0001}')


def test_json_snowflake_digest_no_unexplained_mismatch():
    """Gate-8 fingerprint: key order + trailing-zero never mismatch (D73b)."""
    from services.reconciliation import fingerprint_for_reconcile

    source = fingerprint_for_reconcile('{"a": 1, "n": 3.0, "s": "x"}')
    readback = fingerprint_for_reconcile('{"n":3,"a":1,"s":"x"}')
    assert source == readback
    assert fingerprint_for_reconcile('{"n": 3.0}') != fingerprint_for_reconcile(
        '{"n": "3.0"}'
    )


def test_json_to_jsonb_verdict_is_normalizing_not_preserve():
    """json→jsonb re-serializes the document — not a verbatim preserve."""
    from services.mapping_proof import mapping_fidelity

    verdict = mapping_fidelity(
        {"source": "doc", "target": "doc", "transform": "none"},
        declared_source_type="json",
        declared_target_type="jsonb",
        destination_db_type="postgres",
    )
    assert verdict["verdict"] == "normalizing"
    assert "jsonb" in verdict["reason"]
    # Reverse direction normalizes too.
    verdict2 = mapping_fidelity(
        {"source": "doc", "target": "doc", "transform": "none"},
        declared_source_type="jsonb",
        declared_target_type="json",
        destination_db_type="postgres",
    )
    assert verdict2["verdict"] == "normalizing"


def test_json_same_type_still_preserve():
    """json→json is still a verbatim preserve — the label stays honest."""
    from services.mapping_proof import mapping_fidelity

    verdict = mapping_fidelity(
        {"source": "doc", "target": "doc", "transform": "none"},
        declared_source_type="json",
        declared_target_type="json",
        destination_db_type="postgres",
    )
    assert verdict["verdict"] == "preserve"


def test_canonical_json_text_units():
    from services.value_serializer import canonical_json_text

    assert canonical_json_text('{"n": 3.0}') == canonical_json_text('{"n": 3}')
    assert canonical_json_text('{"n": 3.0001}') != canonical_json_text('{"n": 3}')
    assert canonical_json_text('{"b": 1, "a": 2}') == canonical_json_text(
        '{"a":2,"b":1}'
    )
    assert canonical_json_text("null") == "null"
    assert canonical_json_text("not json") is None


def test_pg_ladder_named_cursor_executes_once(monkeypatch):
    """Regression: C07's json projection ran the introspection query on the
    server-side cursor, then issued the population SELECT on it — psycopg2
    named cursors accept exactly one execute() (ProgrammingError: can't call
    .execute() on named cursors more than once). Introspection must use a
    separate normal cursor."""
    import services.verification_ladder as verification_ladder
    from connectors import postgresql_reader as pg_reader

    def _render(q):
        try:
            return q.as_string(None)  # psycopg2.sql.Composable
        except Exception:
            return str(q)

    named_executes = []

    class _Cur:
        def __init__(self, name):
            self.itersize = 0
            self.description = None
            self._name = name
            self._data_rows = [(1, "null")]

        def execute(self, q, params=None):
            if self._name is not None:
                named_executes.append(_render(q))
                assert len(named_executes) == 1, (
                    "named cursor must run exactly one execute — introspection "
                    "belongs on a separate cursor"
                )
            text = _render(q)
            if "information_schema" in text:
                self.description = [("column_name",)]
                self._cat_rows = [("id",), ("doc",)]
            else:
                self.description = [("id",), ("doc",)]

        def fetchall(self):
            return getattr(self, "_cat_rows", [])

        def fetchmany(self, n):
            rows, self._data_rows = self._data_rows, []
            return rows

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    class _Conn:
        def cursor(self, name=None):
            return _Cur(name)

        def close(self):
            pass

    monkeypatch.setattr(
        "connectors.postgresql_conn.get_connection", lambda **_: _Conn()
    )
    monkeypatch.setattr(
        pg_reader, "_json_column_names", lambda cur, schema, table: {"doc"}
    )

    rows = verification_ladder.read_postgres_rows(
        host="h",
        port=5432,
        database="d",
        username="u",
        password="p",
        schema="public",
        table="t",
    )
    assert rows == [{"id": "1", "doc": "null"}]
    assert len(named_executes) == 1
