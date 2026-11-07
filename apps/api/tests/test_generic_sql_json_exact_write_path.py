"""Generic-SQL JSON DDL uses _ExactJSON, not bare sa.JSON().

stdlib sa.JSON bind re-parses with json.loads, so 1.234567890123456789
collapsed to IEEE before MySQL / SQLite / MSSQL stored the document.
IEEE-exact 1.5 stays a JSON number. The JSON string \"1\" stays a string.
PostgreSQL still uses JSONB. Oracle/ClickHouse stay TEXT.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import sqlalchemy as sa

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.generic_sql import (  # noqa: E402
    _DuckDBJSON,
    _ExactJSON,
    _sa_type_for_logical,
    _to_sa_value,
)

LONG = "1.234567890123456789"
_EXACT_ENGINES = (
    ("mysql", "mysql"),
    ("mysql", "mariadb"),
    ("mysql", "tidb"),
    ("mysql", "singlestore"),
    ("sqlite", "sqlite"),
    ("mssql", "sqlserver"),
    ("duckdb", "duckdb"),
)


def test_exact_json_is_duckdb_alias():
    assert _DuckDBJSON is _ExactJSON


def test_sa_type_for_logical_json_is_exact_on_generic_engines():
    for dialect, db in _EXACT_ENGINES:
        sa_t = _sa_type_for_logical("JSON", dialect, db)
        assert isinstance(sa_t, _ExactJSON), (dialect, db, type(sa_t))
        assert isinstance(sa_t, sa.JSON)


def test_exact_json_survives_the_dialect_impl_swap(tmp_path):
    """A real INSERT must land a document, not a quoted JSON string.

    SQLAlchemy replaces ``sa.JSON`` with the dialect's own JSON type through
    ``colspecs``, and that impl re-serialized the canonical text this type
    already produced — every generic-SQL JSON column read back as
    ``"{\\"tier\\":\\"gold\\"}"``, so a JSON destination silently became text.
    """
    engine = sa.create_engine(f"sqlite:///{tmp_path/'j.db'}")
    md = sa.MetaData()
    t = sa.Table("j", md, sa.Column("payload", _sa_type_for_logical("JSON", "sqlite", "sqlite")))
    md.create_all(engine)
    doc = '{"tags":["a","b"],"tier":"gold"}'
    with engine.begin() as conn:
        conn.execute(t.insert(), [{"payload": doc}])
    with engine.connect() as conn:
        stored = conn.exec_driver_sql("SELECT payload FROM j").scalar_one()
    assert stored == doc
    assert json.loads(stored) == {"tags": ["a", "b"], "tier": "gold"}


def test_sa_type_for_logical_json_stays_jsonb_on_postgres():
    sa_t = _sa_type_for_logical("JSON", "postgresql", "postgresql")
    assert not isinstance(sa_t, _ExactJSON)
    assert "JSONB" in type(sa_t).__name__.upper()


def test_sa_type_for_logical_json_stays_text_on_oracle_clickhouse():
    for dialect, db in (("oracle", "oracle"), ("clickhouse", "clickhouse")):
        sa_t = _sa_type_for_logical("JSON", dialect, db)
        # ClickHouse spells a nullable column ``Nullable(String)``, so the text
        # verdict lives one wrapper down — unwrap rather than accept the outer
        # type, or a JSON impl smuggled inside would pass unnoticed.
        inner = getattr(sa_t, "nested_type", sa_t)
        assert isinstance(inner, (sa.Text, sa.String)), (dialect, type(sa_t))
        assert not isinstance(inner, _ExactJSON)
        assert not isinstance(sa_t, _ExactJSON)


def test_exact_json_bind_keeps_long_fraction_and_string_one():
    proc = _ExactJSON(none_as_null=True).bind_processor(None)
    raw = f'{{"amt": {LONG}, "n": 1.5, "s": "1"}}'
    assert proc(raw) == raw
    collapsed = json.dumps(json.loads(raw), ensure_ascii=False, separators=(",", ":"))
    assert LONG in proc(raw)
    assert LONG not in collapsed
    assert proc('"1"') == '"1"'
    assert proc(None) is None


def test_to_sa_value_exact_json_keeps_engine_text():
    sa_t = _sa_type_for_logical("JSON", "mysql", "mysql")
    raw = f'{{"amt": {LONG}}}'
    bound = _to_sa_value(raw, "JSON", sa_t, "mysql", "mysql")
    assert bound == raw
    proc = sa_t.bind_processor(None)
    assert proc(bound) == raw
    assert LONG in proc(bound)
