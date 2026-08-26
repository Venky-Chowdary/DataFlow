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


def test_sa_type_for_logical_json_stays_jsonb_on_postgres():
    sa_t = _sa_type_for_logical("JSON", "postgresql", "postgresql")
    assert not isinstance(sa_t, _ExactJSON)
    assert "JSONB" in type(sa_t).__name__.upper()


def test_sa_type_for_logical_json_stays_text_on_oracle_clickhouse():
    for dialect, db in (("oracle", "oracle"), ("clickhouse", "clickhouse")):
        sa_t = _sa_type_for_logical("JSON", dialect, db)
        assert isinstance(sa_t, (sa.Text, sa.String)), (dialect, type(sa_t))
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
