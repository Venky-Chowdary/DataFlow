"""Wave 69: GEOGRAPHY polarity fix + SQL_VARIANT envelope + Oracle XML/ROWID."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_geography_uppercase_preserves_postgis_geography():
    from services.type_system import (
        ddl_type,
        geography_contract_would_collapse,
        spatial_polarity,
    )

    assert spatial_polarity("GEOGRAPHY") == "geography"
    assert ddl_type("postgresql", "GEOGRAPHY") == "GEOGRAPHY"
    assert ddl_type("postgresql", "GEOMETRY") == "GEOMETRY"
    # Bare logical alias still uses platform default (GEOMETRY on PG).
    assert ddl_type("postgresql", "geography") == "GEOMETRY"
    assert geography_contract_would_collapse("GEOGRAPHY", "GEOMETRY") is True
    assert geography_contract_would_collapse("GEOGRAPHY", "GEOGRAPHY") is False


def test_sql_variant_introspect_ddl_and_envelope_bind():
    from connectors.sql_bind import coerce_sql_variant_wire, normalize_sql_bind_value
    from services.schema_introspect import _sqlserver_to_logical
    from services.type_system import ddl_type, sql_variant_would_collapse

    assert _sqlserver_to_logical("sql_variant") == "SQL_VARIANT"
    assert ddl_type("sqlserver", "SQL_VARIANT") == "SQL_VARIANT"
    assert ddl_type("postgresql", "SQL_VARIANT") == "JSONB"
    assert ddl_type("snowflake", "SQL_VARIANT") == "VARIANT"
    assert ddl_type("mysql", "SQL_VARIANT") == "VARCHAR(8000)"

    assert sql_variant_would_collapse("SQL_VARIANT", "VARCHAR(8000)") is True
    assert sql_variant_would_collapse("SQL_VARIANT", "JSONB") is False

    env = coerce_sql_variant_wire(42, as_json_envelope=True)
    assert env == {"sql_variant_base": "bigint", "value": 42}
    # The envelope reaches JSONB as text — psycopg2 cannot adapt a dict (wave 88).
    # Content is preserved exactly; only the wire form is serialized.
    bound = normalize_sql_bind_value(42, "SQL_VARIANT", engine="postgresql")
    assert json.loads(bound) == env
    assert normalize_sql_bind_value(42, "SQL_VARIANT", engine="sqlserver") == 42


def test_oracle_xmltype_and_rowid_carriers():
    from connectors.sql_bind import normalize_sql_bind_value
    from services.schema_introspect import _oracle_to_logical
    from services.type_system import ddl_type

    assert _oracle_to_logical("XMLTYPE") == "XMLTYPE"
    assert _oracle_to_logical("ROWID") == "ROWID"
    assert _oracle_to_logical("UROWID") == "UROWID"
    assert ddl_type("oracle", "XMLTYPE") == "XMLTYPE"
    assert ddl_type("postgresql", "XMLTYPE") == "XML"
    assert ddl_type("oracle", "ROWID") == "ROWID"
    assert ddl_type("postgresql", "ROWID") == "VARCHAR(18)"

    assert normalize_sql_bind_value(
        "AAAVqEAAEAAAAG+AAA", "ROWID", engine="oracle"
    ) == "AAAVqEAAEAAAAG+AAA"
    with pytest.raises(ValueError, match="refuse invent"):
        normalize_sql_bind_value(123, "ROWID", engine="oracle")


def test_mssql_geography_create_new_pg():
    from services.schema_introspect import _sqlserver_to_logical
    from services.type_system import ddl_type

    assert _sqlserver_to_logical("geography") == "GEOGRAPHY"
    assert ddl_type("postgresql", _sqlserver_to_logical("geography")) == "GEOGRAPHY"
