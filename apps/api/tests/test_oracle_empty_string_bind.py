"""Oracle '' → NULL write bind (VARCHAR2 / HVR write-location coercion)."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.sql_bind import normalize_sql_bind_value  # noqa: E402


def test_oracle_bind_empty_string_becomes_null():
    assert normalize_sql_bind_value("", "VARCHAR2(100)", engine="oracle") is None
    assert normalize_sql_bind_value("", "NVARCHAR2(50)", engine="oracledb") is None
    assert normalize_sql_bind_value("", "CHAR(1)", engine="oracle") is None


def test_postgres_bind_keeps_empty_string():
    assert normalize_sql_bind_value("", "VARCHAR(100)", engine="postgresql") == ""
    assert normalize_sql_bind_value("", "TEXT", engine="mysql") == ""


def test_oracle_bind_nonempty_passthrough():
    assert normalize_sql_bind_value("hi", "VARCHAR2(10)", engine="oracle") == "hi"


def test_numeric_bind_refuses_empty_null_invent():
    """INT/FLOAT/DECIMAL empty must raise — never invent SQL NULL (upsert wipe)."""
    import pytest
    from connectors.sql_bind import (
        coerce_decimal_wire,
        coerce_float_wire,
        coerce_integer_wire,
        coerce_json_wire,
        coerce_citext_wire,
        coerce_year_wire,
        coerce_inet_wire,
    )

    for ddl in ("INTEGER", "INT", "BIGINT", "SMALLINT"):
        with pytest.raises(ValueError, match="refuse silent NULL invent"):
            normalize_sql_bind_value("", ddl, engine="postgresql")
        with pytest.raises(ValueError, match="refuse silent NULL invent"):
            normalize_sql_bind_value("  ", ddl, engine="mysql")
        with pytest.raises(ValueError, match="refuse silent NULL invent"):
            coerce_integer_wire("", ddl_type=ddl)

    for ddl in ("FLOAT", "DOUBLE", "REAL", "DOUBLE PRECISION"):
        with pytest.raises(ValueError, match="refuse silent NULL invent"):
            normalize_sql_bind_value("", ddl, engine="postgresql")
        with pytest.raises(ValueError, match="refuse silent NULL invent"):
            coerce_float_wire("", ddl_type=ddl)

    for ddl in ("DECIMAL", "DECIMAL(10,2)", "NUMERIC(8,2)"):
        with pytest.raises(ValueError, match="refuse silent NULL invent"):
            normalize_sql_bind_value("", ddl, engine="mysql")
        with pytest.raises(ValueError, match="refuse silent NULL invent"):
            coerce_decimal_wire("", ddl_type=ddl)

    with pytest.raises(ValueError, match="refuse silent NULL invent"):
        coerce_json_wire("")
    with pytest.raises(ValueError, match="refuse silent NULL invent"):
        coerce_year_wire("")
    with pytest.raises(ValueError, match="refuse silent NULL invent"):
        coerce_inet_wire("")
    for coerce, label in (
        (lambda: normalize_sql_bind_value("", "TEXT[]", engine="postgresql"), "ARRAY"),
        (lambda: normalize_sql_bind_value("", "STRUCT<a:INT>", engine="bigquery"), "STRUCT"),
        (lambda: normalize_sql_bind_value("", "MAP<STRING,STRING>", engine="spark"), "MAP"),
    ):
        with pytest.raises(ValueError, match="refuse silent NULL invent"):
            coerce()
    # CITEXT keeps empty string (VARCHAR-class).
    assert coerce_citext_wire("") == ""

    from connectors.sql_bind import (
        coerce_macaddr_wire,
        coerce_hstore_wire,
        coerce_tsvector_wire,
        coerce_pg_lsn_wire,
        coerce_oid_wire,
        coerce_point_wire,
        coerce_bitstring_wire,
    )

    for fn, _label in (
        (coerce_macaddr_wire, "MACADDR"),
        (coerce_hstore_wire, "HSTORE"),
        (coerce_pg_lsn_wire, "PG_LSN"),
        (coerce_oid_wire, "OID"),
        (coerce_point_wire, "POINT"),
        (coerce_bitstring_wire, "BIT"),
    ):
        with pytest.raises(ValueError, match="refuse silent NULL invent"):
            fn("")
    assert coerce_tsvector_wire("") == ""

    from connectors.hubspot_writer import coerce_hubspot_datetime_wire
    from connectors.salesforce_writer import coerce_salesforce_id_wire

    with pytest.raises(ValueError, match="refuse silent NULL invent"):
        coerce_hubspot_datetime_wire("")
    with pytest.raises(ValueError, match="refuse silent NULL invent"):
        coerce_salesforce_id_wire("")


def test_temporal_bind_refuses_empty_null_invent():
    """DATE/DATETIME/TIME empty must raise — never invent SQL NULL / zero-date."""
    import pytest
    from connectors.sql_bind import normalize_sql_bind_value
    from connectors.sql_temporal import coerce_sql_temporal, wire_check_temporal
    from services.type_system import boolean_value_fits

    for ddl in ("DATE", "DATETIME", "TIMESTAMP", "TIME", "TIMESTAMPTZ"):
        with pytest.raises(ValueError, match="refuse silent NULL invent"):
            coerce_sql_temporal("", ddl)
        with pytest.raises(ValueError, match="refuse silent NULL invent"):
            normalize_sql_bind_value("", ddl, engine="postgresql")
        check = wire_check_temporal("", ddl)
        assert check["ok"] is False
        assert "refuse silent NULL invent" in (check.get("reason") or "")

    assert boolean_value_fits("") is False
    assert boolean_value_fits(None) is True
    assert boolean_value_fits("true") is True


def test_bind_sql_mapped_rows_quarantines_empty_integer():
    from connectors.writer_common import bind_sql_mapped_rows_with_quarantine

    details: list[dict] = []
    out = bind_sql_mapped_rows_with_quarantine(
        [("", "ok"), ("30", "ok")],
        ["age", "name"],
        ["INTEGER", "VARCHAR"],
        details,
        "quarantine",
        engine="postgresql",
        dialect_label="PostgreSQL",
    )
    assert out == [(30, "ok")]
    assert details
    assert any("refuse silent NULL invent" in str(d.get("reason") or "") for d in details)


def test_overlay_physical_bind_types_promotes_map_varchar():
    from connectors.writer_common import overlay_physical_bind_types

    out = overlay_physical_bind_types(
        ["id", "created", "age", "flag"],
        ["VARCHAR", "VARCHAR", "VARCHAR", "TEXT"],
        {
            "created": "DATE",
            "age": "INTEGER",
            "flag": "BOOLEAN",
        },
    )
    assert out[1] == "DATE"
    assert out[2] == "INTEGER"
    assert out[3] == "BOOLEAN"
    assert out[0] == "VARCHAR"


def test_run_sparse_cdc_upsert_quarantines_empty_pk():
    from connectors.writer_common import run_sparse_cdc_upsert
    from services.value_serializer import DF_MISSING_SENTINEL

    details: list[dict] = []
    written, skipped, checksum = run_sparse_cdc_upsert(
        target_cols=["id", "note"],
        conflict_columns=["id"],
        sparse_rows=[("", "bad"), ("2", "ok")],
        fetch_existing_row=lambda pk: ("2", "old") if pk == ["2"] else None,
        update_non_pk=lambda non_pk, pk: 1,
        insert_present=lambda present: None,
        rejected_details=details,
        policy="quarantine",
    )
    assert written == 1
    assert details
    assert any("null/empty primary-key" in str(d.get("reason") or "") for d in details)
    del DF_MISSING_SENTINEL, skipped, checksum


def test_snowflake_sparse_bind_quarantines_empty_date():
    """Sparse CDC empty DATE must quarantine — not abort whole Snowflake write."""
    from services.value_serializer import DF_MISSING_SENTINEL
    from connectors.writer_common import bind_sql_mapped_rows_with_quarantine

    details: list[dict] = []
    out = bind_sql_mapped_rows_with_quarantine(
        [
            ("1", "", "keep"),
            ("2", "2024-01-15", DF_MISSING_SENTINEL),
        ],
        ["id", "created", "note"],
        ["VARCHAR", "DATE", "VARCHAR"],
        details,
        "quarantine",
        engine="snowflake",
        dialect_label="Snowflake",
    )
    assert len(out) == 1
    assert out[0][0] == "2"
    assert details
    assert any("refuse silent NULL invent" in str(d.get("reason") or "") for d in details)


def test_quarantine_unfit_temporals_refuses_empty():
    from connectors.writer_common import quarantine_unfit_temporals

    details: list[dict] = []
    out = quarantine_unfit_temporals(
        [("", "ok"), ("2024-01-15", "ok")],
        ["created", "name"],
        ["DATE", "VARCHAR"],
        details,
        "quarantine",
    )
    assert len(out) == 1
    assert out[0][0] == "2024-01-15"
    assert any("refuse silent NULL invent" in str(d.get("reason") or "") for d in details)


def test_elasticsearch_date_refuses_empty():
    import pytest
    from connectors.elasticsearch_writer import _to_es_value

    with pytest.raises(ValueError, match="refuse silent null invent"):
        _to_es_value("", "DATE")
    with pytest.raises(ValueError, match="refuse silent null invent"):
        _to_es_value("  ", "TIMESTAMP")
    # Non-empty date still coerces.
    from datetime import date

    assert _to_es_value("2024-06-01", "DATE") == date(2024, 6, 1)


def test_mongodb_decimal_empty_refuses_via_coerce():
    import pytest
    from connectors.sql_bind import coerce_decimal_wire

    with pytest.raises(ValueError, match="refuse silent NULL invent"):
        coerce_decimal_wire("")
    with pytest.raises(ValueError, match="refuse silent NULL invent"):
        coerce_decimal_wire("  ")


def test_iceberg_float_overlay_from_arrow_schema():
    """Physical float Arrow types must enter quarantine carriers (Map VARCHAR cliff)."""
    from connectors.iceberg_writer import _decimal_target_types_for_iceberg_write

    class _FakeType:
        def __init__(self, kind: str):
            self.kind = kind
            self.tz = None

    class _FakeField:
        def __init__(self, name: str, kind: str):
            self.name = name
            self.type = _FakeType(kind)

    class _FakeSchema:
        names = ["id", "amt"]

        def field(self, name: str):
            return _FakeField(name, "float64" if name == "amt" else "string")

    class _FakeTypes:
        @staticmethod
        def is_decimal(_t):
            return False

        @staticmethod
        def is_fixed_size_binary(_t):
            return False

        @staticmethod
        def is_int32(_t):
            return False

        @staticmethod
        def is_int16(_t):
            return False

        @staticmethod
        def is_int64(_t):
            return False

        @staticmethod
        def is_boolean(_t):
            return False

        @staticmethod
        def is_date(_t):
            return False

        @staticmethod
        def is_timestamp(_t):
            return False

        @staticmethod
        def is_time(_t):
            return False

        @staticmethod
        def is_floating(t):
            return getattr(t, "kind", "") in {"float64", "float32"}

        @staticmethod
        def is_float64(t):
            return getattr(t, "kind", "") == "float64"

        @staticmethod
        def is_binary(_t):
            return False

        @staticmethod
        def is_large_binary(_t):
            return False

        @staticmethod
        def is_string(t):
            return getattr(t, "kind", "") == "string"

        @staticmethod
        def is_large_string(_t):
            return False

    class _FakePa:
        types = _FakeTypes()

    out = _decimal_target_types_for_iceberg_write(
        ["id", "amt"],
        {"id": "string", "amt": "string"},
        arrow_schema=_FakeSchema(),
        pa_mod=_FakePa(),
    )
    assert out[1] == "DOUBLE"
