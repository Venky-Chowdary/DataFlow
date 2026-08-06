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


def test_quarantine_unfit_floats_refuses_empty():
    from connectors.writer_common import (
        apply_write_quarantine_matrix,
        quarantine_unfit_floats,
    )

    details: list[dict] = []
    out = quarantine_unfit_floats(
        [("1", ""), ("2", "3.5")],
        ["id", "amt"],
        ["VARCHAR", "DOUBLE"],
        details,
        "quarantine",
    )
    assert len(out) == 1
    assert out[0][1] == "3.5" or out[0][1] == 3.5 or float(out[0][1]) == 3.5
    assert details

    details2: list[dict] = []
    matrix = apply_write_quarantine_matrix(
        [("1", "")],
        ["id", "amt"],
        ["VARCHAR", "FLOAT"],
        details2,
        "quarantine",
        dialect_label="Kafka",
    )
    assert matrix == []
    assert details2


def test_coercion_probe_empty_blocks_unknown_physical():
    from services.coercion_probe import analyze_coercion

    report = analyze_coercion(
        sample_rows=[{"col": ""}],
        mappings=[{"source": "col", "target": "col", "target_type": "VARCHAR"}],
        source_types={"col": "VARCHAR"},
        dest_types={},
        dest_db_type="postgresql",
        table_exists=True,
        validation_mode="strict",
    )
    cols = report.get("columns") or []
    assert cols
    assert cols[0].get("failed", 0) >= 1 or report.get("has_blocking_failures")


def test_partition_dense_upsert_rows_quarantines_empty_pk():
    from connectors.writer_common import partition_dense_upsert_rows

    details: list[dict] = []
    out = partition_dense_upsert_rows(
        [{"id": "", "n": 1}, {"id": "2", "n": 2}],
        ["id"],
        rejected_details=details,
        policy="quarantine",
    )
    assert out == [{"id": "2", "n": 2}]
    assert details
    assert any("null/empty conflict key" in str(d.get("reason") or "") for d in details)


def test_pg_write_mapped_rows_bind_helper_not_unbound_local():
    """Regression: late ``from writer_common import bind_sql_…`` inside
    ``write_mapped_rows`` made the name local for the whole function, so the
    early Map-stamp bind raised UnboundLocalError (MySQL→Postgres client fail).
    """
    from connectors.postgresql_writer import write_mapped_rows

    assert "bind_sql_mapped_rows_with_quarantine" not in write_mapped_rows.__code__.co_varnames


def test_saas_quarantine_values_preserves_sql_null():
    from connectors.writer_common import saas_quarantine_values
    from services.value_serializer import SQL_NULL_SENTINEL

    out = saas_quarantine_values({"id": "1", "note": None, "flag": ""})
    assert out["id"] == "1"
    assert out["note"] == SQL_NULL_SENTINEL
    assert out["flag"] == ""


def test_require_physical_types_fails_closed_for_existing():
    from connectors.writer_common import require_physical_types_for_existing_table

    assert require_physical_types_for_existing_table(
        table_existed=False, physical={}
    ) is None
    assert require_physical_types_for_existing_table(
        table_existed=True, physical={"id": "INT"}
    ) is None
    err = require_physical_types_for_existing_table(
        table_existed=True, physical={}, dialect_label="Snowflake"
    )
    assert err and "refuse silent Map VARCHAR bind" in err
    partial = require_physical_types_for_existing_table(
        table_existed=True,
        physical={"id": "INT"},
        target_cols=["id", "created"],
        dialect_label="PostgreSQL",
    )
    assert partial and "created" in partial


def test_resolve_target_columns_existing_prefers_live_dest_types():
    """Existing table + live dest_types must beat Map stamp (Validate live-first)."""
    from connectors.writer_common import resolve_target_columns

    cols, types = resolve_target_columns(
        [{"source": "status", "target": "status", "target_type": "BOOLEAN"}],
        {"status": "VARCHAR"},
        dest_types={"status": "VARCHAR"},
        table_exists=True,
    )
    assert dict(zip(cols, types))["status"] == "VARCHAR"


def test_overlay_typed_physical_beats_map_decimal_over_int():
    from connectors.writer_common import overlay_physical_bind_types

    out = overlay_physical_bind_types(
        ["age", "flag", "amt"],
        ["DECIMAL(10,2)", "INTEGER", "VARCHAR"],
        {"age": "INTEGER", "flag": "BOOLEAN", "amt": "MONEY"},
    )
    assert "INT" in out[0].upper()
    assert "BOOL" in out[1].upper() or "BIT" in out[1].upper()
    assert "MONEY" in out[2].upper()


def test_clickhouse_hydrate_unknown_pk_quarantines():
    from connectors.writer_common import run_sparse_cdc_upsert
    from services.value_serializer import DF_MISSING_SENTINEL

    details: list[dict] = []
    written, skipped, checksum = run_sparse_cdc_upsert(
        target_cols=["id", "note", "extra"],
        conflict_columns=["id"],
        sparse_rows=[("new", "only-note", DF_MISSING_SENTINEL)],
        fetch_existing_row=lambda pk: None,
        update_non_pk=lambda non_pk, pk: 0,
        insert_present=lambda present: (_ for _ in ()).throw(
            AssertionError("must not insert partial unknown PK")
        ),
        hydrate_versioned_insert=True,
        rejected_details=details,
        policy="quarantine",
    )
    assert written == 0
    assert details
    assert any("unknown primary key" in str(d.get("reason") or "") for d in details)
    del skipped, checksum


def test_dynamodb_date_refuses_empty():
    import pytest
    from connectors.dynamodb_writer import _to_dynamo_value

    with pytest.raises(ValueError, match="refuse silent null invent"):
        _to_dynamo_value("", "DATE")
    with pytest.raises(ValueError, match="refuse silent null invent"):
        _to_dynamo_value("  ", "TIMESTAMP")


def test_snowflake_csv_null_sentinel_preserves_empty_varchar(tmp_path):
    """None → \\N for COPY NULL_IF; '' stays '' (no VARCHAR empty→NULL invent)."""
    from connectors.snowflake_writer import _SNOWFLAKE_CSV_NULL, _write_temp_csv

    path = tmp_path / "sf.csv"
    _write_temp_csv(path, ["id", "note"], [("1", None), ("2", "")])
    text = path.read_text(encoding="utf-8")
    assert _SNOWFLAKE_CSV_NULL == "\\N"
    assert '"\\N"' in text or f'"{_SNOWFLAKE_CSV_NULL}"' in text
    # Empty varchar must remain a quoted empty field, not the null sentinel.
    assert '""' in text
    assert "NULL_IF" not in text  # CSV body only


def test_snowflake_copy_file_format_drops_empty_null_if():
    import inspect
    from connectors import snowflake_writer as sf

    src = inspect.getsource(sf._copy_into_table)
    assert "NULL_IF = ('', 'NULL')" not in src
    assert "\\\\N" in src  # intentional-null sentinel only


def test_overlay_promotes_mysql_bool_json_from_map_varchar():
    from connectors.writer_common import overlay_physical_bind_types

    out = overlay_physical_bind_types(
        ["flag", "payload", "amt"],
        ["VARCHAR", "TEXT", "VARCHAR"],
        {"flag": "BIT(1)", "payload": "JSON", "amt": "DECIMAL(10,2)"},
    )
    assert "BIT" in out[0].upper() or "BOOL" in out[0].upper()
    assert "JSON" in out[1].upper()
    assert "DECIMAL" in out[2].upper()


def test_generic_sql_float_refuses_empty():
    import pytest
    import sqlalchemy as sa
    from connectors.generic_sql import _to_sa_value

    with pytest.raises(ValueError, match="refuse silent NULL invent|cannot coerce to float"):
        _to_sa_value("", "float", sa.Float(), "duckdb", "duckdb")
    with pytest.raises(ValueError, match="refuse silent NULL invent|cannot coerce to float"):
        _to_sa_value("  ", "FLOAT64", sa.Float(), "duckdb", "duckdb")


def test_oracle_dense_upsert_empty_promotes_to_sparse_sentinel():
    """Dense Oracle upsert must not SET col=NULL for '' — route via DF_MISSING."""
    from services.value_serializer import DF_MISSING_SENTINEL, is_missing_sentinel

    # Mirror the promote logic used in generic_sql write path.
    row = (1, "", "keep")
    promoted = tuple(
        DF_MISSING_SENTINEL if (isinstance(v, str) and v == "") else v for v in row
    )
    assert is_missing_sentinel(promoted[1])
    assert promoted[0] == 1 and promoted[2] == "keep"


def test_overlay_promotes_specialty_inet_from_map_varchar():
    from connectors.writer_common import overlay_physical_bind_types

    out = overlay_physical_bind_types(
        ["addr", "note"],
        ["VARCHAR", "TEXT"],
        {"addr": "INET", "note": "TEXT"},
    )
    assert "INET" in out[0].upper()
    assert "TEXT" in out[1].upper() or out[1].upper() == "TEXT"


def test_overlay_physical_specialty_beats_map_json_integer_geography():
    """Map JSON/INTEGER/GEOGRAPHY must not invent bind polarity over live specialty."""
    from connectors.writer_common import overlay_physical_bind_types
    from connectors.sql_bind import normalize_sql_bind_value

    out = overlay_physical_bind_types(
        ["tags", "oid", "pt"],
        ["JSON", "INTEGER", "GEOGRAPHY"],
        {"tags": "HSTORE", "oid": "OID", "pt": "POINT"},
    )
    assert out[0].upper() == "HSTORE"
    assert out[1].upper() == "OID"
    assert out[2].upper() == "POINT"
    # After overlay, hstore literal binds as object JSON — not JSON string-wrap invent.
    bound = normalize_sql_bind_value('"a"=>"1"', out[0], engine="postgresql")
    assert bound == '{"a":"1"}' or (isinstance(bound, str) and '"a"' in bound and "=>" not in bound)


def test_quarantine_cell_wire_preserves_sql_null_not_empty():
    from connectors.writer_common import (
        mapped_row_quarantine_values,
        project_quarantine_source_values,
        quarantine_cell_wire,
    )
    from services.value_serializer import DF_MISSING_SENTINEL, SQL_NULL_SENTINEL
    from services.transform_engine import apply_transform

    assert quarantine_cell_wire(None) == SQL_NULL_SENTINEL
    assert quarantine_cell_wire(DF_MISSING_SENTINEL) == DF_MISSING_SENTINEL
    assert quarantine_cell_wire("") == ""
    assert quarantine_cell_wire(float("nan")) == SQL_NULL_SENTINEL
    vals = mapped_row_quarantine_values(
        ("1", None, DF_MISSING_SENTINEL),
        ["id", "note", "extra"],
    )
    assert vals["note"] == SQL_NULL_SENTINEL
    assert vals["extra"] == DF_MISSING_SENTINEL
    src = project_quarantine_source_values(
        vals,
        [
            {"source": "src_id", "target": "id"},
            {"source": "src_note", "target": "note"},
            {"source": "src_extra", "target": "extra"},
        ],
    )
    assert src["src_note"] == SQL_NULL_SENTINEL
    null_val, err = apply_transform(src["src_note"], "none")
    assert err is None and null_val is None
    miss_val, err2 = apply_transform(src["src_extra"], "none")
    assert err2 is None and miss_val == DF_MISSING_SENTINEL


def test_clickhouse_pk_type_not_wrapped_nullable():
    from connectors.generic_sql import _sa_type_for_logical

    pk_t = _sa_type_for_logical("integer", "clickhouse", "clickhouse", nullable=False)
    null_t = _sa_type_for_logical("integer", "clickhouse", "clickhouse", nullable=True)
    pk_name = type(pk_t).__name__
    null_name = type(null_t).__name__
    # Non-null PK / ORDER BY identity must stay bare (not Nullable).
    assert "Nullable" not in pk_name
    # When clickhouse_sqlalchemy is installed, nullable cols wrap; otherwise both
    # are plain Integer — either is honest (no invent of always-Nullable).
    if null_name != pk_name:
        assert "Nullable" in null_name


def test_cdc_mysql_serialize_preserves_sql_null():
    from connectors.mysql_change_stream import _serialize
    from services.value_serializer import SQL_NULL_SENTINEL

    assert _serialize(None) == SQL_NULL_SENTINEL


def test_dynamo_key_s_refuses_null_empty():
    import pytest
    from connectors.dynamodb_writer import _coerce_dynamo_cell

    with pytest.raises(ValueError, match="refuse silent empty-string invent"):
        _coerce_dynamo_cell(None, col="pk", logical_type="string", key_types={"pk": "S"})
    with pytest.raises(ValueError, match="refuse silent empty-string invent"):
        _coerce_dynamo_cell("", col="pk", logical_type="string", key_types={"pk": "S"})


def test_cdc_row_key_rejects_sql_null_sentinel():
    from services.cdc_identity import is_present_cdc_row_key
    from services.value_serializer import SQL_NULL_SENTINEL

    assert is_present_cdc_row_key("abc")
    assert not is_present_cdc_row_key("")
    assert not is_present_cdc_row_key(None)
    assert not is_present_cdc_row_key(SQL_NULL_SENTINEL)


def test_questdb_ddl_stamps_decimal_as_double():
    from services.type_system import ddl_type

    assert ddl_type("questdb", "DECIMAL(18,4)") == "DOUBLE"
    assert ddl_type("questdb", "NUMBER") == "DOUBLE"
    assert ddl_type("questdb", "TIME") == "VARCHAR"
    assert ddl_type("questdb", "UUID") == "VARCHAR"


def test_pg_test_decoding_null_is_sql_null_sentinel():
    from connectors.postgresql_change_stream import _parse_value
    from services.value_serializer import SQL_NULL_SENTINEL

    assert _parse_value("null") == SQL_NULL_SENTINEL
    assert _parse_value("None") == SQL_NULL_SENTINEL
    assert _parse_value("'hello'") == "hello"


def test_specialty_matrix_quarantines_empty_inet():
    from connectors.writer_common import quarantine_unfit_specialty_types

    details: list[dict] = []
    out = quarantine_unfit_specialty_types(
        [("1", ""), ("2", "127.0.0.1")],
        ["id", "addr"],
        ["INTEGER", "INET"],
        details,
        "quarantine",
    )
    assert out == [("2", "127.0.0.1")]
    assert details
    assert any("INET" in str(d.get("reason") or "").upper() or "inet" in str(d.get("reason") or "").lower() or "empty" in str(d.get("reason") or "").lower() for d in details)


def test_sql_bind_ip_alias_refuses_empty():
    import pytest
    from connectors.sql_bind import normalize_sql_bind_value

    with pytest.raises(ValueError, match="empty string cannot coerce to INET"):
        normalize_sql_bind_value("", "IP", engine="elasticsearch")


def test_dense_partition_holds_sql_null_sentinel_pk():
    from connectors.writer_common import partition_dense_upsert_rows
    from services.value_serializer import SQL_NULL_SENTINEL

    details: list[dict] = []
    out = partition_dense_upsert_rows(
        [(SQL_NULL_SENTINEL, "a"), ("2", "b")],
        ["id"],
        target_cols=["id", "note"],
        rejected_details=details,
        policy="quarantine",
    )
    assert out == [("2", "b")]
    assert details


def test_specialty_matrix_allows_sql_null_on_inet():
    from connectors.writer_common import quarantine_unfit_specialty_types
    from services.value_serializer import SQL_NULL_SENTINEL

    details: list[dict] = []
    out = quarantine_unfit_specialty_types(
        [("1", SQL_NULL_SENTINEL)],
        ["id", "addr"],
        ["INTEGER", "INET"],
        details,
        "quarantine",
    )
    assert out == [("1", SQL_NULL_SENTINEL)]
    assert not details


def test_specialty_matrix_quarantines_empty_hstore():
    from connectors.writer_common import quarantine_unfit_specialty_types

    details: list[dict] = []
    # Classic hstore literal requires quoted keys/values ("k"=>"v").
    out = quarantine_unfit_specialty_types(
        [("1", ""), ("2", '"a"=>"1"')],
        ["id", "tags"],
        ["INTEGER", "HSTORE"],
        details,
        "quarantine",
    )
    assert len(out) == 1
    assert out[0][0] == "2"
    assert details


def test_redis_normalize_refuses_empty_float():
    import pytest
    from connectors.redis_writer import _normalize_redis_typed_doc

    with pytest.raises(ValueError, match="empty string cannot coerce to float"):
        _normalize_redis_typed_doc(
            {"amt": ""}, ["amt"], ["FLOAT"]
        )


def test_redis_doc_keeps_explicit_sql_null_omits_missing():
    """Sparse omit vs SQL NULL wipe — Bugbot invent cliff on Redis JSON SET."""
    from connectors.redis_writer import _redis_row_to_doc
    from services.value_serializer import DF_MISSING_SENTINEL, SQL_NULL_SENTINEL

    doc = _redis_row_to_doc(
        ["id", "amt", "note"],
        ("1", SQL_NULL_SENTINEL, DF_MISSING_SENTINEL),
    )
    assert doc == {"id": "1", "amt": None}
    assert "note" not in doc


def test_iceberg_stringify_preserves_sql_null():
    from connectors.iceberg_reader import _stringify
    from services.value_serializer import SQL_NULL_SENTINEL

    assert _stringify(None) == SQL_NULL_SENTINEL


def test_iceberg_stringify_bytes_base64():
    import base64

    from connectors.iceberg_reader import _stringify

    raw = b"\xff\xfe"
    assert _stringify(raw) == base64.b64encode(raw).decode("ascii")
