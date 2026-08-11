"""Unicode carrier survives Map → Validate → CREATE DDL for the whole matrix.

A live ``postgresql->mssql`` run read back ``customer-1-éü中`` as
``customer-1-éü?``: create-new invention stamped SQL Server ``VARCHAR(64)`` for a
PostgreSQL ``VARCHAR(64)`` source, and the writer then collapsed every string
carrier to ``sa.Text()`` so even a corrected ``NVARCHAR(64)`` stamp compiled back
to a code-page ``VARCHAR(max)``. Three layers had to agree; these tests pin all
three plus the refusals that must survive.
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import mssql, oracle, postgresql

from connectors.generic_sql import _sa_type_for_logical
from services.conversion_contract import classify_conversion
from services.decision_kernel.type_invent import create_new_mapping_target_type
from services.source_engine_scope import active_source_engine, bind_source_engine
from services.type_system import (
    is_fixed_char_carrier,
    national_charset_would_invent,
    string_carrier_length,
)

UNICODE_SOURCES = ["postgresql", "mongodb", "snowflake", "bigquery"]
CODEPAGE_SOURCES = ["mssql", "oracle", "mysql"]


class TestCreateNewStampsNationalCarrier:
    def test_pg_varchar_to_sqlserver_becomes_nvarchar_same_width(self):
        assert (
            create_new_mapping_target_type(
                "VARCHAR(64)", "sqlserver", source_db="postgresql"
            )
            == "NVARCHAR(64)"
        )

    def test_pg_text_to_sqlserver_becomes_nvarchar_max(self):
        assert (
            create_new_mapping_target_type("TEXT", "sqlserver", source_db="postgresql")
            == "NVARCHAR(MAX)"
        )

    def test_pg_char_to_sqlserver_keeps_fixed_width_national(self):
        assert (
            create_new_mapping_target_type(
                "CHAR(8)", "sqlserver", source_db="postgresql"
            )
            == "NCHAR(8)"
        )

    def test_unicode_only_destination_keeps_plain_varchar(self):
        # PostgreSQL has no national type — VARCHAR already holds every code point.
        assert (
            create_new_mapping_target_type(
                "VARCHAR(64)", "postgresql", source_db="mssql"
            )
            == "VARCHAR(64)"
        )


class TestConversionContractAcceptsRequiredPromotion:
    def test_unicode_source_promotion_is_not_lossy(self):
        for source in UNICODE_SOURCES:
            assert not national_charset_would_invent(
                "VARCHAR(64)", "NVARCHAR(64)", source_db=source
            ), source

    def test_codepage_source_promotion_still_invents(self):
        for source in CODEPAGE_SOURCES:
            assert national_charset_would_invent(
                "VARCHAR(64)", "NVARCHAR(64)", source_db=source
            ), source

    def test_unknown_source_keeps_conservative_answer(self):
        assert national_charset_would_invent("VARCHAR(64)", "NVARCHAR(64)")

    def test_classify_conversion_unblocks_under_bound_source_engine(self):
        with bind_source_engine("postgresql"):
            verdict = classify_conversion(
                "VARCHAR(64)", "NVARCHAR(64)", dest_db="sqlserver"
            )
        assert verdict["lossy"] is False
        assert verdict["conversion_class"] != "needs_user_approval"

    def test_classify_conversion_still_blocks_codepage_source(self):
        with bind_source_engine("mssql"):
            verdict = classify_conversion(
                "VARCHAR(64)", "NVARCHAR(64)", dest_db="sqlserver"
            )
        assert verdict["lossy"] is True

    def test_binding_is_scoped_and_restored(self):
        assert active_source_engine() == ""
        with bind_source_engine("postgresql"):
            assert active_source_engine() == "postgresql"
        assert active_source_engine() == ""


class TestNationalCarrierReachesDDL:
    def test_sqlserver_bounded_national_keeps_width(self):
        compiled = _sa_type_for_logical("NVARCHAR(64)", "mssql", "sqlserver").compile(
            mssql.dialect()
        )
        assert compiled == "NVARCHAR(64)"

    def test_sqlserver_lob_national_is_nvarchar_max_not_ntext(self):
        compiled = _sa_type_for_logical("NVARCHAR(MAX)", "mssql", "sqlserver").compile(
            mssql.dialect()
        )
        assert compiled.upper() == "NVARCHAR(MAX)"

    def test_sqlserver_fixed_national_keeps_nchar(self):
        compiled = _sa_type_for_logical("NCHAR(8)", "mssql", "sqlserver").compile(
            mssql.dialect()
        )
        assert compiled == "NCHAR(8)"

    def test_oracle_national_compiles_to_legal_bounded_type(self):
        compiled = _sa_type_for_logical("NVARCHAR(64)", "oracle", "oracle").compile(
            oracle.dialect()
        )
        assert "64" in compiled and "VARCHAR" in compiled.upper()

    def test_unicode_only_dialect_renders_plain_varchar(self):
        compiled = _sa_type_for_logical(
            "NVARCHAR(64)", "postgresql", "postgresql"
        ).compile(postgresql.dialect())
        assert compiled == "VARCHAR(64)"

    def test_national_carrier_never_compiles_to_codepage_lob(self):
        for carrier in ("NVARCHAR(64)", "NVARCHAR(MAX)", "NCHAR(8)", "NCLOB"):
            compiled = (
                _sa_type_for_logical(carrier, "mssql", "sqlserver")
                .compile(mssql.dialect())
                .upper()
            )
            assert compiled.startswith("N"), (carrier, compiled)

    def test_created_table_ddl_carries_national_type(self):
        from connectors.generic_sql import _build_table_for_write

        table = _build_table_for_write(
            sa.create_mock_engine("mssql+pymssql://", lambda *a, **k: None),
            "ent_pg_ms",
            None,
            ["name"],
            {"name": "NVARCHAR(64)"},
            db_type="sqlserver",
        )
        ddl = str(sa.schema.CreateTable(table).compile(dialect=mssql.dialect()))
        assert "NVARCHAR(64)" in ddl


class TestStringCarrierHelpers:
    def test_width_parsed_for_bounded_carriers(self):
        assert string_carrier_length("VARCHAR(64)") == 64
        assert string_carrier_length("NVARCHAR(64)") == 64
        assert string_carrier_length("VARCHAR2(64 CHAR)") == 64
        assert string_carrier_length("CHAR(8)") == 8

    def test_unlimited_and_non_string_carriers_have_no_width(self):
        assert string_carrier_length("TEXT") is None
        assert string_carrier_length("NVARCHAR(MAX)") is None
        assert string_carrier_length("CLOB") is None
        assert string_carrier_length("DECIMAL(12,2)") is None
        assert string_carrier_length("") is None

    def test_fixed_char_detection_excludes_varchar_family(self):
        assert is_fixed_char_carrier("CHAR(8)")
        assert is_fixed_char_carrier("NCHAR(8)")
        assert not is_fixed_char_carrier("VARCHAR(8)")
        assert not is_fixed_char_carrier("NVARCHAR(8)")
        assert not is_fixed_char_carrier("TEXT")
