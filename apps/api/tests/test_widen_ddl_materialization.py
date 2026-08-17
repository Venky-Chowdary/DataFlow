"""Schema-drift widen DDL must be physical per dialect, and refusals actionable.

The live Oracle/SQL Server/PostgreSQL matrix failed a route with
``ORA-22858: invalid alteration of datatype`` because the Map stamp carrier
``timestamp_ntz`` was emitted verbatim into ``ALTER TABLE ... MODIFY``. Every
dialect branch materializes its own physical spelling now, so these tests pin
the carrier vocabulary that reaches a real engine.
"""

from __future__ import annotations

import pytest

from connectors.schema_drift import (
    WidenNotSupported,
    _build_widen_ddl,
    raise_widen_refusal,
)


class TestOracleWidenMaterialization:
    def test_logical_timestamp_carrier_becomes_oracle_timestamp(self) -> None:
        ddl = _build_widen_ddl("oracle", "DFUSER", "ENT_PG_ORA", "day", "timestamp_ntz")
        assert "timestamp_ntz" not in ddl
        assert 'MODIFY ("day" TIMESTAMP' in ddl

    def test_timestamptz_carrier_becomes_oracle_zoned_timestamp(self) -> None:
        ddl = _build_widen_ddl("oracle", "DFUSER", "T", "ts_tz", "TIMESTAMPTZ")
        assert "TIME ZONE" in ddl.upper()

    def test_decimal_carrier_becomes_oracle_number_with_precision(self) -> None:
        ddl = _build_widen_ddl("oracle", "DFUSER", "T", "amount", "DECIMAL(12,2)")
        assert "NUMBER(12,2)" in ddl
        assert "DECIMAL" not in ddl.upper()

    def test_boolean_carrier_becomes_oracle_numeric_carrier(self) -> None:
        ddl = _build_widen_ddl("oracle", "DFUSER", "T", "flag", "BOOLEAN")
        assert "NUMBER(1)" in ddl

    def test_already_physical_oracle_type_is_unchanged(self) -> None:
        ddl = _build_widen_ddl("oracle", "DFUSER", "T", "name", "VARCHAR2(64 BYTE)")
        assert "VARCHAR2(64 BYTE)" in ddl


class TestOtherDialectsStayPhysical:
    @pytest.mark.parametrize(
        ("dialect", "expected"),
        [
            ("postgresql", "TIMESTAMP"),
            ("mysql", "DATETIME(6)"),
            ("sqlserver", "DATETIME2(7)"),
        ],
    )
    def test_logical_carrier_materializes_per_dialect(
        self, dialect: str, expected: str
    ) -> None:
        ddl = _build_widen_ddl(dialect, None, "t", "day", "timestamp_ntz")
        assert "timestamp_ntz" not in ddl
        assert expected in ddl

    def test_postgres_same_family_width_increase_has_no_cast(self) -> None:
        ddl = _build_widen_ddl(
            "postgresql", None, "t", "name", "VARCHAR(128)", "VARCHAR(64)"
        )
        assert "USING" not in ddl
        assert "VARCHAR(128)" in ddl

    def test_sqlserver_unicode_carrier_is_preserved(self) -> None:
        ddl = _build_widen_ddl("sqlserver", "dbo", "t", "name", "NVARCHAR(MAX)")
        assert "NVARCHAR(MAX)" in ddl


class TestWidenRefusalIsActionable:
    def test_oracle_cross_family_alter_becomes_actionable_refusal(self) -> None:
        exc = RuntimeError("ORA-22858: invalid alteration of datatype")
        with pytest.raises(WidenNotSupported) as caught:
            raise_widen_refusal("day", "DATE", "TIMESTAMP", exc)
        message = str(caught.value)
        assert "'day'" in message
        assert "DATE" in message
        assert "TIMESTAMP" in message
        assert "risk contract" in message

    def test_populated_column_refusal_is_translated(self) -> None:
        exc = RuntimeError("ORA-01439: column to be modified must be empty")
        with pytest.raises(WidenNotSupported):
            raise_widen_refusal("amount", "NUMBER(5,0)", "NUMBER(12,2)", exc)

    def test_unrelated_error_is_left_for_the_caller_to_reraise(self) -> None:
        # Not a conversion refusal: the caller must surface the original error
        # rather than mislabel a connectivity fault as an unsupported widen.
        raise_widen_refusal("x", "INT", "BIGINT", RuntimeError("connection reset"))
