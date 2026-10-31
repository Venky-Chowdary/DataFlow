"""D212: a count-only verdict over a code-page text column is a failure.

Where no value compare can run (no census driver for a side), a mapped text
column whose source can hold any character but whose SQL Server carrier is
a single-byte code page fails verification instead of riding out green.
"""

from __future__ import annotations

from types import SimpleNamespace

from services.value_readback import count_only_text_risk_verdict, text_round_trip_risks


def _ep(fmt: str, table: str = "") -> SimpleNamespace:
    return SimpleNamespace(format=fmt, table=table)


def _maps(target_type: str, source_type: str = "VARCHAR2(100)") -> list[dict]:
    return [
        {"source": "ID", "target": "id", "source_type": "NUMBER(10)", "target_type": "BIGINT"},
        {"source": "CITY", "target": "city", "source_type": source_type,
         "target_type": target_type},
    ]


def test_code_page_carrier_without_value_compare_fails():
    verdict = count_only_text_risk_verdict(
        source_endpoint=_ep("oracle"), endpoint=_ep("sqlserver"),
        mappings=_maps("VARCHAR(100)"), reason="no value compare ran — x",
    )
    assert verdict is not None and verdict["passed"] is False
    assert verdict["culprit_keys"]["text_columns_not_round_trip_proven"] == ["city"]
    assert verdict["value_compare"]["ran"] is False


def test_national_or_utf8_carrier_keeps_the_count_verdict():
    for target in ("NVARCHAR(100)", "VARCHAR(100) COLLATE Latin1_General_100_CI_AS_SC_UTF8"):
        assert count_only_text_risk_verdict(
            source_endpoint=_ep("oracle"), endpoint=_ep("sqlserver"),
            mappings=_maps(target), reason=None,
        ) is None, target


def test_declared_single_byte_source_on_code_page_is_not_a_risk():
    assert text_round_trip_risks(
        source_db="mysql", dest_db="sqlserver", pairs=[("c", "c")],
        source_types={"c": "VARCHAR(10) CHARACTER SET latin1"},
        dest_types={"c": "VARCHAR(10) COLLATE Latin1_General_CI_AS"},
    ) == []
    assert text_round_trip_risks(
        source_db="sqlserver", dest_db="sqlserver", pairs=[("c", "c")],
        source_types={"c": "VARCHAR(10)"}, dest_types={"c": "VARCHAR(10)"},
    ) == []


def test_non_sqlserver_unicode_destination_is_not_a_risk():
    assert text_round_trip_risks(
        source_db="oracle", dest_db="postgresql", pairs=[("c", "c")],
        source_types={"c": "VARCHAR2(10)"}, dest_types={"c": "VARCHAR(10)"},
    ) == []
