"""Unicode source text must not land on a SQL Server code-page carrier.

Live matrix defect: PostgreSQL ``VARCHAR`` → SQL Server ``VARCHAR`` wrote
``customer-1-éü中`` back as ``customer-1-éü?`` — the Latin1 code page has no
CJK point. Read-back verification caught it, but create-new must never invent
the lossy carrier in the first place.
"""

import pytest

from services.decision_kernel.type_invent import create_new_mapping_target_type
from services.type_system import (
    source_text_is_unicode,
    unicode_safe_target_carrier,
)


@pytest.mark.parametrize(
    "source_db",
    ["postgresql", "postgres", "sqlite", "snowflake", "mongodb", "csv", "parquet"],
)
def test_unicode_only_sources_are_recognised(source_db: str) -> None:
    assert source_text_is_unicode(source_db) is True


@pytest.mark.parametrize("source_db", ["sqlserver", "oracle", "mysql", "mariadb", ""])
def test_code_page_capable_sources_are_not_promoted(source_db: str) -> None:
    assert source_text_is_unicode(source_db) is False


@pytest.mark.parametrize(
    ("carrier", "expected"),
    [
        ("VARCHAR(64)", "NVARCHAR(64)"),
        ("CHAR(10)", "NCHAR(10)"),
        ("VARCHAR(MAX)", "NVARCHAR(MAX)"),
        ("TEXT", "NVARCHAR(MAX)"),
        ("VARCHAR(8000)", "NVARCHAR(MAX)"),
    ],
)
def test_sqlserver_text_carrier_promoted_for_unicode_source(
    carrier: str, expected: str
) -> None:
    assert (
        unicode_safe_target_carrier(
            carrier, dest_db="sqlserver", source_db="postgresql"
        )
        == expected
    )


def test_national_carrier_is_left_alone() -> None:
    assert (
        unicode_safe_target_carrier(
            "NVARCHAR(50)", dest_db="sqlserver", source_db="postgresql"
        )
        == "NVARCHAR(50)"
    )


def test_utf8_collation_varchar_already_holds_unicode() -> None:
    carrier = "VARCHAR(50) COLLATE Latin1_General_100_CI_AS_SC_UTF8"
    assert (
        unicode_safe_target_carrier(carrier, dest_db="sqlserver", source_db="postgresql")
        == carrier
    )


def test_code_page_source_keeps_its_own_polarity() -> None:
    """SQL Server VARCHAR → SQL Server VARCHAR is not a Unicode source."""
    assert (
        unicode_safe_target_carrier(
            "VARCHAR(64)", dest_db="sqlserver", source_db="sqlserver"
        )
        == "VARCHAR(64)"
    )


@pytest.mark.parametrize("dest_db", ["postgresql", "oracle", "mysql", "snowflake"])
def test_non_sqlserver_destinations_unchanged(dest_db: str) -> None:
    assert (
        unicode_safe_target_carrier("VARCHAR(64)", dest_db=dest_db, source_db="postgresql")
        == "VARCHAR(64)"
    )


def test_non_text_carrier_unchanged() -> None:
    assert (
        unicode_safe_target_carrier(
            "DECIMAL(12,2)", dest_db="sqlserver", source_db="postgresql"
        )
        == "DECIMAL(12,2)"
    )


def test_create_new_stamp_uses_national_carrier_for_unicode_source() -> None:
    stamped = create_new_mapping_target_type(
        "VARCHAR(64)", "sqlserver", source_db="postgresql"
    )
    assert stamped.upper().startswith("NVARCHAR")


def test_create_new_stamp_without_source_db_is_unchanged() -> None:
    """Default (unknown source) keeps today's polarity — no silent invent."""
    assert create_new_mapping_target_type("VARCHAR(64)", "sqlserver") == "VARCHAR(64)"
