"""A SQL Server create-new carrier is chosen from the source column's capacity.

``NVARCHAR`` is not "the safe default": promoting a genuinely single-byte source
reports a widen the source never had, and stamping ``VARCHAR`` for a Unicode
source refuses the first non-Latin scalar at the write. The declaration decides,
and an unmeasured declaration decides nothing.
"""

from __future__ import annotations

import re

import pytest

from services.dest_dialect_facts import _collation_compatible_with_dest
from services.decision_kernel.type_invent import create_new_mapping_target_type
from services.type_system import (
    ddl_carrier_type,
    is_lossy_coercion,
    is_precision_collapse_coercion,
    source_column_holds_unicode,
)


def _carrier_without_collation(stamp: str) -> str:
    return re.split(r"\s+(?:COLLATE|CHARACTER\s+SET|CHARSET)\s+", stamp, maxsplit=1, flags=re.I)[
        0
    ].strip()


@pytest.mark.parametrize(
    ("source_db", "source_type", "expect_national"),
    [
        ("mysql", "varchar(32) CHARACTER SET utf8mb4", True),
        ("mysql", "varchar(32) COLLATE utf8mb4_0900_ai_ci", True),
        ("mysql", "longtext CHARACTER SET utf8mb4", True),
        ("mysql", "varchar(32) CHARACTER SET latin1", False),
        ("mysql", "varchar(32) COLLATE latin1_swedish_ci", False),
        # Nobody measured this column's charset: the server default is an
        # assumption, and an assumption licenses no widen.
        ("mysql", "varchar(32)", False),
        ("postgresql", "varchar(64)", True),
        ("postgresql", "text", True),
        ("oracle", "varchar2(32)", False),
        ("oracle", "nvarchar2(32)", True),
        ("sqlserver", "varchar(32)", False),
        ("sqlserver", "varchar(32) COLLATE Latin1_General_100_CI_AS_SC_UTF8", True),
        ("sqlserver", "nvarchar(32)", True),
        ("", "varchar(32)", False),
        ("mysql", "int", False),
        ("mysql", "varbinary(32)", False),
    ],
)
def test_source_column_unicode_capacity(
    source_db: str, source_type: str, expect_national: bool
) -> None:
    assert source_column_holds_unicode(source_db, source_type) is expect_national


@pytest.mark.parametrize(
    ("source_db", "source_type", "expected"),
    [
        ("mysql", "varchar(32) CHARACTER SET utf8mb4", "NVARCHAR(32)"),
        ("mysql", "varchar(32) COLLATE utf8mb4_0900_ai_ci", "NVARCHAR(32)"),
        ("mysql", "char(10) CHARACTER SET utf8mb4", "NCHAR(10)"),
        ("mysql", "varchar(32) CHARACTER SET latin1", "VARCHAR(32)"),
        ("mysql", "char(10) CHARACTER SET latin1", "CHAR(10)"),
        ("postgresql", "varchar(64)", "NVARCHAR(64)"),
        ("oracle", "varchar2(32)", "VARCHAR(32)"),
        ("mysql", "varchar(32)", "VARCHAR(32)"),
        ("mysql", "int", "BIGINT"),
    ],
)
def test_create_new_sqlserver_carrier(source_db: str, source_type: str, expected: str) -> None:
    got = create_new_mapping_target_type(source_type, "sqlserver", source_db=source_db)
    assert _carrier_without_collation(got).upper() == expected


def test_unbounded_unicode_source_lands_national_max() -> None:
    got = create_new_mapping_target_type(
        "longtext CHARACTER SET utf8mb4", "sqlserver", source_db="mysql"
    )
    assert got.upper().startswith("NVARCHAR")
    assert "MAX" in got.upper()


def test_code_page_source_is_not_promoted_on_mysql_destination() -> None:
    got = create_new_mapping_target_type(
        "varchar(32) CHARACTER SET latin1", "mysql", source_db="mysql"
    )
    assert "NVARCHAR" not in got.upper()


def test_measured_unicode_promotion_is_not_a_fidelity_collapse() -> None:
    for src in (
        "varchar(32) CHARACTER SET utf8mb4",
        "varchar(32) COLLATE utf8mb4_0900_ai_ci",
        "longtext CHARACTER SET utf8mb4",
    ):
        target = create_new_mapping_target_type(src, "sqlserver", source_db="mysql")
        assert not is_precision_collapse_coercion(src, target, dest_db="sqlserver"), src
        assert not is_lossy_coercion(src, target, dest_db="sqlserver"), src


def test_carrier_report_keeps_the_charset_evidence() -> None:
    # MySQL introspection reports the collation, and dropping it left the
    # create-new invent with no proof the column holds Unicode.
    assert "utf8mb4" in ddl_carrier_type("varchar(32) COLLATE utf8mb4_0900_ai_ci")
    assert (
        ddl_carrier_type("longtext CHARACTER SET utf8mb4").upper().startswith(("LONGTEXT", "TEXT"))
    )


@pytest.mark.parametrize(
    "collation",
    [
        "SQL_Latin1_General_CP1_CI_AS",
        "Latin1_General_100_CI_AI_SC",
        "Latin1_General_100_CI_AS_SC_UTF8",
        "Japanese_CI_AS",
        "Latin1_General_BIN2",
    ],
)
def test_sqlserver_accepts_its_own_collations(collation: str) -> None:
    assert _collation_compatible_with_dest("sqlserver", collation) is True


@pytest.mark.parametrize(
    "collation",
    [
        "utf8mb4_0900_ai_ci",
        "latin1_swedish_ci",
        "en_US.UTF-8",
        "NOT A COLLATION",
    ],
)
def test_sqlserver_rejects_foreign_collations(collation: str) -> None:
    assert _collation_compatible_with_dest("sqlserver", collation) is False
