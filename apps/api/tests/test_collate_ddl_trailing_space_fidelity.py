"""Create-new COLLATE/VARCHAR(n) emit + PAD SPACE uniqueness — enterprise SSOT."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.type_system import (  # noqa: E402
    ddl_type,
    trailing_spaces_insignificant_for_unique,
    unique_equality_key,
)


def test_create_new_preserves_varchar_width_and_collate_mysql():
    ddl = ddl_type("mysql", "VARCHAR(50) COLLATE utf8mb4_unicode_ci")
    assert ddl.upper().startswith("VARCHAR(50)")
    assert "utf8mb4_unicode_ci" in ddl


def test_create_new_preserves_nvarchar_collate_sqlserver():
    ddl = ddl_type("sqlserver", "NVARCHAR(100) COLLATE Latin1_General_CI_AI")
    assert "NVARCHAR(100)" in ddl.upper()
    assert "Latin1_General_CI_AI" in ddl


def test_create_new_preserves_varchar_collate_postgresql():
    ddl = ddl_type("postgresql", "VARCHAR(40) COLLATE en_US")
    assert ddl.upper().startswith("VARCHAR(40)")
    assert "en_US" in ddl


def test_create_new_refuses_cross_engine_collate_invent():
    # MySQL collation must not be pasted onto PostgreSQL create-new.
    ddl = ddl_type("postgresql", "VARCHAR(40) COLLATE utf8mb4_unicode_ci")
    assert "utf8mb4" not in ddl.lower()
    assert ddl.upper().startswith("VARCHAR(40)")


def test_create_new_citext_and_char_width():
    assert ddl_type("postgresql", "CITEXT") == "CITEXT"
    assert ddl_type("mysql", "CHAR(3)").upper().startswith("CHAR(3)")
    assert "BYTE" in ddl_type("oracle", "VARCHAR2(20 BYTE)").upper()


def test_pad_space_polarity_by_engine():
    assert trailing_spaces_insignificant_for_unique("CHAR(5)") is True
    assert trailing_spaces_insignificant_for_unique(
        "VARCHAR(10)", dest_kind="postgresql"
    ) is False
    assert trailing_spaces_insignificant_for_unique(
        "VARCHAR(10)", dest_kind="mysql"
    ) is True
    assert trailing_spaces_insignificant_for_unique(
        "NVARCHAR(10) COLLATE Latin1_General_CI_AS", dest_kind="sqlserver"
    ) is True
    assert trailing_spaces_insignificant_for_unique(
        "VARCHAR2(10)", dest_kind="oracle"
    ) is False


def test_unique_equality_char_pad_vs_varchar_nopad():
    assert unique_equality_key("abc ", "CHAR(5)") == unique_equality_key(
        "abc", "CHAR(5)"
    )
    assert unique_equality_key(
        "abc ", "VARCHAR(10)", dest_kind="postgresql"
    ) != unique_equality_key("abc", "VARCHAR(10)", dest_kind="postgresql")
    assert unique_equality_key(
        "abc ", "VARCHAR(10)", dest_kind="mysql"
    ) == unique_equality_key("abc", "VARCHAR(10)", dest_kind="mysql")


def test_integrity_pg_varchar_trailing_space_not_duplicate():
    from services.data_integrity import _check_duplicate_keys

    result = _check_duplicate_keys(
        [{"source": "code", "target": "code"}],
        [{"code": "abc"}, {"code": "abc "}],
        "strict",
        dest_kind="postgresql",
        primary_key="code",
        sync_mode="append",
        destination_unique_keys=[{"name": "uq_code", "columns": ["code"]}],
        target_types={"code": "VARCHAR(10)"},
    )
    assert result["passed"] is True


def test_integrity_mysql_varchar_trailing_space_is_duplicate():
    from services.data_integrity import _check_duplicate_keys

    result = _check_duplicate_keys(
        [{"source": "code", "target": "code"}],
        [{"code": "abc"}, {"code": "abc "}],
        "strict",
        dest_kind="mysql",
        primary_key="code",
        sync_mode="append",
        destination_unique_keys=[{"name": "uq_code", "columns": ["code"]}],
        target_types={"code": "VARCHAR(10) COLLATE utf8mb4_unicode_ci"},
    )
    assert result["passed"] is False
