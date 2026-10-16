"""File destinations and non-narrowing stamps are not a fidelity collapse."""

from __future__ import annotations

from services.type_coercion import is_lossy_coercion
from services.type_system import ddl_type


def test_national_string_to_file_export_is_not_lossy():
    for dest in ("s3", "sftp", "gcs", "csv", "parquet"):
        target = ddl_type(dest, "NVARCHAR(200)")
        assert is_lossy_coercion("NVARCHAR(200)", target, dest_db=dest) is False, (
            dest,
            target,
        )


def test_real_decimal_narrowing_still_blocks():
    assert (
        is_lossy_coercion("DECIMAL(12,2)", "DECIMAL(4,2)", dest_db="mysql") is True
    )


def test_nvarchar_on_mysql_keeps_national_carrier():
    target = ddl_type("mysql", "NVARCHAR(200)")
    assert is_lossy_coercion("NVARCHAR(200)", target, dest_db="mysql") is False


def test_mongo_decimal128_to_mysql_open_carrier_is_not_lossy():
    target = ddl_type("mysql", "decimal128")
    assert is_lossy_coercion("decimal128", target, dest_db="mysql") is False
