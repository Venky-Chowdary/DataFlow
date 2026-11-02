"""Gate-8 must not fail a correct write over two spellings of one column type.

The writer fingerprints with the DDL it emitted (``DATETIME(6)``,
``varchar(255)``); the read-back fingerprints with what the live catalog
reports (``TIMESTAMP_NTZ(6)``, ``VARCHAR(255) COLLATE utf8mb4_0900_ai_ci``).
Those name the same physical column, so hashing the raw spelling made an
identical population hash differently and a correct PG→MySQL transfer failed
reconciliation.
"""

from __future__ import annotations

import pytest

from services.reconciliation import _canonical_fingerprint_ddl, canonical_checksum

EQUIVALENT_MYSQL_SPELLINGS = [
    ("DATETIME(6)", "TIMESTAMP_NTZ(6)"),
    ("varchar(255)", "VARCHAR(255) COLLATE utf8mb4_0900_ai_ci"),
    ("bigint", "BIGINT"),
]


@pytest.mark.parametrize("written,read_back", EQUIVALENT_MYSQL_SPELLINGS)
def test_equivalent_type_spellings_canonicalize_together(
    written: str, read_back: str
) -> None:
    assert _canonical_fingerprint_ddl("mysql", written) == _canonical_fingerprint_ddl(
        "mysql", read_back
    )


def test_distinct_types_stay_distinct() -> None:
    assert _canonical_fingerprint_ddl("mysql", "BIGINT") != _canonical_fingerprint_ddl(
        "mysql", "VARCHAR(255)"
    )


def test_empty_metadata_is_left_alone() -> None:
    assert _canonical_fingerprint_ddl("mysql", "") == ""


def test_write_and_readback_checksums_match_across_spellings() -> None:
    rows = [
        {"id": 1, "email": "a@example.com", "updated_at": "2024-01-01T00:00:00"},
        {"id": 2, "email": "b@example.com", "updated_at": "2024-01-02T00:00:00"},
    ]
    columns = ["id", "email", "updated_at"]
    written = canonical_checksum(
        rows,
        columns,
        dest_db_type="mysql",
        dest_types={
            "id": "bigint",
            "email": "varchar(255)",
            "updated_at": "DATETIME(6)",
        },
    )
    read_back = canonical_checksum(
        rows,
        columns,
        dest_db_type="mysql",
        dest_types={
            "id": "BIGINT",
            "email": "VARCHAR(255) COLLATE utf8mb4_0900_ai_ci",
            "updated_at": "TIMESTAMP_NTZ(6)",
        },
    )
    assert written == read_back
