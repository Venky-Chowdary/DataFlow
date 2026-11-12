"""Audit finding 12 (M6) — the static lossy-type predicate missed narrowing pairs.

``is_lossy_coercion`` / ``is_precision_collapse_coercion`` said "no loss" for
pairs that do lose values on an existing destination column:

* a character-counted ``varchar(10)`` into Oracle ``VARCHAR2(10)`` or
  ``VARCHAR2(10 BYTE)`` (byte semantics, the Oracle default): ten characters
  are up to forty bytes in AL32UTF8;
* PostgreSQL bare ``varchar`` (unbounded) into ``VARCHAR(5)``;
* ``bigint`` (19 digits) into Oracle ``NUMBER(10)``;
* ``double precision`` (binary64) into MySQL ``FLOAT`` (binary32);
* SQL Server ``float`` (binary64, ``FLOAT(53)``) into PostgreSQL ``REAL``.

The other five pairs the audit listed (unconstrained ``numeric`` into MySQL
``DECIMAL(38,15)``, SQL Server ``tinyint`` into MySQL ``TINYINT``,
``datetime2(7)`` into ``TIMESTAMP(6)``, Oracle ``DATE`` into PostgreSQL
``DATE``, MySQL ``timestamp`` into PostgreSQL ``TIMESTAMP``) are the product's
own create-new mapping today, so grading them lossy would refuse the second
run of every table Datawrap creates; they need the create-new mapper changed
first and are pinned below as the known, deferred remainder.
"""

from __future__ import annotations

import pytest

from services.type_coercion import (
    is_lossy_coercion,
    is_precision_collapse_coercion,
)
from services.type_system import create_new_mapping_target_type


def _lossy(source_db: str, source_type: str, dest_db: str, target_type: str) -> bool:
    return bool(
        is_lossy_coercion(
            source_type,
            target_type,
            dest_db=dest_db,
            source_db=source_db,
            dest_table_exists=True,
        )
    ) or bool(
        is_precision_collapse_coercion(
            source_type, target_type, dest_db=dest_db, dest_table_exists=True
        )
    )


@pytest.mark.parametrize(
    "source_db,source_type,dest_db,target_type",
    [
        ("postgresql", "varchar(10)", "oracle", "VARCHAR2(10)"),
        ("postgresql", "varchar(10)", "oracle", "VARCHAR2(10 BYTE)"),
        ("mysql", "varchar(10)", "oracle", "VARCHAR2(39 BYTE)"),
        ("postgresql", "varchar", "postgresql", "VARCHAR(5)"),
        ("postgresql", "character varying", "mysql", "VARCHAR(255)"),
        ("postgresql", "bigint", "oracle", "NUMBER(10)"),
        ("postgresql", "int4", "oracle", "NUMBER(9)"),
        ("postgresql", "smallint", "mysql", "DECIMAL(6,2)"),
        ("postgresql", "double precision", "mysql", "FLOAT"),
        ("sqlserver", "float", "postgresql", "REAL"),
        ("sqlserver", "float", "mysql", "FLOAT"),
    ],
)
def test_missed_lossy_pair_is_now_lossy(source_db, source_type, dest_db, target_type):
    assert _lossy(source_db, source_type, dest_db, target_type) is True


@pytest.mark.parametrize(
    "source_db,source_type,dest_db,target_type",
    [
        # Same character capacity, or enough bytes for any character.
        ("postgresql", "varchar(10)", "oracle", "VARCHAR2(10 CHAR)"),
        ("postgresql", "varchar(10)", "oracle", "VARCHAR2(40 BYTE)"),
        ("postgresql", "varchar(10)", "oracle", "VARCHAR2(40)"),
        ("oracle", "VARCHAR2(10)", "oracle", "VARCHAR2(10)"),
        ("oracle", "VARCHAR2(10 BYTE)", "oracle", "VARCHAR2(10 BYTE)"),
        # A width the source never had is not invented without its engine.
        ("", "varchar", "postgresql", "VARCHAR(5)"),
        ("mysql", "varchar(10)", "postgresql", "VARCHAR(10)"),
        ("postgresql", "varchar", "postgresql", "TEXT"),
        # Integer digits that fit.
        ("postgresql", "integer", "oracle", "NUMBER(10)"),
        ("postgresql", "bigint", "oracle", "NUMBER(19)"),
        ("postgresql", "bigint", "oracle", "NUMBER(38,0)"),
        ("postgresql", "bigint", "oracle", "NUMBER"),
        ("postgresql", "smallint", "mysql", "DECIMAL(7,2)"),
        ("postgresql", "bigint", "sqlite", "INTEGER"),
        # Same float width.
        ("postgresql", "double precision", "mysql", "DOUBLE"),
        ("mysql", "float", "mysql", "FLOAT"),
        ("mysql", "float", "postgresql", "REAL"),
        ("sqlserver", "float", "postgresql", "DOUBLE PRECISION"),
        ("sqlserver", "real", "mysql", "FLOAT"),
        ("postgresql", "double precision", "sqlite", "REAL"),
    ],
)
def test_value_preserving_pair_stays_clean(source_db, source_type, dest_db, target_type):
    assert _lossy(source_db, source_type, dest_db, target_type) is False


_SOURCES = {
    "postgresql": [
        "varchar(10)", "varchar", "character varying", "text", "smallint",
        "integer", "bigint", "real", "double precision", "float", "numeric(12,2)",
    ],
    "mysql": [
        "varchar(10)", "tinyint", "smallint", "mediumint", "int", "bigint",
        "int unsigned", "float", "double", "decimal(12,2)",
    ],
    "sqlserver": [
        "varchar(10)", "nvarchar(10)", "tinyint", "smallint", "int", "bigint",
        "real", "float", "decimal(12,2)",
    ],
    "oracle": ["VARCHAR2(10)", "VARCHAR2(10 CHAR)", "NUMBER(10)", "BINARY_DOUBLE", "FLOAT"],
}
_DESTS = ["postgresql", "mysql", "sqlserver", "oracle", "sqlite", "snowflake"]


#: Create-new pairs the predicate graded lossy before this change (the mapper's
#: business, unchanged here).
_KNOWN_LOSSY_CREATE_NEW: set[tuple[str, str, str]] = {
    ("mysql", "int unsigned", "sqlite"),
    ("oracle", "NUMBER(10)", "postgresql"),
    ("oracle", "NUMBER(10)", "mysql"),
    ("oracle", "NUMBER(10)", "sqlserver"),
    ("oracle", "NUMBER(10)", "oracle"),
    ("oracle", "NUMBER(10)", "snowflake"),
}


@pytest.mark.parametrize("source_db", sorted(_SOURCES))
@pytest.mark.parametrize("dest_db", _DESTS)
def test_the_products_own_create_new_target_is_never_graded_lossy(source_db, dest_db):
    """A rule change must not refuse the second run of a table Datawrap created."""
    flagged = set()
    for source_type in _SOURCES[source_db]:
        target = create_new_mapping_target_type(source_type, dest_db, source_db=source_db)
        if target and _lossy(source_db, source_type, dest_db, target):
            flagged.add((source_db, source_type, dest_db))
    assert flagged <= _KNOWN_LOSSY_CREATE_NEW


@pytest.mark.parametrize(
    "source_db,source_type,dest_db,target_type",
    [
        ("postgresql", "numeric", "mysql", "DECIMAL(38,15)"),
        ("sqlserver", "tinyint", "mysql", "TINYINT"),
        ("sqlserver", "datetime2(7)", "postgresql", "TIMESTAMP(6)"),
        ("oracle", "DATE", "postgresql", "DATE"),
        ("mysql", "timestamp", "postgresql", "TIMESTAMP"),
    ],
)
def test_deferred_pairs_are_the_create_new_mapping(source_db, source_type, dest_db, target_type):
    """The remaining audit pairs are what create-new emits; fixing them is a mapper change."""
    assert create_new_mapping_target_type(source_type, dest_db, source_db=source_db) == target_type
    assert _lossy(source_db, source_type, dest_db, target_type) is False
