"""D211 - every source document type grades DataFlow's own JSON column as equivalent.

Run 2 grades the source's logical type against the destination column as
introspection reads it back. D198 recognised DataFlow's own JSON carrier only
for a JSON source; the nested grader (ARRAY / STRUCT / MAP: Mongo arrays,
DynamoDB L, Kafka JSON arrays, Elasticsearch nested, Firestore arrays and maps)
still demanded a read-back that normalises to logical JSON, and MariaDB's JSON
column reads back as ``LONGTEXT COLLATE utf8mb4_bin``. The live route is
``test_r6_d211_mongo_document_run2_e2e.py``; this pins the shared grader for
every source type and every engine's own carrier.
"""

from __future__ import annotations

import pytest

from services.specialty_fit import objectid_text_domain_polarity
from services.type_coercion import is_lossy_coercion
from services.type_system import (
    _own_json_carrier_read_backs,
    ddl_type,
    is_nested_document_collapse,
    is_own_json_carrier,
    objectid_would_collapse,
    specialty_wire_preserves_value,
)

# Untyped document sources: what Mongo (ARRAY / OBJECT), DynamoDB (L / M),
# Kafka JSON (ARRAY / STRUCT), Elasticsearch (nested / object) and Firestore
# (array / map) report. Typed nests (ARRAY<STRING>) get native nested DDL where
# the dialect has it, so their run 2 target is that DDL, not JSON.
_DOCUMENT_SOURCES = (
    "ARRAY", "OBJECT", "STRUCT", "MAP", "L", "M", "NESTED", "LIST",
    "array", "map", "object", "nested", "DOCUMENT", "RECORD",
)
_ENGINES = ("mariadb", "mysql", "sqlserver", "postgresql", "oracle", "sqlite")


def _own_carriers(db: str) -> list[str]:
    return sorted(set(_own_json_carrier_read_backs(db)))


@pytest.mark.parametrize("db", _ENGINES)
@pytest.mark.parametrize("src", _DOCUMENT_SOURCES)
def test_document_source_onto_own_json_column_is_not_lossy(db, src):
    carriers = _own_carriers(db)
    assert carriers, db
    for target in carriers:
        assert not is_nested_document_collapse(src, target, dest_db=db), (src, target, db)
        assert not is_lossy_coercion(src, target, dest_db=db), (src, target, db)
        # The planner's own create-new type for this source is graded the same.
        assert not is_lossy_coercion(src, ddl_type(db, src), dest_db=db), (src, db)


@pytest.mark.parametrize("db", ["mariadb", "mysql"])
def test_mariadb_json_alias_read_back_is_own_carrier(db):
    # What the MySQL-family catalog reader reports for MariaDB's JSON column.
    assert is_own_json_carrier("LONGTEXT COLLATE utf8mb4_bin", dest_db=db)
    assert is_own_json_carrier("longtext collate UTF8MB4_BIN", dest_db=db)
    assert is_own_json_carrier("JSON", dest_db=db)


@pytest.mark.parametrize(
    "target",
    ["LONGTEXT", "LONGTEXT COLLATE utf8mb4_general_ci", "VARCHAR(255)", "TEXT"],
)
def test_user_text_column_is_still_a_nested_collapse_on_mysql_family(target):
    # Not widened: a user's plain/other-collation text column never was
    # DataFlow's JSON column, and bounded text is never a document wire.
    for db in ("mariadb", "mysql"):
        assert not is_own_json_carrier(target, dest_db=db)
        assert is_nested_document_collapse("ARRAY", target, dest_db=db), (target, db)


def test_bounded_text_stays_lossy_everywhere():
    for db in _ENGINES:
        assert is_lossy_coercion("ARRAY", "VARCHAR(100)", dest_db=db), db


@pytest.mark.parametrize(
    "target",
    [
        "CHAR(24) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
        "CHAR(24) CHARSET utf8mb4",
        "VARCHAR(24) CHARACTER SET ascii COLLATE ascii_bin",
        # SQL Server reads its key-carrying ObjectId column back as NCHAR(24).
        "NCHAR(24) COLLATE Latin1_General_BIN",
        "NCHAR(24)",
        # Oracle's VARCHAR2(24 CHAR) as introspection reads it back.
        "VARCHAR(24 CHAR)",
    ],
)
def test_objectid_char24_with_charset_is_its_own_wire(target):
    # DataFlow's own MySQL/MariaDB ObjectId column; charset/collation are
    # equality, not width. This blocked every Mongo -> MySQL/MariaDB run 1.
    assert specialty_wire_preserves_value("OBJECTID", target)
    assert not objectid_would_collapse("OBJECTID", target)
    assert not objectid_text_domain_polarity("OBJECTID", target)


def test_narrow_objectid_carrier_with_charset_still_collapses():
    target = "CHAR(12) CHARACTER SET utf8mb4"
    assert not specialty_wire_preserves_value("OBJECTID", target)
    assert objectid_would_collapse("OBJECTID", target)
