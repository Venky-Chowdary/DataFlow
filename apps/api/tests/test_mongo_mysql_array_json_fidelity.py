"""Mongo → MySQL ARRAY/JSON + COLLATE honesty (enterprise Validate unblock).

Document arrays mapped to MySQL native JSON are representation-preserving.
Uncollated VARCHAR → MySQL TEXT COLLATE utf8mb4_*_ci is platform default wire,
not case/accent invent that floods Risk Contracts.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.type_system import (  # noqa: E402
    array_to_native_document_wire_preserved,
    accent_polarity_invent,
    case_fold_polarity_invent,
    is_lossy_coercion,
    is_nested_document_collapse,
    is_precision_collapse_coercion,
)


def test_mysql_array_to_json_is_representation_not_lossy():
    assert array_to_native_document_wire_preserved(
        "ARRAY", "JSON", dest_db="mysql"
    ) is True
    assert array_to_native_document_wire_preserved(
        "ARRAY<STRING>", "JSON", dest_db="mysql"
    ) is True
    assert is_nested_document_collapse("ARRAY", "JSON", dest_db="mysql") is False
    assert is_lossy_coercion("ARRAY", "JSON", dest_db="mysql") is False
    assert is_lossy_coercion("ARRAY<STRING>", "JSON", dest_db="mysql") is False
    # Without dest_db, fail closed (prior honesty bar).
    assert is_lossy_coercion("ARRAY", "JSON") is True


def test_pg_typed_array_to_jsonb_still_collapse():
    # PostgreSQL can stamp INTEGER[] — JSONB sink drops typed-array polarity.
    assert array_to_native_document_wire_preserved(
        "ARRAY<INTEGER>", "JSONB", dest_db="postgresql"
    ) is False
    assert is_lossy_coercion("ARRAY<INTEGER>", "JSONB", dest_db="postgresql") is True
    # Bare ARRAY → JSONB is create-new SSOT on PG.
    assert array_to_native_document_wire_preserved(
        "ARRAY", "JSONB", dest_db="postgresql"
    ) is True
    assert is_lossy_coercion("ARRAY", "JSONB", dest_db="postgresql") is False


def test_uncollated_varchar_to_mysql_ci_text_not_invent():
    tgt = "TEXT COLLATE utf8mb4_0900_ai_ci"
    assert case_fold_polarity_invent("VARCHAR", tgt) is False
    assert accent_polarity_invent("VARCHAR", tgt) is False
    assert is_precision_collapse_coercion("VARCHAR", tgt, dest_db="mysql") is False
    assert is_lossy_coercion("VARCHAR", tgt, dest_db="mysql") is False
    assert is_lossy_coercion("TEXT", tgt, dest_db="mysql") is False
    # Explicit CS → CI still invents.
    assert case_fold_polarity_invent(
        "VARCHAR COLLATE utf8mb4_bin", tgt
    ) is True
    # CITEXT invent still blocks.
    assert case_fold_polarity_invent("TEXT", "CITEXT") is True


def test_array_to_text_still_lossy_on_mysql():
    # Plain TEXT is not the MySQL array create-new wire (JSON is).
    assert is_lossy_coercion("ARRAY", "TEXT", dest_db="mysql") is True
    assert is_nested_document_collapse("ARRAY", "TEXT", dest_db="mysql") is True


def test_mysql_struct_map_to_json_is_representation():
    # Mongo/ES objects → MySQL JSON is create-new SSOT (no native STRUCT).
    assert is_lossy_coercion("STRUCT", "JSON", dest_db="mysql") is False
    assert is_lossy_coercion("STRUCT<a:INTEGER>", "JSON", dest_db="mysql") is False
    assert is_lossy_coercion("MAP", "JSON", dest_db="mysql") is False
    # BigQuery can stamp fielded STRUCT — JSON sink still collapses.
    assert is_lossy_coercion(
        "STRUCT<a:INTEGER>", "JSON", dest_db="bigquery"
    ) is True
    assert is_lossy_coercion(
        "MAP<STRING,INTEGER>", "JSON", dest_db="bigquery"
    ) is True
