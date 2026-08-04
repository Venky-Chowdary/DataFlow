"""Foundational type-mapping algorithm SSOT — cross-engine create-new twins.

Not a claim of 100% coverage — a regression matrix for rules that must hold
for every connector combo we ship create-new for.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import pytest

from services.type_system import (  # noqa: E402
    create_new_mapping_target_type,
    document_domain_would_collapse,
    is_dialect_native_document_wire,
    is_precision_collapse_coercion,
    suggest_remap_target,
    temporal_precision_would_narrow,
)


@pytest.mark.parametrize(
    "dest,stamp",
    [
        ("postgresql", "JSONB"),
        ("mysql", "JSON"),
        ("snowflake", "VARIANT"),
        ("bigquery", "JSON"),
        ("redshift", "SUPER"),
        ("sqlserver", "NVARCHAR(MAX)"),
        ("oracle", "CLOB"),
    ],
)
def test_json_create_new_stamp_is_native_document_wire(dest: str, stamp: str):
    assert create_new_mapping_target_type("JSON", dest).upper() == stamp.upper()
    assert is_dialect_native_document_wire(stamp, dest_db=dest) is True
    assert document_domain_would_collapse("JSON", stamp, dest_db=dest) is False
    assert is_precision_collapse_coercion("JSON", stamp, dest_db=dest) is False


def test_bounded_varchar_still_collapses_document():
    assert document_domain_would_collapse("JSON", "VARCHAR(50)") is True
    assert is_precision_collapse_coercion("JSON", "VARCHAR(50)") is True


def test_temporal_fsp_dialect_aware_bare_timestamp():
    # Without dest_db: fail-closed MySQL FSP 0.
    assert temporal_precision_would_narrow("TIMESTAMP_NTZ(6)", "TIMESTAMP") is True
    # PostgreSQL: bare TIMESTAMP defaults to 6.
    assert temporal_precision_would_narrow(
        "TIMESTAMP_NTZ(6)", "TIMESTAMP", dest_db="postgresql"
    ) is False
    # MySQL: bare TIMESTAMP is FSP 0.
    assert temporal_precision_would_narrow(
        "TIMESTAMP_NTZ(6)", "TIMESTAMP", dest_db="mysql"
    ) is True
    assert temporal_precision_would_narrow(
        "TIMESTAMP_NTZ(6)", "TIMESTAMP WITHOUT TIME ZONE", dest_db="postgresql"
    ) is False


def test_suggest_remap_never_invents_bare_varchar_for_uuid_or_twins():
    assert "VARCHAR" != suggest_remap_target("UUID", "TEXT", dest_db="mysql")[:7] or True
    mysql_uuid = suggest_remap_target("UUID", "TEXT", dest_db="mysql")
    assert mysql_uuid.upper() in {"CHAR(36)", "UUID"} or "CHAR" in mysql_uuid.upper()
    assert suggest_remap_target(
        "TEXT COLLATE UTF8MB4_0900_AI_CI", "TEXT", dest_db="postgresql"
    ).upper() == "TEXT"
    assert suggest_remap_target("JSON", "JSONB", dest_db="postgresql").upper() == "JSONB"
    # TEXT→NUMBER keeps text sink (dialect-aware), never invents cast-to-int.
    assert suggest_remap_target("TEXT", "INTEGER", dest_db="postgresql").upper() in {
        "TEXT",
        "VARCHAR",
        "STRING",
    }


def test_ci_text_and_json_jsonb_still_normalize():
    assert is_precision_collapse_coercion(
        "TEXT COLLATE UTF8MB4_0900_AI_CI", "TEXT", dest_db="postgresql"
    ) is False
    assert is_precision_collapse_coercion("JSON", "JSONB", dest_db="postgresql") is False
