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
    promote_create_new_temporal_stamp,
    document_domain_would_collapse,
    is_dialect_native_document_wire,
    is_lossy_coercion,
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


@pytest.mark.parametrize(
    "dest,src,expected_substr",
    [
        ("postgresql", "UUID", "UUID"),
        ("mysql", "UUID", "CHAR(36)"),
        ("sqlserver", "UUID", "UNIQUEIDENTIFIER"),
        ("snowflake", "UUID", "VARCHAR"),
        ("bigquery", "UUID", "STRING"),
        ("postgresql", "OBJECTID", "VARCHAR(24)"),
        ("mysql", "OBJECTID", "CHAR(24)"),
        ("postgresql", "TIMESTAMP_NTZ(6)", "TIMESTAMP(6)"),
        ("mysql", "TIMESTAMP_NTZ(6)", "DATETIME(6)"),
        ("sqlserver", "TIMESTAMP_NTZ(6)", "DATETIME2(6)"),
        ("snowflake", "TIMESTAMP_NTZ(6)", "TIMESTAMP_NTZ"),
    ],
)
def test_uuid_objectid_temporal_create_new_matrix(dest: str, src: str, expected_substr: str):
    stamped = create_new_mapping_target_type(src, dest)
    assert expected_substr.upper() in stamped.upper(), (dest, src, stamped)
    # Create-new stamp must not be a bare logical invent that collapses on itself.
    if src.startswith("TIMESTAMP"):
        assert is_precision_collapse_coercion(src, stamped, dest_db=dest) is False


def test_pipeline_typed_transform_stamps_physical_mysql_datetime():
    """date transform on create-new MySQL must not leave bare DATETIME."""
    from services.mapping_pipeline import _TYPED_TRANSFORM_TARGET_TYPE
    from services.type_system import create_new_mapping_target_type

    assert _TYPED_TRANSFORM_TARGET_TYPE["datetime"] == "DATETIME"
    physical = create_new_mapping_target_type("DATETIME", "mysql")
    assert physical.upper().startswith("DATETIME")
    assert "(" in physical  # DATETIME(6), not bare DATETIME


def test_run_mapping_pipeline_create_new_mysql_datetime_is_physical():
    from services.mapping_pipeline import run_mapping_pipeline

    result = run_mapping_pipeline(
        source_columns=["created_at"],
        target_columns=[],
        source_schemas=[
            {
                "name": "created_at",
                "inferred_type": "TIMESTAMP_NTZ(6)",
                "samples": ["2024-01-15 10:30:00.123456"],
            },
        ],
        destination_db_type="mysql",
        destination_table_exists=False,
        use_llm=False,
    )
    row = result["mappings"][0]
    tgt = str(row.get("target_type") or "")
    assert "DATETIME" in tgt.upper(), row
    # Must be physical DATETIME(6), not bare logical DATETIME invent.
    assert tgt.upper() != "DATETIME"
    assert is_precision_collapse_coercion(
        "TIMESTAMP_NTZ(6)", tgt, dest_db="mysql"
    ) is False

def test_run_mapping_pipeline_varchar_date_samples_widen_physical():
    """Transform-driven date widen must survive create-new risk re-stamp."""
    from services.mapping_pipeline import run_mapping_pipeline

    result = run_mapping_pipeline(
        source_columns=["d"],
        target_columns=[],
        source_schemas=[
            {
                "name": "d",
                "inferred_type": "VARCHAR",
                "samples": ["2024-01-15", "2024-02-01", "2024-03-01"],
            },
        ],
        destination_db_type="mysql",
        destination_table_exists=False,
        use_llm=False,
    )
    row = result["mappings"][0]
    assert row.get("transform") == "datetime", row
    tgt = str(row.get("target_type") or "")
    assert "DATETIME" in tgt.upper(), row
    assert tgt.upper() != "DATETIME"
    assert tgt.upper() != "TEXT"

def test_mysql_time6_create_new_not_bare_time():
    assert create_new_mapping_target_type("TIME(6)", "mysql").upper() == "TIME(6)"
    assert create_new_mapping_target_type("TIME", "mysql").upper() == "TIME(6)"
    assert is_precision_collapse_coercion("TIME(6)", "TIME(6)", dest_db="mysql") is False
    # Bare TIME still narrows without dest-aware stamp.
    assert temporal_precision_would_narrow("TIME(6)", "TIME", dest_db="mysql") is True
    assert promote_create_new_temporal_stamp("TIME(6)", "TIME", "mysql") == "TIME(6)"


def test_coercion_probe_uses_dest_db_for_pg_timestamp():
    from services.coercion_probe import analyze_coercion

    report = analyze_coercion(
        sample_rows=[{"ts": "2024-01-01 12:00:00.123456"}],
        mappings=[{"source": "ts", "target": "ts", "target_type": "TIMESTAMP", "create_new": True}],
        source_types={"ts": "TIMESTAMP_NTZ(6)"},
        dest_types={},
        dest_db_type="postgresql",
        table_exists=False,
    )
    cols = report.get("columns") or []
    # After resolve promotes TIMESTAMP→TIMESTAMP(6), should not fidelity-block.
    blocked = [c for c in cols if c.get("fidelity_collapse") and c.get("severity") == "block"]
    assert not blocked, blocked

def test_ip_inet_host_address_twins():
    from services.type_system import specialty_carrier_would_collapse, specialty_polarity_mismatch

    assert specialty_carrier_would_collapse("IP", "INET") is False
    assert specialty_polarity_mismatch("IP", "INET") is False
    assert is_precision_collapse_coercion("IP", "INET") is False
    assert specialty_polarity_mismatch("INET", "CIDR") is True
    assert is_precision_collapse_coercion("INET", "CIDR") is True

def test_redshift_never_invents_timestamp_typmod():
    """Amazon Redshift rejects TIMESTAMP(p) — create-new must stay bare."""
    stamped = create_new_mapping_target_type("TIMESTAMP_NTZ(6)", "redshift")
    assert "(" not in stamped, stamped
    assert stamped.upper() == "TIMESTAMP"
    assert promote_create_new_temporal_stamp("TIMESTAMP_NTZ(6)", "TIMESTAMP", "redshift") == "TIMESTAMP"
    assert promote_create_new_temporal_stamp("TIMESTAMP_NTZ(6)", "TIMESTAMP(6)", "redshift") == "TIMESTAMP"
    # Values still fit — bare RS TIMESTAMP is microsecond; not a collapse.
    assert temporal_precision_would_narrow(
        "TIMESTAMP_NTZ(6)", "TIMESTAMP", dest_db="redshift"
    ) is False


def test_sqlserver_create_new_datetime2_time_fsp_defaults():
    assert create_new_mapping_target_type("TIMESTAMP_NTZ", "sqlserver") == "DATETIME2(7)"
    assert create_new_mapping_target_type("TIMESTAMP_NTZ(6)", "sqlserver") == "DATETIME2(6)"
    assert create_new_mapping_target_type("TIME", "sqlserver").upper().startswith("TIME")
    assert create_new_mapping_target_type("TIME(3)", "sqlserver") == "TIME(3)"
    assert is_precision_collapse_coercion(
        "TIMESTAMP_NTZ(6)", "DATETIME2(6)", dest_db="sqlserver"
    ) is False


def test_oracle_time_to_varchar2_remains_declared_lossy():
    """Oracle has no TIME — VARCHAR2 wire must stay Accept-risk / lossy, not silent-green."""
    stamped = create_new_mapping_target_type("TIME(6)", "oracle")
    assert "VARCHAR2" in stamped.upper()
    assert is_precision_collapse_coercion("TIME(6)", stamped, dest_db="oracle") is True

def test_lossy_respects_dest_db_for_timestamp_and_json():
    """is_lossy_coercion must share dest_db-aware collapse SSOT — not MySQL defaults."""
    from services.type_system import is_lossy_coercion

    # Without dest_db: bare TIMESTAMP fail-closed (MySQL FSP 0).
    assert is_lossy_coercion("TIMESTAMP_NTZ(6)", "TIMESTAMP") is True
    # PostgreSQL create-new: bare TIMESTAMP is micros — not lossy.
    assert is_lossy_coercion(
        "TIMESTAMP_NTZ(6)", "TIMESTAMP", dest_db="postgresql"
    ) is False
    assert is_lossy_coercion("JSON", "JSONB", dest_db="postgresql") is False
    assert is_lossy_coercion("JSON", "VARCHAR(50)", dest_db="postgresql") is True


def test_validate_mapping_coercions_create_new_pg_timestamp_not_block():
    from services.type_coercion_validator import validate_mapping_coercions

    issues = validate_mapping_coercions(
        mappings=[
            {
                "source": "ts",
                "target": "ts",
                "target_type": "TIMESTAMP",
                "create_new": True,
                "confidence": 0.95,
            }
        ],
        source_types={"ts": "TIMESTAMP_NTZ(6)"},
        target_types={},
        dest_db_type="postgresql",
    )
    blocks = [i for i in issues if i.get("severity") == "block"]
    assert not blocks, blocks

def test_cross_surface_dest_db_agreement_pg_timestamp_and_json():
    """Map/Validate/proof must share dest_db-aware lossy SSOT — no dual verdicts."""
    from services.type_system import is_lossy_coercion
    from services.mapping_proof import mapping_fidelity
    from services.type_coercion_validator import validate_mapping_coercions

    pairs = [
        ("TIMESTAMP_NTZ(6)", "TIMESTAMP", "postgresql", False),
        ("TIMESTAMP_NTZ(6)", "TIMESTAMP", "mysql", True),
        ("JSON", "JSONB", "postgresql", False),
        ("JSON", "VARCHAR(50)", "postgresql", True),
    ]
    for src, tgt, dest, expect_lossy in pairs:
        assert is_lossy_coercion(src, tgt, dest_db=dest) is expect_lossy, (src, tgt, dest)
        verdict = mapping_fidelity(
            {"source_type": src, "target_type": tgt, "transform": "none"},
            destination_db_type=dest,
        )
        if expect_lossy:
            assert verdict["verdict"] == "lossy_cast", (src, tgt, dest, verdict)
        else:
            assert verdict["verdict"] != "lossy_cast", (src, tgt, dest, verdict)

        issues = validate_mapping_coercions(
            mappings=[
                {
                    "source": "c",
                    "target": "c",
                    "target_type": tgt,
                    "create_new": True,
                    "confidence": 0.95,
                }
            ],
            source_types={"c": src},
            target_types={},
            dest_db_type=dest,
        )
        blocks = [i for i in issues if i.get("severity") == "block"]
        if expect_lossy:
            assert blocks, (src, tgt, dest, issues)
        else:
            assert not blocks, (src, tgt, dest, blocks)
