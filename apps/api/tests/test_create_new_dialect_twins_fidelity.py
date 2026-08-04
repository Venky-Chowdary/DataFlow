"""Create-new dialect twins must not false-block Validate (MySQL/Mongo → PG)."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.type_system import (  # noqa: E402
    case_fold_polarity_invent,
    is_precision_collapse_coercion,
    specialty_carrier_would_collapse,
    specialty_domain_would_invent,
    temporal_precision_would_narrow,
)


def test_json_jsonb_is_dialect_twin_not_collapse():
    assert specialty_domain_would_invent("JSON", "JSONB") is False
    assert specialty_carrier_would_collapse("JSONB", "JSON") is False
    assert is_precision_collapse_coercion("JSON", "JSONB") is False
    assert is_precision_collapse_coercion("JSONB", "JSON") is False
    # Real invent still blocks.
    assert specialty_domain_would_invent("JSON", "HSTORE") is True
    assert is_precision_collapse_coercion("JSON", "HSTORE") is True


def test_mysql_ci_text_to_bare_pg_text_is_normalize():
    src = "TEXT COLLATE UTF8MB4_0900_AI_CI"
    assert case_fold_polarity_invent(src, "TEXT") is False
    assert case_fold_polarity_invent(src, "VARCHAR") is False
    assert is_precision_collapse_coercion(src, "TEXT") is False
    # CITEXT polarity drop / invent still blocks.
    assert case_fold_polarity_invent("CITEXT", "TEXT") is True
    assert case_fold_polarity_invent("TEXT", "CITEXT") is True
    assert is_precision_collapse_coercion("TEXT", "CITEXT") is True


def test_timestamp_ntz6_to_bare_timestamp_fail_closed_mysql_default():
    # Bare TIMESTAMP without dest_db: MySQL FSP 0 — still collapse.
    assert temporal_precision_would_narrow("TIMESTAMP_NTZ(6)", "TIMESTAMP") is True
    assert is_precision_collapse_coercion("TIMESTAMP_NTZ(6)", "TIMESTAMP") is True
    assert temporal_precision_would_narrow(
        "TIMESTAMP_NTZ(6)", "TIMESTAMP", dest_db="postgresql"
    ) is False
    assert temporal_precision_would_narrow("TIMESTAMP_NTZ(6)", "TIMESTAMP WITHOUT TIME ZONE") is False
    assert temporal_precision_would_narrow("TIMESTAMP_NTZ(6)", "TIMESTAMP(6)") is False
    assert is_precision_collapse_coercion("TIMESTAMP_NTZ(6)", "TIMESTAMP(6)") is False
    # MySQL-class bare TIME still narrows.
    assert temporal_precision_would_narrow("TIME(6)", "TIME") is True


def test_create_new_promotes_bare_timestamp_stamp_for_pg():
    from services.type_system import create_new_mapping_target_type, promote_create_new_temporal_stamp

    assert create_new_mapping_target_type("TIMESTAMP_NTZ(6)", "postgresql") == "TIMESTAMP(6)"
    assert promote_create_new_temporal_stamp("TIMESTAMP_NTZ(6)", "TIMESTAMP", "postgresql") == "TIMESTAMP(6)"
    assert promote_create_new_temporal_stamp("TIMESTAMP_NTZ(6)", "TIMESTAMP", "mysql") == "TIMESTAMP"
