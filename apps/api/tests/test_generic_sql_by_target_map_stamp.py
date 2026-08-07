"""generic_sql CREATE/ALTER must stamp by Map target — never mappings[i] zip."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_by_target_stamps_survive_omit_reorder():
    """Reproduce invent cliff: omit + reorder would index-zip wrong types."""
    from services.type_system import materialize_dest_ddl
    from services.mapping_constraints import write_mappings

    # Same algorithm as generic_sql create path (by-target).
    target_cols = ["skills", "id"]
    logical_types = ["JSON", "BIGINT"]
    mappings = [
        {"source": "skip_me", "target": "", "omit": True, "target_type": "TEXT"},
        {"source": "title", "target": "title", "target_type": "TEXT"},
        {"source": "skills", "target": "skills", "target_type": "JSON"},
        {"source": "id", "target": "id", "target_type": "BIGINT"},
    ]
    column_types = {
        "skip_me": "VARCHAR",
        "title": "VARCHAR",
        "skills": "ARRAY",
        "id": "INTEGER",
    }
    by_tgt: dict[str, dict] = {}
    for mapping in write_mappings(list(mappings)):
        tgt = str(mapping.get("target") or "").strip()
        if tgt and tgt not in by_tgt:
            by_tgt[tgt] = mapping
            by_tgt.setdefault(tgt.lower(), mapping)

    dest_db = "sqlserver"
    out: dict[str, str] = {}
    for i, col in enumerate(target_cols):
        mapping = by_tgt.get(col) or by_tgt.get(str(col).lower()) or {}
        explicit = str(
            mapping.get("target_type") or mapping.get("dest_type") or ""
        ).strip()
        source_type = (
            column_types.get(str(mapping.get("source") or ""))
            or (logical_types[i] if i < len(logical_types) else "string")
        )
        derived = materialize_dest_ddl(dest_db, explicit or source_type)
        out[col] = derived

    # Index zip would have stamped skills from omit TEXT — must be JSON wire.
    assert "JSON" in out["skills"].upper() or out["skills"].upper() in {
        "NVARCHAR(MAX)",
        "NVARCHAR",
    }
    assert "BIGINT" in out["id"].upper() or out["id"].upper().startswith("INT")


def test_mapping_proof_create_new_uses_physical_not_source_identity():
    from services.mapping_proof import mapping_fidelity

    verdict = mapping_fidelity(
        {
            "source": "id",
            "target": "id",
            "create_new": True,
            "transform": "none",
        },
        declared_source_type="UUID",
        declared_target_type="",
        destination_db_type="bigquery",
    )
    reason = str(verdict.get("reason") or "")
    # Must stamp BQ physical STRING(36) — never green UUID→UUID identity invent.
    assert "STRING" in reason.upper(), verdict
    assert "UUID → UUID" not in reason


def test_snowflake_create_types_preserve_blank_carriers():
    from connectors.snowflake_writer import resolve_snowflake_create_types

    out = resolve_snowflake_create_types(["NUMBER(10,2)", "", "BOOLEAN"], [])
    assert out[0].upper().startswith("NUMBER") or "DECIMAL" in out[0].upper()
    assert out[1] == ""
    assert "BOOLEAN" in out[2].upper() or out[2].upper() == "BOOL"


def test_lakehouse_bare_array_document_wire_not_string_element_invent():
    from services.type_system import (
        create_new_mapping_target_type,
        ddl_type,
        is_lossy_coercion,
    )

    assert ddl_type("databricks", "ARRAY") == "STRING"
    assert create_new_mapping_target_type("ARRAY", "databricks") == "STRING"
    assert is_lossy_coercion("ARRAY", "STRING", dest_db="databricks") is False
    # Typed arrays stay native.
    assert "ARRAY<" in ddl_type("databricks", "ARRAY<INTEGER>")
    assert ddl_type("iceberg", "ARRAY") == "string"
    assert "list<" in ddl_type("iceberg", "ARRAY<INTEGER>")
