"""Redis source Map stamp must match Execute materialize.

Cartesian redis→{postgresql,mysql,sqlite} failed with
``amount: NUMERIC → TEXT`` / ``id: BIGINT → TEXT`` (SQLite ``INTEGER → TEXT``):
peek stamped every Redis header ``string`` while Validate re-inferred
INTEGER/DECIMAL from the same digit samples (``cell_to_string`` wire). Execute
invented TEXT; the proof_bundle hashed the numeric dest. One fingerprint.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.decision_kernel import (
    DdlIdentityError,
    assert_ddl_identity,
    stamp_additive_mapping_types,
)
from services.object_store_introspect import profile_schemaless_source_schema
from services.preflight_service import run_file_preflight
from services.source_engine_scope import bind_source_engine

COLUMNS = ["id", "amount", "code"]
# Flattened Redis JSON numbers — the reader emits ``cell_to_string`` text.
SAMPLE_ROWS = [
    {"id": "1", "amount": "1000.00", "code": "USD"},
    {"id": "2", "amount": "2000.50", "code": "EUR"},
]
SAMPLE_MATRIX = [[row[c] for c in COLUMNS] for row in SAMPLE_ROWS]
MAPPINGS = [
    {
        "source": "id",
        "target": "id",
        "confidence": 0.99,
        "transform": "integer",
        "approved": True,
        "create_new": True,
    },
    {
        "source": "amount",
        "target": "amount",
        "confidence": 0.99,
        "transform": "decimal",
        "approved": True,
        "create_new": True,
    },
    {
        "source": "code",
        "target": "code",
        "confidence": 0.99,
        "transform": "none",
        "approved": True,
        "create_new": True,
    },
]


def _samples_by_source() -> dict[str, list]:
    out: dict[str, list] = {}
    for row in SAMPLE_ROWS:
        for k, v in row.items():
            out.setdefault(k, []).append(v)
    return out


def _execute_stamp(source_types: dict[str, str], dest_db: str) -> list[dict]:
    with bind_source_engine("redis"):
        stamped, unstamped = stamp_additive_mapping_types(
            [dict(m) for m in MAPPINGS],
            dest_db=dest_db,
            live_dest_types={},
            source_types=source_types,
            samples_by_source=_samples_by_source(),
            dest_table_exists=False,
        )
    assert unstamped == []
    return stamped


def _validate_proof(source_types: dict[str, str], dest_db: str) -> dict:
    with bind_source_engine("redis"):
        pf = run_file_preflight(
            columns=COLUMNS,
            column_types=dict(source_types),
            row_count=len(SAMPLE_ROWS),
            mappings=[dict(m) for m in MAPPINGS],
            destination_connected=True,
            sample_rows=SAMPLE_ROWS,
            source_kind="database",
            source_format="redis",
            destination_db_type=dest_db,
            destination_table_exists=False,
            destination_can_create=True,
            destination_can_write=True,
            sync_mode="full_refresh_overwrite",
        )
    return pf


@pytest.mark.parametrize("dest_db", ["postgresql", "mysql", "sqlite"])
def test_redis_placeholder_string_peek_diverges_from_validate(dest_db: str):
    """The cartesian bug: all-string peek vs Validate numeric invent."""
    placeholder = {c: "string" for c in COLUMNS}
    execute_maps = _execute_stamp(placeholder, dest_db)
    pf = _validate_proof(placeholder, dest_db)
    stamp = (pf.get("proof_bundle") or {}).get("ddl_identity") or {}
    approved = str(stamp.get("ddl_identity_hash") or "")
    approved_columns = [c for c in (stamp.get("columns") or []) if isinstance(c, dict)]
    assert approved
    with pytest.raises(DdlIdentityError) as err:
        assert_ddl_identity(
            approved,
            execute_maps,
            dest_db=dest_db,
            approved_columns=approved_columns,
        )
    msg = str(err.value)
    assert "TEXT" in msg.upper()
    assert any(tok in msg.upper() for tok in ("NUMERIC", "DECIMAL", "BIGINT", "INTEGER"))


@pytest.mark.parametrize("dest_db", ["postgresql", "mysql", "sqlite"])
def test_redis_profiled_peek_matches_validate_ddl_identity(dest_db: str):
    """Peek uses the same schemaless profiler Validate does — one fingerprint."""
    peek_schema = profile_schemaless_source_schema(
        COLUMNS, SAMPLE_MATRIX, source_format="redis"
    )
    execute_maps = _execute_stamp(peek_schema, dest_db)
    pf = _validate_proof(peek_schema, dest_db)
    stamp = (pf.get("proof_bundle") or {}).get("ddl_identity") or {}
    approved = str(stamp.get("ddl_identity_hash") or "")
    approved_columns = [c for c in (stamp.get("columns") or []) if isinstance(c, dict)]
    assert approved
    assert (
        assert_ddl_identity(
            approved,
            execute_maps,
            dest_db=dest_db,
            approved_columns=approved_columns,
        )
        == approved
    )
    materialized = {
        str(c.get("target")): str(c.get("materialized_ddl") or "").upper()
        for c in approved_columns
    }
    assert "TEXT" not in materialized.get("id", "")
    assert "VARCHAR" not in materialized.get("id", "")
    if dest_db == "sqlite":
        assert "INT" in materialized.get("id", "")
    else:
        assert "BIGINT" in materialized.get("id", "") or "INT" in materialized.get(
            "id", ""
        )
    amount = materialized.get("amount", "")
    if dest_db != "sqlite":
        assert "NUMERIC" in amount or "DECIMAL" in amount
