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


def test_schemaless_placeholder_catalog_does_not_clobber_profiled_peek():
    """endpoint_source_column_types used to overlay Redis ``string`` on INTEGER."""
    from src.transfer.engine import (
        _authoritative_source_schema,
        _schemaless_live_is_placeholder,
    )
    from src.transfer.models import EndpointConfig

    assert _schemaless_live_is_placeholder(
        "redis", {"id": "string", "amount": "string"}
    )
    assert not _schemaless_live_is_placeholder(
        "sqlite", {"id": "TEXT", "amount": "TEXT"}
    )
    assert not _schemaless_live_is_placeholder(
        "mongodb", {"_id": "OBJECTID"}
    )

    peek = {"id": "INTEGER", "amount": "DECIMAL", "code": "TEXT"}
    source = EndpointConfig(kind="database", format="redis", table="lab")

    def _placeholder(_endpoint):
        return {c: "string" for c in peek}

    import services.source_schema_authority as ssa

    original = ssa.endpoint_source_column_types
    ssa.endpoint_source_column_types = _placeholder  # type: ignore[method-assign]
    try:
        merged = _authoritative_source_schema(source, peek, list(peek))
    finally:
        ssa.endpoint_source_column_types = original  # type: ignore[method-assign]
    assert merged["id"] == "INTEGER"
    assert merged["amount"] == "DECIMAL"


def test_redis_reader_stamps_sampled_native_types(monkeypatch):
    """Introspect reads native_types so the string placeholder cannot pose as DDL."""
    from unittest.mock import MagicMock

    from connectors.redis_reader import RedisScanState, read_keys_batch
    from services.type_system import normalize_logical_type

    client = MagicMock()
    client.scan.return_value = (0, [b"lab:1", b"lab:2"])
    client.type.return_value = b"string"
    client.get.side_effect = [
        b'{"id":1,"amount":1000.00,"code":"USD"}',
        b'{"id":2,"amount":2000.50,"code":"EUR"}',
    ]
    monkeypatch.setattr("connectors.redis_reader._redis_client", lambda *_a, **_k: client)
    monkeypatch.setattr(
        "connectors.redis_reader.scan_all_keys", lambda *_a, **_k: ["lab:1", "lab:2"]
    )
    batch, _state = read_keys_batch(
        cfg={}, pattern="lab:*", limit=10, scan_state=RedisScanState()
    )
    native = (batch.meta or {}).get("native_types") or {}
    assert "id" in batch.headers
    assert normalize_logical_type(native.get("id")) == "integer"
    assert normalize_logical_type(native.get("amount")) == "decimal"
    assert "(" not in str(native.get("amount") or "")
