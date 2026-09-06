"""A fast-path CREATE declares the carrier Map declared, not a TEXT default.

The multi-stream contract route re-introspects inside each stream, so it hands
the copy planner an empty schema. Reading only ``type`` from the mapping meant
every column fell through to ``TEXT``: a declared BIGINT key landed as SQLite
``TEXT`` and its values were stored as text, which loses integer ordering,
range predicates and key-type parity with the source.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from services.copy_fast_path import declared_copy_carrier
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


def _maps() -> list[dict]:
    return [
        {
            "source": "id",
            "target": "id",
            "source_type": "BIGINT",
            "target_type": "BIGINT",
            "approved": True,
            "confidence": 0.99,
        },
        {
            "source": "label",
            "target": "label",
            "source_type": "TEXT",
            "target_type": "VARCHAR(32)",
            "approved": True,
            "confidence": 0.99,
        },
    ]


def test_declared_carrier_prefers_map_target_over_text_default():
    item = {"source": "id", "target": "id", "source_type": "INTEGER", "target_type": "BIGINT"}
    assert declared_copy_carrier(item, {}, "id", "id") == "BIGINT"


def test_declared_carrier_falls_back_to_source_type_then_schema():
    assert declared_copy_carrier({"source_type": "INTEGER"}, {}, "id", "id") == "INTEGER"
    assert declared_copy_carrier({}, {"id": "NUMERIC"}, "id", "id") == "NUMERIC"
    assert declared_copy_carrier({}, {}, "id", "id") == ""


def test_declared_carrier_keeps_schema_derived_type_key_first():
    item = {"type": "DATE", "target_type": "BIGINT"}
    assert declared_copy_carrier(item, {}, "id", "id") == "DATE"


def test_engine_count_token_is_read_as_cardinality_not_a_digest():
    """``dest_count:<n>`` is a COUNT(*) proof; comparing it to a hex digest is a
    hash-versus-row-count comparison that failed every correct engine copy."""
    from src.transfer.reconcile_step import _engine_count_proof_only

    engine_copy = {
        "proof_scope": "dest_count_equals_source_snapshot",
        "checksum": "dest_count:4",
    }
    assert _engine_count_proof_only(engine_copy) == 4
    assert _engine_count_proof_only({}) is None
    assert (
        _engine_count_proof_only(
            {"proof_scope": "dest_count_equals_source_snapshot", "checksum": "deadbeef"}
        )
        is None
    )
    assert (
        _engine_count_proof_only(
            {"proof_scope": "full_table_digest", "checksum": "dest_count:4"}
        )
        is None
    )


def test_engine_copy_reports_count_proof_without_claiming_value_fidelity(tmp_path: Path):
    src_path = tmp_path / "count_src.db"
    dst_path = tmp_path / "count_dst.db"
    src = sqlite3.connect(str(src_path))
    try:
        src.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT NOT NULL)")
        src.executemany(
            "INSERT INTO items (id, label) VALUES (?, ?)", [(1, "a"), (2, "b"), (3, "c")]
        )
        src.commit()
    finally:
        src.close()

    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="sqlite", database=str(src_path), table="items"
        ),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(dst_path), table="items_out"
        ),
        mappings=_maps(),
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
        skip_preflight=True,
    )
    result = UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])
    assert result.success, result.error
    report = result.reconciliation or {}
    assert report.get("target_rows") == 3
    # Cardinality proven, value fidelity explicitly not compared.
    assert report.get("checksum_scope") != "full_checksum"
    assert not report.get("source_checksum")


def test_multi_stream_sqlite_copy_keeps_declared_integer_key(tmp_path: Path):
    src_path = tmp_path / "carrier_src.db"
    dst_path = tmp_path / "carrier_dst.db"
    src = sqlite3.connect(str(src_path))
    try:
        for table in ("customers", "orders"):
            src.execute(
                f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, label TEXT NOT NULL)"
            )
            src.executemany(
                f"INSERT INTO {table} (id, label) VALUES (?, ?)", [(1, "a"), (2, "b")]
            )
        src.commit()
    finally:
        src.close()

    contracts = [
        {
            "name": name,
            "selected": True,
            "sync_mode": "full_refresh_overwrite",
            "primary_key": "id",
            "mappings": _maps(),
        }
        for name in ("customers", "orders")
    ]
    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="sqlite", database=str(src_path), table="customers"
        ),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(dst_path), table="customers"
        ),
        mappings=_maps(),
        sync_mode="full_refresh_overwrite",
        validation_mode="warn",
        skip_preflight=True,
        stream_contracts=contracts,
    )
    result = UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])
    assert result.success, result.error

    dest = sqlite3.connect(str(dst_path))
    try:
        for table in ("customers", "orders"):
            ddl = dest.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
            ).fetchone()[0]
            assert "BIGINT" in ddl.upper(), ddl
            rows = dest.execute(
                f"SELECT id, typeof(id) FROM {table} ORDER BY id"  # noqa: S608
            ).fetchall()
            assert rows == [(1, "integer"), (2, "integer")]
    finally:
        dest.close()
