"""ITEM 2 — skip_preflight must complete; Validate drift must still refuse.

Programmatic API/CLI/scheduler callers set ``skip_preflight=True``. The DDL
identity gate must inline-stamp a fingerprint rather than refuse with
\"requires Validate preflight\". When an approved hash IS present and mappings
drift, Execute must still fail closed.
"""

from __future__ import annotations

import os
import tempfile
import uuid
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

# Match apps/api/tests/conftest.py — durable checkpoint needs a job store.
os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from services.decision_kernel import approved_mapping_ddl_fingerprint
from src.transfer.engine import UniversalTransferEngine, _enforce_ddl_identity
from src.transfer.models import EndpointConfig, TransferRequest


def test_skip_preflight_inline_stamps_without_validate_proof():
    maps = [
        {"source": "id", "target": "id", "target_type": "BIGINT"},
        {"source": "v", "target": "v", "target_type": "BIGINT"},
    ]
    err = _enforce_ddl_identity(
        None,
        maps,
        dest_db="postgresql",
        skip_preflight=True,
    )
    assert err is None


def test_skip_preflight_inline_stamps_when_proof_bundle_hollow():
    """Incomplete Validate proof must not block programmatic skip_preflight."""
    maps = [{"source": "a", "target": "a", "target_type": "TEXT"}]
    for pf in (
        {},
        {"proof_bundle": {}},
        {"proof_bundle": {"ddl_identity": {}}},
        {"proof_bundle": {"ddl_identity": {"ddl_identity_hash": ""}}},
    ):
        err = _enforce_ddl_identity(
            pf,
            maps,
            dest_db="sqlite",
            skip_preflight=True,
        )
        assert err is None, (pf, err)


def test_ui_execute_without_validate_still_refuses():
    """skip_preflight=False keeps the Validate gate (UI path)."""
    err = _enforce_ddl_identity(
        None,
        [{"source": "a", "target": "a", "target_type": "TEXT"}],
        dest_db="postgresql",
        skip_preflight=False,
    )
    assert err is not None
    assert "validate" in err.lower() or "preflight" in err.lower()


def test_approved_hash_drift_refused_even_with_skip_preflight():
    maps = [
        {"source": "id", "target": "id", "target_type": "BIGINT"},
        {"source": "v", "target": "v", "target_type": "BIGINT"},
    ]
    fp = approved_mapping_ddl_fingerprint(maps, dest_db="sqlite")
    drifted = [
        {"source": "id", "target": "id", "target_type": "TEXT"},
        {"source": "v", "target": "v", "target_type": "TEXT"},
    ]
    err = _enforce_ddl_identity(
        None,
        drifted,
        dest_db="sqlite",
        approved_ddl_identity_hash=fp,
        skip_preflight=True,
    )
    assert err is not None
    assert "mismatch" in err.lower() or "diverg" in err.lower()

    err_ok = _enforce_ddl_identity(
        None,
        maps,
        dest_db="sqlite",
        approved_ddl_identity_hash=fp,
        skip_preflight=True,
    )
    assert err_ok is None


def test_ute_skip_preflight_completes_sqlite_to_sqlite(tmp_path: Path):
    """End-to-end: programmatic transfer with skip_preflight=True writes rows."""
    src = tmp_path / "item2_src.sqlite"
    dst = tmp_path / "item2_dst.sqlite"
    eng = create_engine(f"sqlite:///{src}")
    try:
        with eng.begin() as c:
            c.execute(text("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)"))
            c.execute(text("INSERT INTO t VALUES (1, 42), (2, 99)"))
    finally:
        eng.dispose()

    req = TransferRequest(
        source=EndpointConfig(
            kind="database",
            format="sqlite",
            database=str(src),
            table="t",
        ),
        destination=EndpointConfig(
            kind="database",
            format="sqlite",
            database=str(dst),
            table="t",
        ),
        sync_mode="full_refresh_overwrite",
        validation_mode="warn",
        skip_preflight=True,
        mappings=[
            {
                "source": "id",
                "target": "id",
                "target_type": "BIGINT",
                "approved": True,
                "confidence": 0.99,
            },
            {
                "source": "v",
                "target": "v",
                "target_type": "BIGINT",
                "approved": True,
                "confidence": 0.99,
            },
        ],
    )
    result = UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24])
    assert result.success, result.error
    assert "DDL identity requires Validate" not in (result.error or "")
    assert result.records_transferred == 2

    deng = create_engine(f"sqlite:///{dst}")
    try:
        with deng.connect() as c:
            rows = c.execute(text("SELECT id, v FROM t ORDER BY id")).fetchall()
        assert list(rows) == [(1, 42), (2, 99)]
    finally:
        deng.dispose()


def test_ute_drifted_approved_hash_refuses_write(tmp_path: Path):
    """Operator-supplied hash + changed mappings → refuse (safety property)."""
    src = tmp_path / "item2_d_src.sqlite"
    dst = tmp_path / "item2_d_dst.sqlite"
    eng = create_engine(f"sqlite:///{src}")
    try:
        with eng.begin() as c:
            c.execute(text("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)"))
            c.execute(text("INSERT INTO t VALUES (1, 1)"))
    finally:
        eng.dispose()

    approved_maps = [
        {
            "source": "id",
            "target": "id",
            "target_type": "BIGINT",
            "approved": True,
            "confidence": 0.99,
        },
        {
            "source": "v",
            "target": "v",
            "target_type": "BIGINT",
            "approved": True,
            "confidence": 0.99,
        },
    ]
    fp = approved_mapping_ddl_fingerprint(approved_maps, dest_db="sqlite")
    drifted = [
        {
            "source": "id",
            "target": "id",
            "target_type": "TEXT",
            "approved": True,
            "confidence": 0.99,
        },
        {
            "source": "v",
            "target": "v",
            "target_type": "TEXT",
            "approved": True,
            "confidence": 0.99,
        },
    ]
    req = TransferRequest(
        source=EndpointConfig(
            kind="database",
            format="sqlite",
            database=str(src),
            table="t",
        ),
        destination=EndpointConfig(
            kind="database",
            format="sqlite",
            database=str(dst),
            table="t",
        ),
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        mappings=drifted,
        approved_ddl_identity_hash=fp,
    )
    result = UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24])
    assert result.success is False
    assert result.records_transferred == 0
    err = (result.error or "").lower()
    assert "mismatch" in err or "diverg" in err or "ddl identity" in err
