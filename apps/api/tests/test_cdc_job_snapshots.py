"""Job-scoped CDC incremental snapshot resolve + enqueue (no mocks)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def _cdc_job(**overrides):
    base = {
        "sync_mode": "cdc",
        "cdc_plugin": "pgoutput",
        "transfer_request": {
            "sync_mode": "cdc",
            "source": {
                "kind": "database",
                "format": "postgresql",
                "host": "localhost",
                "port": 5432,
                "database": "app",
                "table": "orders",
            },
            "destination": {
                "kind": "database",
                "format": "postgresql",
                "host": "localhost",
                "port": 5432,
                "database": "app",
                "table": "orders",
            },
            "stream_contracts": [{"name": "orders", "primary_key": "order_id"}],
        },
    }
    base.update(overrides)
    return base


def test_resolve_job_cdc_snapshot_context_fingerprint():
    from services.cdc_job_snapshots import resolve_job_cdc_snapshot_context

    ctx = resolve_job_cdc_snapshot_context(_cdc_job())
    assert ctx["table"] == "orders"
    assert ctx["primary_key"] == "order_id"
    assert ctx["source_key"].startswith("postgresql:")
    assert "app" in ctx["source_key"]
    assert "at-least-once" in ctx["honesty"]


def test_resolve_prefers_host_fingerprint_without_connector():
    from services.cdc_job_snapshots import resolve_job_cdc_snapshot_context

    ctx = resolve_job_cdc_snapshot_context(_cdc_job())
    assert ctx["source_key"] == "postgresql:localhost:5432/app"


def test_request_snapshot_for_job_enqueues(tmp_path, monkeypatch):
    import services.cdc_incremental_snapshot as snap_mod
    from services.cdc_job_snapshots import list_signals_for_job, request_snapshot_for_job

    monkeypatch.setattr(snap_mod, "_PATH", str(tmp_path / "signals.json"))
    monkeypatch.setattr(snap_mod, "_DATA_DIR", str(tmp_path))

    job = _cdc_job()
    row = request_snapshot_for_job(job, chunk_size=50)
    assert row["status"] == "pending"
    assert row["table"] == "orders"
    assert row["primary_key"] == "order_id"
    assert row["chunk_size"] == 50
    listed = list_signals_for_job(job)
    assert any(s["id"] == row["id"] for s in listed)


def test_non_cdc_job_rejected_without_cdc_signals():
    from services.cdc_job_snapshots import resolve_job_cdc_snapshot_context

    job = _cdc_job(sync_mode="full_refresh_append", cdc_plugin=None)
    job["transfer_request"]["sync_mode"] = "full_refresh_append"
    with pytest.raises(ValueError, match="CDC"):
        resolve_job_cdc_snapshot_context(job)


def test_resolve_connector_config_stamps_connector_id_when_inline():
    from src.transfer.adapters import resolve_connector_config
    from src.transfer.models import EndpointConfig

    cfg = resolve_connector_config(
        EndpointConfig(
            kind="database",
            format="postgresql",
            host="db.example",
            port=5432,
            database="app",
        )
    )
    assert not cfg.get("connector_id")
    with pytest.raises(ValueError, match="not found"):
        resolve_connector_config(
            EndpointConfig(
                kind="database",
                format="postgresql",
                connector_id="missing_connector_xyz",
                host="db.example",
                port=5432,
                database="app",
            )
        )
