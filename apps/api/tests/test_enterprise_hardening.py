"""Enterprise hardening tests: leases, honesty, workspace isolation, CDC handoff."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from services.connector_capability_registry import get_connector_capability
from services.team_store import can_read_workspace, require_workspace_isolation
from services.worker_leases import WorkerLeaseStore, active_fence, clear_active_fence, requires_distributed_backend


def test_capability_registry_demotes_fiction_connectors():
    # Still demoted: no production modules.
    for key in ("delta", "sap", "databricks", "kinesis", "hudi"):
        cap = get_connector_capability(key)
        assert cap.get("transfer_ready") is False, key


def test_capability_registry_keeps_real_drivers_ready():
    for key in ("postgresql", "mysql", "mongodb", "snowflake", "s3", "csv", "iceberg", "kafka"):
        cap = get_connector_capability(key)
        assert cap.get("transfer_ready") is True, key
    # SQL Server / Oracle are first-class; transfer_ready depends on DBAPI install.
    for key in ("sqlserver", "oracle"):
        cap = get_connector_capability(key)
        assert "transfer_ready" in cap


def test_requires_distributed_backend_respects_memory_and_flag(monkeypatch):
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.delenv("DATAFLOW_MULTI_REPLICA", raising=False)
    assert requires_distributed_backend() is False
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "mongodb")
    monkeypatch.setenv("DATAFLOW_MULTI_REPLICA", "1")
    assert requires_distributed_backend() is True


def test_lease_acquire_exclusive_same_process():
    a = WorkerLeaseStore("worker-a")
    b = WorkerLeaseStore("worker-b")
    clear_active_fence("job-exclusive")
    assert a.acquire("job-exclusive", ttl_seconds=60) is True
    fence = active_fence("job-exclusive")
    assert fence is not None and fence >= 1
    assert b.acquire("job-exclusive", ttl_seconds=60) is False
    a.release("job-exclusive")
    assert b.acquire("job-exclusive", ttl_seconds=60) is True
    b.release("job-exclusive")


def test_lease_steal_after_expiry_increments_fence():
    a = WorkerLeaseStore("worker-a")
    b = WorkerLeaseStore("worker-b")
    assert a.acquire("job-expire", ttl_seconds=1) is True
    fence1 = active_fence("job-expire")
    # Force expiry in memory store
    with a._lock:
        a._memory["job-expire"]["expires_at"] = datetime.now(timezone.utc) - timedelta(seconds=1)
    clear_active_fence("job-expire")
    assert b.acquire("job-expire", ttl_seconds=60) is True
    fence2 = active_fence("job-expire")
    assert fence2 is not None and fence1 is not None and fence2 > fence1
    b.release("job-expire")


def test_lease_fail_closed_when_distributed_required(monkeypatch):
    monkeypatch.setenv("DATAFLOW_MULTI_REPLICA", "1")
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "mongodb")
    store = WorkerLeaseStore("worker-x")
    with patch.object(store, "_mongo_collection", return_value=None):
        assert store.acquire("job-fail-closed", ttl_seconds=60) is False


def test_duplicate_key_treated_as_lost_race(monkeypatch):
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    store = WorkerLeaseStore("worker-y")

    class Dup(Exception):
        pass

    Dup.__name__ = "DuplicateKeyError"
    coll = MagicMock()
    coll.find_one.return_value = None
    coll.find_one_and_update.side_effect = Dup("E11000 duplicate key")
    with patch.object(store, "_mongo_collection", return_value=coll):
        # Not distributed → would fall through, but DuplicateKey returns False first
        monkeypatch.setenv("DATAFLOW_MULTI_REPLICA", "1")
        monkeypatch.setenv("DATAFLOW_JOB_STORE", "mongodb")
        assert store.acquire("job-dup", ttl_seconds=60) is False


def test_workspace_isolation_denies_empty_in_production(monkeypatch):
    monkeypatch.setenv("DATAFLOW_REQUIRE_WORKSPACE", "1")
    assert require_workspace_isolation() is True
    assert can_read_workspace("", "anyone@example.com") is False
    monkeypatch.setenv("DATAFLOW_REQUIRE_WORKSPACE", "0")
    assert can_read_workspace("", "anyone@example.com") is True


def test_mysql_snapshot_captures_binlog_position():
    from connectors.mysql_change_stream import MySqlChangeStreamCdc

    cdc = MySqlChangeStreamCdc({"host": "localhost", "database": "db"}, table="t", primary_key="id")
    with patch.object(cdc, "_current_binlog_position", return_value={"file": "bin.0001", "pos": 42, "table": "t"}):
        with patch("connectors.mysql_change_stream.read_table_batch") as read:
            class Batch:
                headers = ["id"]
                rows = [["1"]]

            read.side_effect = [Batch(), type("Empty", (), {"headers": ["id"], "rows": []})()]
            batches = list(cdc.snapshot())
    assert batches
    assert batches[-1].resume_token is not None
    assert batches[-1].resume_token.get("file") == "bin.0001"
    assert batches[-1].resume_token.get("pos") == 42


def test_sdk_singer_bridge_registered():
    from connectors.sdk import SingerTapBridge, get_sdk_connector, list_sdk_connectors

    assert "singer_tap" in list_sdk_connectors()
    assert get_sdk_connector("singer_tap") is SingerTapBridge


def test_usage_metering_records_locally(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    # Re-import path uses data_dir at call time via platform_config
    from services import usage_metering as um

    monkeypatch.setattr(um, "STORE_PATH", tmp_path / "usage_metering.json")
    monkeypatch.setattr(um, "_mongo", lambda: None)
    eid = um.record_transfer_usage(job_id="j1", rows_written=10, source_type="csv", dest_type="postgresql")
    assert eid
    summary = um.summarize_usage()
    assert summary["rows_written"] == 10


def test_strict_g8_fails_without_verifier_non_dest_only():
    """Strict mode stays fail-closed when a duplex destination has no verifier."""
    from src.transfer.models import EndpointConfig
    from src.transfer.reconcile_step import run_reconciliation

    endpoint = EndpointConfig(kind="database", format="redis", database="db", table="t")
    with patch("src.transfer.reconcile_step.verify_target", return_value=(-1, "")):
        with patch("src.transfer.reconcile_step.resolve_connector_config", return_value={}):
            report = run_reconciliation(
                endpoint=endpoint,
                records=[],
                columns=["id"],
                rows_written=5,
                writer_checksum="abc",
                dest_summary={},
                validation_mode="strict",
            )
    assert report["passed"] is False
    assert "read-back" in report["message"].lower() or "verifier" in report["message"].lower()


def test_strict_g8_writer_ack_for_dest_only():
    """dest_only sinks have no SQL read-back — strict accepts matched writer-ack."""
    from src.transfer.models import EndpointConfig
    from src.transfer.reconcile_step import run_reconciliation

    endpoint = EndpointConfig(kind="database", format="qdrant", database="db", table="t")
    with patch("src.transfer.reconcile_step.verify_target", return_value=(-1, "")):
        with patch("src.transfer.reconcile_step.resolve_connector_config", return_value={}):
            report = run_reconciliation(
                endpoint=endpoint,
                records=[],
                columns=["id"],
                rows_written=5,
                writer_checksum="abc",
                dest_summary={},
                validation_mode="strict",
            )
    assert report["passed"] is True
    assert "writer" in report["message"].lower() or "read-back" in report["message"].lower()
