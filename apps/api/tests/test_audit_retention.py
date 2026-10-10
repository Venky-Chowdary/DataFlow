"""Time retention, legal hold, checkpoints, and the purge API."""

from __future__ import annotations

import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


@pytest.fixture
def file_audit(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_AUDIT_RETENTION_DAYS", "30")
    monkeypatch.setenv("DATAFLOW_AUDIT_LEGAL_HOLD", "0")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "test-audit-retention-" + "x" * 40)
    from services import audit_log, evidence_chain

    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    monkeypatch.setattr(audit_log, "MAX_EVENTS", 5000)
    monkeypatch.setattr(audit_log, "_LAST_RETENTION_PURGE", time.monotonic(), raising=False)
    monkeypatch.setattr(
        evidence_chain,
        "truncation_store_path",
        lambda: tmp_path / "truncations.jsonl",
    )
    return audit_log, evidence_chain, tmp_path


def _append(audit, name):
    return audit.append_audit_event(
        action=name,
        actor="retention-test",
        resource="/retention-test",
        workspace_id="workspace-retention",
    )


def _seed_old_and_recent(audit, monkeypatch):
    times = iter(
        [
            "2025-01-01T00:00:00+00:00",
            "2025-01-10T00:00:00+00:00",
            "2025-02-01T00:00:00+00:00",
        ]
    )
    with monkeypatch.context() as patcher:
        patcher.setattr(audit, "_now", lambda: next(times))
        return [_append(audit, f"seed.{index}") for index in range(3)]


def test_file_time_purge_freezes_cutoff_and_dry_run_does_not_mutate(
    file_audit, monkeypatch
):
    audit, _chain, _tmp_path = file_audit
    rows = _seed_old_and_recent(audit, monkeypatch)
    before = audit.STORE_PATH.read_text(encoding="utf-8")
    now = datetime(2025, 2, 1, tzinfo=timezone.utc)

    dry = audit.purge_expired_audit_events(now=now, dry_run=True)
    assert dry == {
        "removed": 0,
        "oldest_kept_time": rows[0]["time"],
        "legal_hold": False,
        "retention_days": 30,
        "dry_run": True,
    }
    assert audit.STORE_PATH.read_text(encoding="utf-8") == before

    result = audit.purge_expired_audit_events(now=now)
    assert result == {
        "removed": 1,
        "oldest_kept_time": rows[1]["time"],
        "legal_hold": False,
        "retention_days": 30,
        "dry_run": False,
    }
    kept = [json.loads(line) for line in audit.STORE_PATH.read_text().splitlines()]
    assert [row["id"] for row in kept] == [rows[1]["id"], rows[2]["id"]]
    checkpoints = [
        json.loads(line)
        for line in (file_audit[2] / "truncations.jsonl").read_text().splitlines()
    ]
    assert checkpoints[-1]["removed_count"] == 1
    assert checkpoints[-1]["last_removed_event_hash"] == rows[0]["event_hash"]
    assert checkpoints[-1]["first_kept_event_hash"] == rows[1]["event_hash"]


def test_legal_hold_disables_time_purge_and_max_event_trim(file_audit, monkeypatch):
    audit, _chain, _tmp_path = file_audit
    monkeypatch.setenv("DATAFLOW_AUDIT_LEGAL_HOLD", "1")
    monkeypatch.setattr(audit, "MAX_EVENTS", 1)
    rows = _seed_old_and_recent(audit, monkeypatch)
    before = audit.STORE_PATH.read_text(encoding="utf-8")

    result = audit.purge_expired_audit_events(
        now=datetime(2025, 2, 1, tzinfo=timezone.utc)
    )
    assert result["legal_hold"] is True
    assert result["removed"] == 0
    assert audit.STORE_PATH.read_text(encoding="utf-8") == before
    assert len(rows) == 3


def test_invalid_retention_config_raises_typed_error_and_names_env(
    file_audit, monkeypatch, caplog
):
    audit, _chain, _tmp_path = file_audit
    monkeypatch.setenv("DATAFLOW_AUDIT_RETENTION_DAYS", "29")

    with pytest.raises(audit.AuditConfigError, match="DATAFLOW_AUDIT_RETENTION_DAYS"):
        audit.purge_expired_audit_events()
    assert "DATAFLOW_AUDIT_RETENTION_DAYS" in caplog.text


def test_append_does_not_raise_when_opportunistic_purge_fails(
    file_audit, monkeypatch, caplog
):
    audit, _chain, _tmp_path = file_audit

    def fail_purge(**_kwargs):
        raise OSError("retention store unavailable")

    monkeypatch.setattr(audit, "_LAST_RETENTION_PURGE", float("-inf"))
    monkeypatch.setattr(audit, "purge_expired_audit_events", fail_purge)
    event = audit.append_audit_event(
        action="append.with.purge.failure",
        actor="retention-test",
        resource="/append",
    )
    assert event["event_hash"]
    assert "OSError" in caplog.text


def test_purge_api_writes_event_and_verify_reports_retention(file_audit, monkeypatch):
    audit, _chain, _tmp_path = file_audit
    _seed_old_and_recent(audit, monkeypatch)

    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2025, 2, 1, tzinfo=tz or timezone.utc)

    monkeypatch.setattr(audit, "datetime", FrozenDateTime)
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "0")
    from src.services import auth_service

    monkeypatch.setattr(auth_service, "_REQUIRE_AUTH", False)
    from src.main import app

    with TestClient(app) as client:
        response = client.post("/api/v1/audit/retention/purge?dry_run=false")
        assert response.status_code == 200
        result = response.json()
        assert result["removed"] == 1
        assert result["dry_run"] is False
        events = client.get("/api/v1/audit/verify")
        assert events.status_code == 200
        report = events.json()
        assert report["verified"] is True
        assert report["findings"] == []
        assert any(
            checkpoint["removed_count"] == 1
            for checkpoint in report["retention_checkpoints"]
        )
    audit_events = [
        json.loads(line) for line in audit.STORE_PATH.read_text().splitlines()
    ]
    assert any(row["action"] == "audit.retention.purge" for row in audit_events)


LIVE_MONGO_URI = os.getenv("DATAFLOW_LIVE_MONGO_URI", "").strip()


@pytest.mark.skipif(not LIVE_MONGO_URI, reason="DATAFLOW_LIVE_MONGO_URI is not configured")
def test_live_mongo_time_purge_and_checkpoint(file_audit, monkeypatch):
    from pymongo import MongoClient
    from services import audit_log, evidence_chain

    _file_audit, _chain, tmp_path = file_audit
    client = MongoClient(LIVE_MONGO_URI, serverSelectionTimeoutMS=3000)
    client.admin.command("ping")
    db = client[f"dataflow_audit_retention_{uuid.uuid4().hex}"]
    collection = db["audit_events"]
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: collection)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "mongo-fallback.jsonl")
    monkeypatch.setattr(
        evidence_chain,
        "truncation_store_path",
        lambda: tmp_path / "mongo-truncations.jsonl",
    )
    try:
        times = iter(
            ["2025-01-01T00:00:00+00:00", "2025-02-01T00:00:00+00:00"]
        )
        with monkeypatch.context() as patcher:
            patcher.setattr(audit_log, "_now", lambda: next(times))
            first = _append(audit_log, "live.old")
            second = _append(audit_log, "live.kept")
        result = audit_log.purge_expired_audit_events(
            now=datetime(2025, 2, 1, tzinfo=timezone.utc)
        )
        assert result["removed"] == 1
        assert result["oldest_kept_time"] == second["time"]
        kept = list(collection.find({}))
        assert [row["event_hash"] for row in kept] == [second["event_hash"]]
        checkpoint = json.loads(
            (tmp_path / "mongo-truncations.jsonl").read_text().splitlines()[-1]
        )
        assert checkpoint["last_removed_event_hash"] == first["event_hash"]
        assert checkpoint["first_kept_event_hash"] == second["event_hash"]
    finally:
        client.drop_database(db.name)
        client.close()
