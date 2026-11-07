"""Audit §2.3 — orphan auto-create DDL rollback."""

from __future__ import annotations

from services.auto_create_lifecycle import (
    clear_auto_create_job,
    mark_auto_create_committed,
    register_auto_create,
    rollback_uncommitted_auto_creates,
)


def test_rollback_drops_uncommitted_auto_create(monkeypatch):
    clear_auto_create_job("job-a")
    dropped: list[tuple] = []

    def fake_drop(db_type, cfg, table, schema=None):
        dropped.append((db_type, table, schema))
        return True

    monkeypatch.setattr("connectors.table_manager.drop_table", fake_drop)

    register_auto_create(
        db_type="postgresql",
        table="dst_wide_x",
        schema="public",
        config={"host": "h"},
        job_id="job-a",
    )
    out = rollback_uncommitted_auto_creates("job-a")
    assert out == ["public.dst_wide_x"]
    assert dropped == [("postgresql", "dst_wide_x", "public")]


def test_committed_auto_create_not_rolled_back(monkeypatch):
    clear_auto_create_job("job-b")
    calls: list = []

    monkeypatch.setattr(
        "connectors.table_manager.drop_table",
        lambda *a, **k: calls.append(1) or True,
    )

    register_auto_create(
        db_type="postgresql",
        table="keep_me",
        schema="public",
        config={},
        job_id="job-b",
    )
    mark_auto_create_committed("job-b")
    assert rollback_uncommitted_auto_creates("job-b") == []
    assert calls == []
