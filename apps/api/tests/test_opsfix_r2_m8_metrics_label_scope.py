"""Ops review round 2, Medium 8: a workspace admin's scrape must not show another tenant's pipelines.

Round 1 kept the per-route HTTP series and the job-store gauges for platform
administrators, but the per-pipeline series (CDC lag and polls, labelled by
schedule, stream and job) were still rendered for every scraper, so tenant
A's workspace admin read tenant B's schedule ids, stream names and job ids.
A scrape that is not deployment-wide now keeps only the pipelines whose
proven workspace is one of the caller's.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from services import ops_metrics
from services.workspace_filter import WorkspaceFilter


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    monkeypatch.setattr(ops_metrics, "_labeled_gauges", {})
    monkeypatch.setattr(ops_metrics, "_labeled_counters", {})
    monkeypatch.setattr(ops_metrics, "_pipeline_workspace", {})
    monkeypatch.setattr(ops_metrics, "_pipeline_heartbeat", {})
    monkeypatch.setattr(ops_metrics, "_store_cache", {"at": 0.0, "value": None})
    monkeypatch.setattr(ops_metrics, "_http", {})
    for ws, sched, stream in (("ws-tenant-b", "sched-tenantB-payroll", "hr.salaries"), ("ws-tenant-a", "sched-a", "sales.orders")):
        ops_metrics.record_cdc_poll(lag_seconds=42.0, lag_bytes=1234, lag_basis="wal_bytes", schedule_id=sched,
                                    stream=stream, job_id=f"job-{ws}", workspace_id=ws)


def _scrape(monkeypatch, scope) -> str:
    from services import workspace_access
    from src import metrics_app

    monkeypatch.setattr(workspace_access, "tenant_scope", lambda _request, *_a: scope)
    response = asyncio.run(metrics_app.metrics_response(SimpleNamespace(state=SimpleNamespace())))
    return response.body.decode()


def _workspace_admin(workspace_id: str) -> SimpleNamespace:
    only = WorkspaceFilter(frozenset({workspace_id}), False)
    return SimpleNamespace(unrestricted=False, filter=only, strict_filter=lambda: only)


def test_a_workspace_admin_sees_only_its_own_pipelines(monkeypatch):
    text = _scrape(monkeypatch, _workspace_admin("ws-tenant-a"))
    assert "tenantB" not in text and "hr.salaries" not in text and "job-ws-tenant-b" not in text
    assert 'schedule_id="sched-a"' in text, "its own pipeline is still shown"


def test_a_scope_without_a_workspace_filter_sees_no_pipeline(monkeypatch):
    text = _scrape(monkeypatch, SimpleNamespace(unrestricted=False))
    assert "schedule_id=" not in text


def test_a_platform_admin_sees_every_pipeline(monkeypatch):
    text = _scrape(monkeypatch, SimpleNamespace(unrestricted=True))
    assert 'schedule_id="sched-tenantB-payroll"' in text and 'schedule_id="sched-a"' in text
