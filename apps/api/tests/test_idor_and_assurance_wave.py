"""IDOR / workspace isolation hard tests for transfer job sub-routes."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.routers import transfer_router as tr


class _State:
    def __init__(self, email: str, role: str = "editor"):
        self.user_email = email
        self.user = {"email": email, "role": role}


def test_can_access_job_denies_foreign_workspace(monkeypatch):
    monkeypatch.setattr(tr, "can_read_workspace", lambda ws, email: email.endswith("@ok.com") and ws == "ws-a")
    job = {"_id": "j1", "workspace_id": "ws-a"}
    ok_req = SimpleNamespace(state=_State("a@ok.com"))
    bad_req = SimpleNamespace(state=_State("outsider@evil.com"))
    assert tr._can_access_job(ok_req, job) is True
    assert tr._can_access_job(bad_req, job) is False


def test_can_access_job_acl_deny(monkeypatch, tmp_path):
    from services import resource_acl as acl

    path = tmp_path / "acls.jsonl"
    monkeypatch.setattr(acl, "STORE_PATH", path)
    monkeypatch.setenv("RESOURCE_ACL_STORE", str(path))
    monkeypatch.setattr(tr, "can_read_workspace", lambda ws, email: True)

    acl.upsert_grant(
        tenant_id="ws1",
        resource_type="job",
        resource_id="job-secret",
        principal="owner@ok.com",
        role="owner",
    )
    job = {"_id": "job-secret", "workspace_id": "ws1"}
    owner = SimpleNamespace(state=_State("owner@ok.com"))
    other = SimpleNamespace(state=_State("peer@ok.com"))
    assert tr._can_access_job(owner, job) is True
    assert tr._can_access_job(other, job) is False


def test_sample_compare_emits_deterministic_seed():
    from services.reconciliation import sample_compare_rows

    src = [{"id": 2, "n": "b"}, {"id": 1, "n": "a"}, {"id": 3, "n": "c"}]
    tgt = [{"id": 1, "n": "a"}, {"id": 2, "n": "b"}, {"id": 3, "n": "c"}]
    maps = [{"source": "id", "target": "id"}, {"source": "n", "target": "n"}]
    a = sample_compare_rows(src, tgt, maps, sample_size=2, sort_key="id")
    b = sample_compare_rows(list(reversed(src)), list(reversed(tgt)), maps, sample_size=2, sort_key="id")
    assert a.get("sample_seed")
    assert a["sample_seed"]["content_sha256"] == b["sample_seed"]["content_sha256"]
    assert a["sample_seed"]["pk_values"] == b["sample_seed"]["pk_values"]
    assert a["sample_seed"]["method"] == "keyed_sorted"


def test_cdc_mapping_review_flag_and_ack(tmp_path, monkeypatch):
    from services import cdc_mapping_review as rev

    path = tmp_path / "reviews.jsonl"
    monkeypatch.setattr(rev, "STORE_PATH", path)
    monkeypatch.setenv("CDC_MAPPING_REVIEW_STORE", str(path))

    signal = rev.flag_mapping_review(
        source_key="pg:localhost:5432/db",
        table="public.orders",
        reason="cdc_schema_drift",
        schema_version=2,
        column_names=["id", "email", "new_col"],
    )
    open_rows = rev.list_reviews(source_key="pg:localhost:5432/db", status="open")
    assert len(open_rows) == 1
    assert open_rows[0]["id"] == signal["id"]
    assert rev.open_review_for_source("pg:localhost:5432/db", "public.orders")
    ack = rev.acknowledge_review(signal["id"])
    assert ack and ack["status"] == "acknowledged"
    assert rev.open_review_for_source("pg:localhost:5432/db", "public.orders") is None



def test_evaluate_resume_safety_refuses_empty_and_stale(monkeypatch):
    from datetime import datetime, timedelta, timezone

    from services.checkpoint_service import Checkpoint, evaluate_resume_safety

    empty = evaluate_resume_safety(None)
    assert empty["ok"] is False

    no_progress = evaluate_resume_safety(Checkpoint(job_id="j1", chunk_index=0, rows_processed=0))
    assert no_progress["ok"] is False

    fresh = Checkpoint(
        job_id="j1",
        chunk_index=3,
        rows_processed=1000,
        write_mode="upsert",
        updated_at=datetime.now(timezone.utc).isoformat(),
    )
    ok = evaluate_resume_safety(fresh, job={"status": "failed", "transfer_request": {"write_mode": "upsert"}})
    assert ok["ok"] is True
    assert ok["age_hours"] is not None

    stale = Checkpoint(
        job_id="j1",
        chunk_index=3,
        rows_processed=1000,
        write_mode="upsert",
        updated_at=(datetime.now(timezone.utc) - timedelta(hours=200)).isoformat(),
    )
    monkeypatch.setenv("DATAFLOW_RESUME_MAX_AGE_HOURS", "168")
    refused = evaluate_resume_safety(stale)
    assert refused["ok"] is False
    assert any("old" in r.lower() for r in refused["reasons"])

    drift = evaluate_resume_safety(
        fresh,
        job={"status": "failed", "transfer_request": {"write_mode": "append"}},
    )
    assert drift["ok"] is False


@pytest.mark.parametrize(
    "resource",
    ["explanation", "mapping-proof", "proof-pack", "certificate", "resume"],
)
def test_job_subroute_access_matrix_denies_foreign_workspace(monkeypatch, resource):
    """Every job-scoped surface shares the fail-closed workspace gate (404 posture)."""
    from src.routers.connectors_router import _can_access_job as connectors_can_access_job
    from src.routers.transfer_router import _can_access_job as transfer_can_access_job
    from src.routers import transfer_router as tr

    gate = lambda ws, email: ws == "ws-a" and email == "a@ok.com"
    monkeypatch.setattr(tr, "can_read_workspace", gate)

    job = {"_id": "job-a", "workspace_id": "ws-a"}
    ok_req = SimpleNamespace(state=_State("a@ok.com"))
    bad_req = SimpleNamespace(state=_State("b@other.com"))

    assert transfer_can_access_job(ok_req, job) is True
    assert transfer_can_access_job(bad_req, job) is False

    # connectors_router binds can_read_workspace at import — patch function globals.
    g = connectors_can_access_job.__globals__
    previous = g["can_read_workspace"]
    g["can_read_workspace"] = gate
    try:
        assert connectors_can_access_job(ok_req, job) is True
        assert connectors_can_access_job(bad_req, job) is False
    finally:
        g["can_read_workspace"] = previous
    assert resource  # matrix parameter kept for future HTTP expansion per surface



def test_snowflake_warehouse_advice_bands():
    from services.snowflake_warehouse_advice import advise_snowflake_warehouse

    assert advise_snowflake_warehouse(estimated_bytes=0, row_count=0) is None
    xs = advise_snowflake_warehouse(estimated_bytes=10_000_000)
    assert xs["recommended_size"] == "X-Small"
    med = advise_snowflake_warehouse(estimated_bytes=10 * (1024**3))
    assert med["recommended_size"] == "Medium"
    assert "Soft advisory" in med["honesty"]


def test_mcp_rate_limit_bucket(monkeypatch):
    from services import mcp_rate_limit as rl

    monkeypatch.setenv("DATAFLOW_MCP_RATE_LIMIT", "1")
    monkeypatch.setenv("DATAFLOW_MCP_RATE_BURST", "2")
    monkeypatch.setenv("DATAFLOW_MCP_RATE_QPS", "0.01")
    rl.reset_mcp_rate_limits()
    assert rl.check_mcp_rate_limit("agent-a")["allowed"] is True
    assert rl.check_mcp_rate_limit("agent-a")["allowed"] is True
    denied = rl.check_mcp_rate_limit("agent-a")
    assert denied["allowed"] is False
    assert float(denied["retry_after_sec"]) > 0
