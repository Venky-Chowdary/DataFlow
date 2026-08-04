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
