"""D346: a CDC mapping review holds only the schedules that read its table.

QA (prod b1d7ca75): a review flagged on ``"public"."mx4_cdc"`` refused
run_schedule_now for an incremental_append schedule on ``mx4_ia`` and an
incremental_deduped schedule on ``mx4_iu`` of the same source connector. The
gate looked the review up by table and then again with ``table=""``, which
returned any open review of the source. The refusal pointed at "Open Map ...
re-enable", and no MCP tool listed or acknowledged a review.
"""

from __future__ import annotations

import asyncio

import pytest

import services.schedule_runner as runner
import services.schedule_store as store
from services import cdc_mapping_review as rev

_SRC = {
    "_id": "src-pg", "id": "src-pg", "name": "QA-Postgres", "type": "postgresql",
    "host": "h", "port": 5432, "database": "db", "schema": "public",
    "username": "u", "password": "p",
}
_DST = {**_SRC, "_id": "dst-pg", "id": "dst-pg", "name": "QA-Dest"}


class _Saved:
    id = "src-pg"
    workspace_id = ""
    name = "QA-Postgres"
    type = "postgresql"

    def to_dict(self):
        return dict(_SRC)


@pytest.fixture
def env(tmp_path, monkeypatch):
    path = tmp_path / "reviews.jsonl"
    monkeypatch.setattr(rev, "STORE_PATH", path)
    monkeypatch.setenv("CDC_MAPPING_REVIEW_STORE", str(path))
    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "schedules.json")
    import services.connector_store as cs

    monkeypatch.setattr(
        cs, "get_connector",
        lambda cid, workspace_id=None, *a, **k: _Saved() if cid == "src-pg" else None,
    )
    review = rev.flag_mapping_review(
        source_key="connector:src-pg",
        table='"public"."mx4_cdc"',
        reason="cdc_schema_drift",
        column_names=["id", "name", "tier"],
    )
    return review


def _schedule(table: str, mode: str, **extra):
    return store.create_schedule({
        "name": f"s-{table}-{mode}", "source_connector_id": "src-pg", "source_table": table,
        "dest_connector_id": "dst-pg", "dest_table": f"{table}_wh", "interval": "hourly",
        "sync_mode": mode, "cursor_column": "updated_at", "primary_key": "id",
        "cursor_semantics": "modification_timestamp",
        "mappings": [{"source": "id", "target": "id"}], **extra,
    })


@pytest.mark.parametrize(
    "table,mode", [("mx4_ia", "incremental_append"), ("mx4_iu", "incremental_deduped")]
)
def test_review_on_another_table_does_not_hold_the_schedule(env, table, mode):
    sched = _schedule(table, mode)
    request = runner.build_schedule_request(sched, _SRC, _DST)
    assert request is not None


@pytest.mark.parametrize("table", ["mx4_cdc", "public.mx4_cdc", '"public"."mx4_cdc"', "MX4_CDC"])
def test_review_holds_the_schedule_on_its_own_table_and_names_a_reachable_remedy(env, table):
    sched = _schedule(table, "incremental_deduped")
    with pytest.raises(ValueError) as caught:
        runner.build_schedule_request(sched, _SRC, _DST)
    msg = str(caught.value)
    assert env["id"] in msg
    assert "acknowledge_cdc_mapping_review" in msg
    assert f"/api/v1/cdc/mapping-reviews/{env['id']}/acknowledge" in msg
    assert "list_cdc_mapping_reviews" in msg
    assert "re-enable" not in msg


def test_review_holds_a_multi_stream_schedule_that_reads_its_table(env):
    sched = _schedule("mx4_ia", "incremental_append", stream_contracts=[
        {"selected": True, "name": "mx4_ia", "stream": "mx4_ia"},
        {"selected": True, "name": "mx4_cdc", "stream": "mx4_cdc"},
    ])
    with pytest.raises(ValueError, match="CDC mapping review required"):
        runner.build_schedule_request(sched, _SRC, _DST)


def test_other_schema_with_same_table_name_is_not_held(env):
    sched = _schedule("audit.mx4_cdc", "incremental_append")
    assert runner.build_schedule_request(sched, _SRC, _DST) is not None


def test_review_lookup_is_per_table_and_empty_table_is_not_a_wildcard(env):
    key = "connector:src-pg"
    assert rev.open_review_for_source(key, "mx4_cdc")["id"] == env["id"]
    assert rev.open_review_for_source(key, "mx4_ia") is None
    assert rev.open_review_for_source(key, "") is None
    assert rev.open_review_for_tables(key, []) is None
    assert rev.review_matches_table('"public"."mx4_cdc"', "mx4_cdc")
    assert not rev.review_matches_table('"public"."mx4_cdc"', "mx4_cdc_old")
    assert not rev.review_matches_table('"public"."mx4_cdc"', "other.mx4_cdc")
    assert not rev.review_matches_table("", "mx4_cdc")


def test_acknowledge_clears_only_that_review(env):
    key = "connector:src-pg"
    other = rev.flag_mapping_review(source_key=key, table='"public"."mx4_b"', reason="cdc_schema_drift")
    assert rev.acknowledge_review(env["id"])["status"] == "acknowledged"
    assert rev.open_review_for_source(key, "mx4_cdc") is None
    assert rev.open_review_for_source(key, "mx4_b")["id"] == other["id"]


def test_mcp_lists_and_acknowledges_a_review(env, monkeypatch):
    from src.ai.copilot import lifecycle_tools as lt
    from src.ai.copilot.tool_permissions import (
        ACK_KIND_PERMISSIONS,
        MUTATE,
        READ,
        tool_requirement,
    )
    from src.ai.copilot.tools import TOOL_DEFINITIONS
    from services.rbac import Permission

    names = {t["name"] for t in TOOL_DEFINITIONS}
    assert {"list_cdc_mapping_reviews", "acknowledge_cdc_mapping_review"} <= names
    # Same permissions as the REST routes (GET job.read, POST ack job.manage).
    assert tool_requirement("list_cdc_mapping_reviews") == (Permission.JOB_READ, READ)
    assert tool_requirement("acknowledge_cdc_mapping_review") == (Permission.JOB_MANAGE, MUTATE)
    assert ACK_KIND_PERMISSIONS["acknowledge_cdc_mapping_review"] == Permission.JOB_MANAGE

    monkeypatch.setattr(lt, "_connector_dict", lambda cid, name, exact_name=False: dict(_SRC))
    listed = lt.list_cdc_mapping_reviews(name="QA-Postgres")
    assert listed.success, listed.error
    assert [r["id"] for r in listed.output["reviews"]] == [env["id"]]
    assert "acknowledge_cdc_mapping_review" in listed.output["next"]
    assert lt.list_cdc_mapping_reviews(name="QA-Postgres", table="mx4_ia").output["count"] == 0

    assert lt.stage_acknowledge_cdc_mapping_review("").success is False
    assert lt.stage_acknowledge_cdc_mapping_review("no-such-review").success is False
    staged = lt.stage_acknowledge_cdc_mapping_review(env["id"], "tier column mapped")
    assert staged.success, staged.error
    assert staged.output["requires_confirm"] is True
    assert staged.output["action"] == "acknowledge_cdc_mapping_review"
    # Staging changes nothing.
    assert rev.open_review_for_source("connector:src-pg", "mx4_cdc") is not None

    from starlette.requests import Request

    from src.routers.copilot_router import _run_lifecycle_confirm

    http_request = Request({"type": "http", "method": "POST", "path": "/", "headers": []})
    result = asyncio.run(_run_lifecycle_confirm(
        "acknowledge_cdc_mapping_review", {"review_id": env["id"]}, http_request, None,
    ))
    assert result["status"] == "acknowledged"
    assert rev.open_review_for_source("connector:src-pg", "mx4_cdc") is None
    sched = _schedule("mx4_cdc", "incremental_deduped")
    assert runner.build_schedule_request(sched, _SRC, _DST) is not None
