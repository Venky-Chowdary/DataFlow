"""Owner decision 2: Mirror soft-deletes by default; hard delete warns first.

QA on 557f556c: a mirror run physically deleted destination rows (552 -> 540)
although Studio, schedules and the docs promise ``_deleted`` soft deletes, and
nothing said so before the run. Round 7 made soft delete the engine default.
The owner's decision adds: a hard delete stays available only as an explicit
``delete_mode=hard`` and is announced at plan / preflight time — in Studio's
Validate, in the MCP plan, for a schedule's stored contract — with one wording
the job ledger and the web app share.

Routes driven here, PostgreSQL -> PostgreSQL, run 1 and run 2 after source
deletes, NULL / '' / whitespace / unicode survivors:

* Studio REST: ``/api/v1/preflight/run`` then ``/api/v1/transfer/execute``;
* MCP: ``start_transfer`` on ``/api/v1/mcp`` then ``/api/v1/copilot/confirm``;
* schedule: ``build_schedule_request`` from a stored schedule, then the engine.
"""

from __future__ import annotations

import re
import uuid
from pathlib import Path

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(600)

_ROWS = [(1, "alpha"), (2, None), (3, ""), (4, "   "), (5, "Zoë ✓ 中文"), (6, "f")]
_DELETED = (2, 4)
_WEB_WORDING = (
    Path(__file__).resolve().parents[2] / "web" / "src" / "lib" / "mirrorDeleteMode.ts"
)


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


@pytest.fixture
def pg_tables():
    h.dest_config("postgresql")  # skips when PostgreSQL is absent
    token = uuid.uuid4().hex[:8]
    names = {"src": f"r8m_s_{token}", "dst": f"r8m_d_{token}", "token": token}
    h.execute("postgresql", f'CREATE TABLE "{names["src"]}" (id INT PRIMARY KEY, note TEXT NULL)')
    for rid, note in _ROWS:
        lit = "NULL" if note is None else "'" + note.replace("'", "''") + "'"
        h.execute("postgresql", f'INSERT INTO "{names["src"]}" VALUES ({rid}, {lit})')  # nosec B608
    try:
        yield names
    finally:
        h.drop_table("postgresql", names["src"])
        h.drop_table("postgresql", names["dst"])


def _connectors(token: str):
    from services.connector_store import create_connector

    cfg = {"role": "source", "workspace_id": "", "ssl": False, **h.dest_config("postgresql")}
    src = create_connector({**cfg, "name": f"r8m-src-{token}"})
    dst = create_connector({**cfg, "role": "destination", "name": f"r8m-dst-{token}"})
    return src, dst


def _dest(table: str) -> list[tuple]:
    cols = {r[0] for r in h.query(
        "postgresql",
        f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}'",  # nosec B608
    )}
    flag = '"_deleted"' if "_deleted" in cols else "FALSE"
    return [
        (int(r[0]), r[1], bool(r[2]))
        for r in h.query("postgresql", f'SELECT id, note, {flag} FROM "{table}" ORDER BY id')  # nosec B608
    ]


def _assert_soft(table: str) -> None:
    got = _dest(table)
    assert len(got) == len(_ROWS), got  # physical rows kept
    assert {r[0] for r in got if r[2]} == set(_DELETED), got
    assert [(r[0], r[1]) for r in got if not r[2]] == [r for r in _ROWS if r[0] not in _DELETED]


def _assert_hard(table: str) -> None:
    got = _dest(table)
    assert [(r[0], r[1]) for r in got] == [r for r in _ROWS if r[0] not in _DELETED], got
    assert not any(r[2] for r in got)


def _ledger(job: dict, mode: str) -> None:
    from services.mirror_delete_notice import MIRROR_LEDGER_HARD_TEXT, MIRROR_LEDGER_SOFT_TEXT

    ra = job.get("row_accounting") or {}
    assert ra.get("conservation_kind") == "mirror", ra
    assert ra.get("balanced") is True, ra
    assert ra.get("mirror_delete_mode") == mode, ra
    assert (MIRROR_LEDGER_HARD_TEXT if mode == "hard" else MIRROR_LEDGER_SOFT_TEXT) in str(ra.get("note")), ra


def _hard_warning(warnings) -> dict | None:
    return next(
        (w for w in warnings or [] if isinstance(w, dict) and w.get("id") == "mirror_hard_delete"),
        None,
    )


# --------------------------------------------------------------- Studio REST


def _studio_run(client, src, dst, src_table, dest_table, extra=None) -> tuple[dict, dict]:
    contract = {"name": src_table, "selected": True, "sync_mode": "mirror",
                "primary_key": "id", **(extra or {})}
    mappings = [{"source": c, "target": c, "confidence": 1.0} for c in ("id", "note")]
    pf = client.post("/api/v1/preflight/run", json={
        "dataset_name": src_table, "columns": ["id", "note"],
        "column_types": {"id": "integer", "note": "text"},
        "mappings": mappings, "source_connector_id": src.id,
        "source_kind": "database", "source_type": "postgresql",
        "source_table": src_table, "connector_id": dst.id,
        "dest_kind": "database", "dest_type": "postgresql",
        "dest_table": dest_table, "sync_mode": "mirror",
        "stream_contracts": [contract], "validation_mode": "strict",
    })
    assert pf.status_code == 200, pf.text
    resp = client.post("/api/v1/transfer/execute", json={
        "source": {"kind": "database", "format": "postgresql", "connector_id": src.id, "table": src_table},
        "destination": {"kind": "database", "format": "postgresql", "connector_id": dst.id, "table": dest_table},
        "mappings": mappings, "sync_mode": "mirror", "stream_contracts": [contract],
        "validation_mode": "strict", "async_mode": False, "skip_preflight": True,
    })
    assert resp.status_code == 200, resp.text
    from services.mongodb_service import get_mongodb_service

    job = get_mongodb_service().get_job(str(resp.json().get("job_id") or "")) or {}
    assert job.get("status") == "completed", {k: job.get(k) for k in ("status", "error")}
    return pf.json(), job


def test_studio_rest_hard_delete_is_warned_at_preflight_and_soft_is_the_default(client, pg_tables):
    from services.mirror_delete_notice import MIRROR_HARD_DELETE_WARNING, MIRROR_SOFT_DELETE_TEXT

    src_t, dst_t = pg_tables["src"], pg_tables["dst"]
    hard_t = dst_t + "_h"
    src, dst = _connectors(pg_tables["token"])
    try:
        pf, _ = _studio_run(client, src, dst, src_t, dst_t)
        assert _hard_warning(pf.get("warnings")) is None, pf.get("warnings")
        assert pf.get("mirror_delete") == {
            "mode": "soft", "physical_delete": False, "message": MIRROR_SOFT_DELETE_TEXT, "streams": [],
        }
        pf_h, _ = _studio_run(client, src, dst, src_t, hard_t, {"delete_mode": "hard"})
        warning = _hard_warning(pf_h.get("warnings"))
        assert warning and warning["message"] == MIRROR_HARD_DELETE_WARNING, pf_h.get("warnings")
        assert "physically deleted" in warning["message"]
        assert pf_h["warnings"][0] is warning  # first, never truncated away
        assert pf_h["mirror_delete"]["mode"] == "hard" and pf_h["mirror_delete"]["streams"] == [src_t]

        h.execute("postgresql", f'DELETE FROM "{src_t}" WHERE id IN {_DELETED}')  # nosec B608
        _, job = _studio_run(client, src, dst, src_t, dst_t)
        _assert_soft(dst_t)
        _ledger(job, "soft")
        pf_h2, job_h = _studio_run(client, src, dst, src_t, hard_t, {"delete_mode": "hard"})
        assert _hard_warning(pf_h2.get("warnings")), "run 2 warns again"
        _assert_hard(hard_t)
        _ledger(job_h, "hard")
    finally:
        h.drop_table("postgresql", hard_t)


# ------------------------------------------------------------------------ MCP


def _mcp_stage(client, src_id, dst_id, src_t, dest_t, **extra) -> dict:
    ok, staged = h.mcp_call(client, "start_transfer", {
        "source_connector_id": src_id, "source_table": src_t,
        "dest_connector_id": dst_id, "dest_table": dest_t,
        "sync_mode": "mirror", "primary_key": "id", "validation_mode": "strict", **extra,
    })
    assert ok, staged
    assert not h.blockers_of(staged), h.blockers_of(staged)
    return staged


def test_mcp_hard_delete_is_warned_in_the_plan_and_soft_is_the_default(client, pg_tables):
    from services.mirror_delete_notice import MIRROR_HARD_DELETE_WARNING

    src_t, dst_t = pg_tables["src"], pg_tables["dst"]
    hard_t = dst_t + "_h"
    src, dst = _connectors(pg_tables["token"])
    try:
        soft = _mcp_stage(client, src.id, dst.id, src_t, dst_t)
        plan = soft.get("plan") or soft
        assert _hard_warning((plan.get("preflight") or {}).get("warnings")) is None
        assert (plan.get("mirror_delete") or {}).get("mode") == "soft", plan.get("mirror_delete")
        h.confirm(client, soft["ack_id"])

        hard = _mcp_stage(client, src.id, dst.id, src_t, hard_t, delete_mode="hard")
        plan_h = hard.get("plan") or hard
        warning = _hard_warning((plan_h.get("preflight") or {}).get("warnings"))
        assert warning and warning["message"] == MIRROR_HARD_DELETE_WARNING, plan_h.get("preflight")
        assert plan_h["mirror_delete"]["physical_delete"] is True
        assert [c.get("delete_mode") for c in plan_h.get("stream_contracts") or []] == ["hard"]
        h.confirm(client, hard["ack_id"])

        ok, refused = h.mcp_call(client, "plan_transfer", {
            "source_connector_id": src.id, "source_table": src_t, "dest_connector_id": dst.id,
            "dest_table": dst_t, "sync_mode": "incremental_upsert", "primary_key": "id",
            "cursor_column": "id", "delete_mode": "hard",
        })
        assert not ok and "mirror sync only" in str(refused), refused

        h.execute("postgresql", f'DELETE FROM "{src_t}" WHERE id IN {_DELETED}')  # nosec B608
        job = h.confirm(client, _mcp_stage(client, src.id, dst.id, src_t, dst_t)["ack_id"])
        _assert_soft(dst_t)
        _ledger(job, "soft")
        job_h = h.confirm(
            client, _mcp_stage(client, src.id, dst.id, src_t, hard_t, delete_mode="hard")["ack_id"]
        )
        _assert_hard(hard_t)
        _ledger(job_h, "hard")
    finally:
        h.drop_table("postgresql", hard_t)


# ------------------------------------------------------------------- schedule


def test_schedule_contract_hard_delete_and_default_soft_reach_the_ledger(client, pg_tables):
    import services.schedule_runner as runner
    import services.schedule_store as store
    from services.mongodb_service import get_mongodb_service
    from src.transfer.engine import UniversalTransferEngine

    src_t, dst_t = pg_tables["src"], pg_tables["dst"]
    hard_t = dst_t + "_h"
    src, dst = _connectors(pg_tables["token"])
    cfg = h.dest_config("postgresql")

    def schedule(dest_table: str, extra: dict) -> store.PipelineSchedule:
        return store.PipelineSchedule.from_dict({
            "id": "r8m-" + uuid.uuid4().hex[:8], "name": "r8 mirror",
            "source_connector_id": src.id, "source_table": src_t,
            "dest_connector_id": dst.id, "dest_table": dest_table,
            "interval": "hourly", "sync_mode": "mirror", "primary_key": "id",
            "stream_contracts": [
                {"name": src_t, "selected": True, "primary_key": ["id"], "sync_mode": "mirror", **extra}
            ],
            "mappings": [{"source": c, "target": c, "confidence": 1.0} for c in ("id", "note")],
        })

    def run(sched) -> dict:
        request = runner.build_schedule_request(sched, dict(cfg), dict(cfg))
        job_id = "r8m" + uuid.uuid4().hex[:16]
        get_mongodb_service().update_job_status(job_id, "pending", transfer_request={})
        result = UniversalTransferEngine().execute_tracked(request, job_id)
        assert result.success, result.error
        return get_mongodb_service().get_job(job_id) or {}

    try:
        soft_s, hard_s = schedule(dst_t, {}), schedule(hard_t, {"delete_mode": "hard"})
        from services.mirror_delete_notice import mirror_delete_notice

        # What the schedule form and Validate say for the stored contracts.
        assert mirror_delete_notice("mirror", soft_s.stream_contracts)["mode"] == "soft"
        assert mirror_delete_notice("mirror", hard_s.stream_contracts)["physical_delete"] is True
        run(soft_s), run(hard_s)
        h.execute("postgresql", f'DELETE FROM "{src_t}" WHERE id IN {_DELETED}')  # nosec B608
        _ledger(run(soft_s), "soft")
        _assert_soft(dst_t)
        _ledger(run(hard_s), "hard")
        _assert_hard(hard_t)
    finally:
        h.drop_table("postgresql", hard_t)


# ------------------------------------------------------- one wording, two apps


def test_web_app_uses_the_engine_wording_verbatim():
    from services import mirror_delete_notice as notice

    text = _WEB_WORDING.read_text(encoding="utf-8")
    for name in (
        "MIRROR_SOFT_DELETE_TEXT",
        "MIRROR_HARD_DELETE_WARNING",
        "MIRROR_LEDGER_HARD_TEXT",
        "MIRROR_LEDGER_SOFT_TEXT",
    ):
        match = re.search(rf'export const {name} =\s*"((?:[^"\\]|\\.)*)";', text)
        assert match, name
        assert match.group(1) == getattr(notice, name), name
