"""Schedule delete releases a PostgreSQL CDC slot/publication — with guards.

Live half runs against localhost PostgreSQL (wal_level=logical); the guard
half uses fakes so it runs everywhere.
"""

from __future__ import annotations

import socket
import sys
import uuid
from pathlib import Path

import psycopg2
import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.postgresql_change_stream import (
    _publication_name,
    _slot_name,
    release_pg_capture,
)
from connectors.postgresql_conn import get_connection
from services import cdc_capture_release as mod
from services.cdc_multi_table import shared_route_cursor_key
from services.schedule_store import PipelineSchedule

CFG = {
    "host": "localhost",
    "port": 5432,
    "database": "dataflow",
    "username": "dataflow",
    "password": "dataflow",
    "connection_string": "",
    "ssl": False,
}


def _logical_ready() -> bool:
    try:
        socket.create_connection(("localhost", 5432), timeout=1).close()
        with get_connection(**CFG) as conn, conn.cursor() as cur:
            cur.execute("SHOW wal_level")
            row = cur.fetchone()
            return bool(row) and row[0] == "logical"
    except (OSError, psycopg2.Error):
        return False


live = pytest.mark.skipif(not _logical_ready(), reason="no logical PostgreSQL on :5432")


def _sched(**over) -> PipelineSchedule:
    base = dict(
        id="s1",
        name="s1",
        source_connector_id="src",
        source_table="orders",
        dest_connector_id="dst",
        dest_table="orders",
        interval="hourly",
        sync_mode="cdc",
        last_job_id="job1",
        workspace_id="ws",
    )
    base.update(over)
    return PipelineSchedule(**base)


def test_shared_route_key_is_job_independent() -> None:
    a = shared_route_cursor_key(
        engine="postgresql", database="app", tables=["orders", "users"],
        dest_type="mysql", dest_database="dw",
    )
    b = shared_route_cursor_key(
        engine="postgresql", database="app", tables=["users", "orders"],
        dest_type="mysql", dest_database="dw",
    )
    c = shared_route_cursor_key(
        engine="postgresql", database="app", tables=["orders", "users"],
        dest_type="mysql", dest_database="other",
    )
    assert a == b  # table order is irrelevant; the run id never enters the key
    assert a != c  # a different destination is a different capture route


def test_non_cdc_and_never_ran_are_noops() -> None:
    assert mod.release_schedule_cdc_capture(_sched(sync_mode="incremental")) == {
        "released": False,
        "reason": "not_cdc",
    }
    assert mod.release_schedule_cdc_capture(_sched(last_job_id=None))["reason"] == "never_ran"


def test_route_shared_by_another_cdc_schedule_keeps_slot(monkeypatch) -> None:
    twin = _sched(id="s2")
    monkeypatch.setattr(mod, "list_schedules", lambda: [_sched(), twin])
    out = mod.release_schedule_cdc_capture(_sched())
    assert out == {"released": False, "reason": "route_shared"}

    other_ws = _sched(id="s3", workspace_id="tenant-b")
    monkeypatch.setattr(mod, "list_schedules", lambda: [_sched(), other_ws])
    non_cdc = _sched(id="s4", sync_mode="full_refresh_append")
    assert mod.route_still_in_use(_sched()) is False
    monkeypatch.setattr(mod, "list_schedules", lambda: [_sched(), non_cdc])
    assert mod.route_still_in_use(_sched()) is False


class _Connector:
    type = "postgresql"

    def to_dict(self):
        return dict(CFG)


def _wire(
    monkeypatch,
    *,
    job: dict | None = None,
    jobs: dict[str, dict] | None = None,
    release,
    derived: list[dict] | None = None,
    watermark_keys: dict[str, list[str]] | None = None,
    tmp_path: Path | None = None,
):
    import services.connector_store as cs
    import services.mongodb_service as ms
    import services.sync_cursor as sc
    import connectors.postgresql_change_stream as pcs

    by_id = dict(jobs or {})

    class _Svc:
        def get_job(self, _id):
            return by_id.get(_id, job) if jobs is not None else job

    monkeypatch.setattr(ms, "get_mongodb_service", lambda: _Svc())
    monkeypatch.setattr(cs, "get_connector", lambda _id, _ws=None: _Connector())
    monkeypatch.setattr(pcs, "release_pg_capture", release)
    monkeypatch.setattr(mod, "list_schedules", lambda: [_sched()])
    monkeypatch.setattr(mod, "_derived_route_identities", lambda _s: list(derived or []))
    monkeypatch.setattr(
        sc, "cursor_keys_for_job", lambda jid: list((watermark_keys or {}).get(jid, []))
    )
    monkeypatch.setattr(sc, "get_watermark", lambda _k: None)
    ledger = (tmp_path or Path(__import__("tempfile").mkdtemp())) / "ledger.json"
    monkeypatch.setattr(mod, "_ledger_path", lambda: ledger)
    monkeypatch.setattr(mod, "_ledger_coll", lambda: None)
    cleared: list[str] = []
    monkeypatch.setattr(
        mod, "clear_watermark", lambda k: cleared.append(k) or {"cleared": True}
    )
    return cleared


def test_active_slot_is_never_dropped_and_watermark_kept(monkeypatch) -> None:
    cleared = _wire(
        monkeypatch,
        job={"cursor_key": "k", "cursor_value": "slot=df_x|phase=streaming|lsn=0/1"},
        release=lambda cfg, *, slot_name, publication_name: {
            "slot_name": slot_name,
            "publication_name": publication_name,
            "slot": "active",
            "publication": "absent",
        },
    )
    out = mod.release_schedule_cdc_capture(_sched())
    assert out["released"] is False
    assert out["reason"] == "slot_active"
    assert out["slot_name"] == "df_x"  # slot identity comes from the run's token
    assert cleared == []


def test_unreachable_source_reports_manual_next_action(monkeypatch) -> None:
    def boom(cfg, *, slot_name, publication_name):
        raise psycopg2.OperationalError("connection refused")

    cleared = _wire(
        monkeypatch,
        job={"cursor_key": "k", "cursor_value": "slot=df_x|phase=streaming|lsn=0/1"},
        release=boom,
    )
    out = mod.release_schedule_cdc_capture(_sched())
    assert out["reason"] == "source_unreachable"
    assert "pg_drop_replication_slot('df_x')" in out["next_action"]
    assert cleared == []


def test_release_drops_slot_publication_and_watermark(monkeypatch) -> None:
    calls: list[tuple[str, str]] = []

    def rel(cfg, *, slot_name, publication_name):
        calls.append((slot_name, publication_name))
        return {"slot": "dropped", "publication": "dropped"}

    cleared = _wire(monkeypatch, job={"cursor_key": "route-k", "cursor_value": ""}, release=rel)
    out = mod.release_schedule_cdc_capture(_sched())
    assert out["released"] is True and out["reason"] == "ok"
    assert cleared == ["route-k"]
    # No token on the job → the reader's default naming is what gets dropped.
    assert calls == [
        (
            _slot_name("dataflow", "orders", "route-k"),
            _publication_name("dataflow", "orders", "route-k"),
        )
    ]


def _dropping(calls: list[tuple[str, str]]):
    def rel(cfg, *, slot_name, publication_name):
        calls.append((slot_name, publication_name))
        return {"slot": "dropped", "publication": "dropped"}

    return rel


def test_s14_failed_last_job_without_cursor_key_still_releases(monkeypatch, tmp_path) -> None:
    """QA S14: the last attempt failed before stamping cursor_key. An earlier
    run in history carries it — that slot must be dropped, not leaked."""
    calls: list[tuple[str, str]] = []
    cleared = _wire(
        monkeypatch,
        jobs={
            "job2": {"status": "failed"},
            "job1": {"cursor_key": "route-k", "cursor_value": "slot=df_real|phase=streaming"},
        },
        release=_dropping(calls),
        tmp_path=tmp_path,
    )
    sched = _sched(
        last_job_id="job2",
        run_history=[{"job_id": "job1", "status": "completed"}, {"job_id": "job2", "status": "failed"}],
    )
    out = mod.release_schedule_cdc_capture(sched)
    assert out["released"] is True, out
    assert out["reason"] == "ok"
    assert [c[0] for c in calls] == ["df_real"]
    assert cleared == ["route-k"]


def test_s14_identity_from_watermark_metadata(monkeypatch, tmp_path) -> None:
    calls: list[tuple[str, str]] = []
    _wire(
        monkeypatch,
        jobs={"job1": {"status": "failed"}},
        release=_dropping(calls),
        watermark_keys={"job1": ["wm-k"]},
        tmp_path=tmp_path,
    )
    out = mod.release_schedule_cdc_capture(_sched())
    assert out["released"] is True
    assert calls == [(_slot_name("dataflow", "orders", "wm-k"), _publication_name("dataflow", "orders", "wm-k"))]


def test_s14_identity_rederived_from_route_when_no_job_has_it(monkeypatch, tmp_path) -> None:
    calls: list[tuple[str, str]] = []
    _wire(
        monkeypatch,
        jobs={"job1": {"status": "failed"}},
        release=_dropping(calls),
        derived=[{"cursor_key": "derived-k", "tables": "orders"}],
        tmp_path=tmp_path,
    )
    out = mod.release_schedule_cdc_capture(_sched())
    assert out["released"] is True and out["reason"] == "ok"
    assert calls[0][0] == _slot_name("dataflow", "orders", "derived-k")


def test_s14_multi_table_route_releases_shared_and_per_table_slots(monkeypatch, tmp_path) -> None:
    calls: list[tuple[str, str]] = []
    _wire(
        monkeypatch,
        jobs={"job1": {"status": "failed"}},
        release=_dropping(calls),
        derived=[
            {"cursor_key": "shared-k", "tables": ["orders", "users"]},
            {"cursor_key": "orders-k", "tables": "orders"},
            {"cursor_key": "users-k", "tables": "users"},
        ],
        tmp_path=tmp_path,
    )
    out = mod.release_schedule_cdc_capture(_sched())
    assert out["released"] is True
    assert len(calls) == 3
    assert len(out["slots"]) == 3


def test_s14_no_identity_anywhere_is_recorded_not_silent(monkeypatch, tmp_path) -> None:
    _wire(monkeypatch, jobs={"job1": {}}, release=_dropping([]), tmp_path=tmp_path)
    out = mod.release_schedule_cdc_capture(_sched())
    assert out["released"] is False
    assert out["reason"] == "identity_unknown"
    assert out["pending_id"]
    assert mod.list_pending_capture_releases()[0]["schedule_id"] == "s1"


def test_s14_unreachable_release_is_retried_by_the_beat(monkeypatch, tmp_path) -> None:
    import connectors.postgresql_change_stream as pcs

    def boom(cfg, *, slot_name, publication_name):
        raise psycopg2.OperationalError("connection refused")

    cleared = _wire(
        monkeypatch,
        job={"cursor_key": "k", "cursor_value": "slot=df_x|phase=streaming"},
        release=boom,
        tmp_path=tmp_path,
    )
    out = mod.release_schedule_cdc_capture(_sched())
    assert out["reason"] == "source_unreachable"
    pending = mod.list_pending_capture_releases()
    assert len(pending) == 1 and pending[0]["slots"][0]["slot_name"] == "df_x"
    assert cleared == []

    calls: list[tuple[str, str]] = []
    monkeypatch.setattr(pcs, "release_pg_capture", _dropping(calls))
    outcomes = mod.retry_pending_capture_releases()
    assert outcomes == [{"id": pending[0]["id"], "released": True}]
    assert calls[0][0] == "df_x"
    assert cleared == ["k"]
    assert mod.list_pending_capture_releases() == []


def test_retry_backs_off_after_a_failed_attempt() -> None:
    now = mod.datetime.now(mod.timezone.utc)
    fresh = {"recorded_at": now.isoformat()}
    assert mod._retry_due(fresh, now) is True
    tried = {"recorded_at": now.isoformat(), "last_attempt_at": now.isoformat(), "attempts": 1}
    assert mod._retry_due(tried, now) is False
    from datetime import timedelta

    assert mod._retry_due(tried, now + timedelta(seconds=121)) is True


def test_route_identity_matches_the_cdc_run_paths() -> None:
    """Release must derive the very key the run opened the slot under."""
    from services.cdc_multi_table import shared_route_cursor_key
    from services.sync_cursor import build_cursor_key
    from src.transfer.cdc_transfer import cdc_route_cursor_keys
    from src.transfer.models import EndpointConfig

    src = EndpointConfig(kind="database", format="postgresql", database="app", table="orders")
    dst = EndpointConfig(kind="database", format="postgresql", database="dw", table="orders_wh")
    single = cdc_route_cursor_keys(src, dst, [{"name": "orders", "selected": True}])
    assert single == [{
        "cursor_key": build_cursor_key(
            source_type="postgresql", source_database="app", source_object="orders",
            dest_type="postgresql", dest_database="dw", dest_object="orders_wh",
            stream_name="orders",
        ),
        "tables": "orders",
    }]

    multi = cdc_route_cursor_keys(
        src, dst,
        [{"name": "orders", "selected": True}, {"name": "users", "selected": True}],
    )
    keys = [m["cursor_key"] for m in multi]
    assert keys[0] == shared_route_cursor_key(
        engine="postgresql", database="app", tables=["orders", "users"],
        dest_type="postgresql", dest_database="dw",
    )
    # Sequential fallback opens one slot per table under its own key.
    assert len(keys) == 3 and len(set(keys)) == 3
    assert multi[1]["tables"] == "orders" and multi[2]["tables"] == "users"


@live
def test_live_release_is_idempotent_and_leaves_active_slots() -> None:
    suffix = uuid.uuid4().hex[:8]
    slot = f"df_test_release_{suffix}"
    pub = f"df_pub_test_release_{suffix}"
    table = f"df_rel_{suffix}"
    setup = get_connection(**CFG)
    setup.autocommit = True
    with setup.cursor() as cur:
        cur.execute(f'CREATE TABLE "{table}"(id int primary key)')
        cur.execute(f'CREATE PUBLICATION "{pub}" FOR TABLE "{table}"')
        cur.execute(
            "SELECT pg_create_logical_replication_slot(%s, 'test_decoding')", (slot,)
        )
    setup.close()
    try:
        first = release_pg_capture(CFG, slot_name=slot, publication_name=pub)
        assert first["slot"] == "dropped" and first["publication"] == "dropped"
        second = release_pg_capture(CFG, slot_name=slot, publication_name=pub)
        assert second["slot"] == "absent" and second["publication"] == "absent"
        with get_connection(**CFG) as conn, conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_replication_slots WHERE slot_name=%s", (slot,))
            assert cur.fetchone() is None
            cur.execute("SELECT 1 FROM pg_publication WHERE pubname=%s", (pub,))
            assert cur.fetchone() is None
    finally:
        with get_connection(**CFG) as conn, conn.cursor() as cur:
            conn.autocommit = True
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
