"""D96 + M24: schedules carry the declared key for mirror/scd2, and a paused
or parked schedule must not show a future ``next_run_at``.

Repro (M24): ``set_schedule_enabled false`` left ``next_run_at`` pointing at
the next cron slot — the paused pipeline advertised a run that never fired.
Repro (D96): the fired request's stream contract must carry the declared key
for mirror/scd2, which merge on it; a keyless contract fell back to name-shape
inference or refused at run time.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import services.schedule_runner as runner
import services.schedule_store as store
from src.routers.schedules_router import ScheduleResponse

_SRC_CONN = {
    "_id": "src", "id": "src", "type": "postgresql", "host": "h", "port": 5432,
    "database": "d", "username": "u", "password": "p",
}
_DST_CONN = {
    "_id": "dst", "id": "dst", "type": "snowflake", "host": "h2",
    "database": "wh", "username": "u", "password": "p",
}
_MAPPINGS = [{"source": "id", "target": "id"}]


@pytest.fixture
def temp_store(tmp_path, monkeypatch):
    path = tmp_path / "schedules.json"
    monkeypatch.setattr(store, "STORE_PATH", path)
    monkeypatch.setattr(store, "_mongo_backend", lambda: None)
    yield path


def _sched(**kw) -> store.PipelineSchedule:
    base = {
        "id": "s1", "name": "n", "source_connector_id": "src",
        "source_table": "orders", "dest_connector_id": "dst",
        "dest_table": "orders_wh", "interval": "hourly",
        "mappings": _MAPPINGS,
    }
    base.update(kw)
    return store.PipelineSchedule.from_dict(base)


# --------------------------------------------------------------------------- #
# D96: mirror / scd2 requests carry the declared key                          #
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("mode", ["mirror", "scd2"])
def test_schedule_request_carries_key_for_merge_modes(mode: str):
    sched = _sched(sync_mode=mode, primary_key="id")
    req = runner.build_schedule_request(sched, _SRC_CONN, _DST_CONN)
    contract = req.stream_contracts[0]
    assert contract["sync_mode"] == mode
    assert contract["primary_key"] == "id"
    assert (req.destination.extra or {}).get("primary_key") == "id"


@pytest.mark.parametrize("mode", ["mirror", "scd2"])
def test_schedule_key_fills_contract_missing_key(mode: str):
    """Contracts persisted without a key (older plans) get the schedule's."""
    sched = _sched(
        sync_mode=mode,
        primary_key="id",
        stream_contracts=[{"selected": True, "name": "orders"}],
    )
    req = runner.build_schedule_request(sched, _SRC_CONN, _DST_CONN)
    assert req.stream_contracts[0]["primary_key"] == "id"


def test_schedule_persists_primary_key(temp_store):
    sched = store.create_schedule({
        "name": "mirror orders",
        "source_connector_id": "src-1",
        "source_table": "orders",
        "dest_connector_id": "dst-1",
        "dest_table": "orders_wh",
        "interval": "hourly",
        "sync_mode": "mirror",
        "primary_key": "id",
        "mappings": _MAPPINGS,
    })
    assert sched.primary_key == "id"
    assert store.get_schedule(sched.id).primary_key == "id"


# --------------------------------------------------------------------------- #
# M24 / D96: paused or parked schedules show no future next_run_at            #
# --------------------------------------------------------------------------- #

def test_paused_schedule_next_run_null(temp_store):
    sched = store.create_schedule({
        "name": "p",
        "source_connector_id": "src-1",
        "source_table": "orders",
        "dest_connector_id": "dst-1",
        "dest_table": "orders_wh",
        "interval": "hourly",
        "sync_mode": "incremental_append",
        "mappings": _MAPPINGS,
    })
    assert sched.next_run_at

    paused = store.update_schedule(sched.id, {"enabled": False})
    assert paused is not None
    assert paused.enabled is False
    assert paused.next_run_at is None

    resumed = store.update_schedule(sched.id, {"enabled": True})
    assert resumed is not None
    assert resumed.enabled is True
    assert resumed.next_run_at  # recomputed on resume


def test_created_disabled_schedule_has_no_next_run(temp_store):
    sched = store.create_schedule({
        "name": "draft",
        "source_connector_id": "src-1",
        "source_table": "orders",
        "dest_connector_id": "dst-1",
        "dest_table": "orders_wh",
        "interval": "hourly",
        "sync_mode": "incremental_append",
        "mappings": _MAPPINGS,
        "enabled": False,
    })
    assert sched.enabled is False
    assert sched.next_run_at is None


def test_visible_next_run_at_masks_paused_and_parked():
    enabled = _sched(enabled=True, next_run_at="2099-01-01T00:00:00+00:00")
    assert store.visible_next_run_at(enabled) == "2099-01-01T00:00:00+00:00"

    paused = _sched(enabled=False, next_run_at="2099-01-01T00:00:00+00:00")
    assert store.visible_next_run_at(paused) is None

    parked = _sched(
        enabled=True,
        next_run_at="2099-01-01T00:00:00+00:00",
        approval_request={"status": "open", "finding": "x"},
    )
    assert store.visible_next_run_at(parked) is None

    closed = _sched(
        enabled=True,
        next_run_at="2099-01-01T00:00:00+00:00",
        approval_request={"status": "approved", "finding": "x"},
    )
    assert store.visible_next_run_at(closed) == "2099-01-01T00:00:00+00:00"


def test_schedule_response_next_run_null_when_paused_or_parked():
    paused = _sched(enabled=False, next_run_at="2099-01-01T00:00:00+00:00")
    assert ScheduleResponse.from_schedule(paused).next_run_at is None

    parked = _sched(
        enabled=True,
        next_run_at="2099-01-01T00:00:00+00:00",
        approval_request={"status": "open", "finding": "x"},
    )
    assert ScheduleResponse.from_schedule(parked).next_run_at is None

    enabled = _sched(enabled=True, next_run_at="2099-01-01T00:00:00+00:00")
    assert ScheduleResponse.from_schedule(enabled).next_run_at == "2099-01-01T00:00:00+00:00"
