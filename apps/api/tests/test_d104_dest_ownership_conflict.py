"""D104: one destination table, one writer.

A second pipeline (or one-shot transfer) onto a destination table another
enabled pipeline already writes used to be accepted silently — discovered
only at run time as a key collision with misleading guidance. The creation
surface (plan_transfer, start_transfer, create_schedule) must now refuse
unless the operator explicitly acknowledges the shared destination, and the
refusal must name the owning pipeline and its sync mode.
"""

from __future__ import annotations

import pytest

import services.schedule_store as schedule_store
from services.connector_store import create_connector, delete_connector
from services.schedule_store import (
    PipelineSchedule,
    conflicting_dest_writers,
    describe_dest_conflict,
)
from src.ai.copilot import transfer_tools


def _schedule(**kw) -> PipelineSchedule:
    base = dict(
        id="sched-a",
        name="Nightly HQ sync",
        source_connector_id="c-src",
        source_table="src_t",
        dest_connector_id="c-dst",
        dest_table="hq_shared",
        interval="daily",
        enabled=True,
        sync_mode="incremental_upsert",
    )
    base.update(kw)
    return PipelineSchedule(**base)


def test_enabled_same_destination_is_a_conflict(monkeypatch):
    sched = _schedule()
    monkeypatch.setattr(schedule_store, "_load_all", lambda: [sched])
    hits = conflicting_dest_writers("c-dst", "hq_shared")
    assert [s.id for s in hits] == ["sched-a"]


def test_table_match_folds_case(monkeypatch):
    sched = _schedule(dest_table="HQ_SHARED")
    monkeypatch.setattr(schedule_store, "_load_all", lambda: [sched])
    assert conflicting_dest_writers("c-dst", "hq_shared") == [sched]


def test_disabled_or_other_destination_is_not_a_conflict(monkeypatch):
    sched = _schedule()
    monkeypatch.setattr(
        schedule_store,
        "_load_all",
        lambda: [
            sched,
            _schedule(id="s-disabled", dest_table="hq_shared", enabled=False),
            _schedule(id="s-other-table", dest_table="other_t"),
            _schedule(id="s-other-conn", dest_connector_id="c-other"),
        ],
    )
    hits = conflicting_dest_writers("c-dst", "hq_shared")
    assert [s.id for s in hits] == ["sched-a"]
    # Excluding the schedule itself (an update of its own route) is fine.
    assert conflicting_dest_writers(
        "c-dst", "hq_shared", exclude_schedule_id="sched-a"
    ) == []


def test_conflict_description_names_pipeline_and_mode():
    text = describe_dest_conflict(_schedule())
    assert "Nightly HQ sync" in text
    assert "sched-a" in text
    assert "incremental_upsert" in text


@pytest.fixture()
def connectors():
    src = create_connector(
        {
            "name": "d104-src",
            "type": "postgresql",
            "host": "localhost",
            "port": 5432,
            "database": "dataflow",
            "username": "dataflow",
            "password": "dataflow",
            "schema": "public",
        }
    )
    dst = create_connector(
        {
            "name": "d104-dst",
            "type": "postgresql",
            "host": "localhost",
            "port": 5432,
            "database": "dataflow",
            "username": "dataflow",
            "password": "dataflow",
            "schema": "public",
        }
    )
    yield (
        str(getattr(src, "id", "") or ""),
        str(getattr(dst, "id", "") or ""),
    )
    delete_connector(str(getattr(src, "id", "") or ""))
    delete_connector(str(getattr(dst, "id", "") or ""))


def test_plan_transfer_refuses_shared_destination(
    monkeypatch, connectors
):
    src_id, dst_id = connectors
    sched = _schedule(
        source_connector_id=src_id,
        dest_connector_id=dst_id,
        dest_table="d104_shared",
    )
    monkeypatch.setattr(schedule_store, "_load_all", lambda: [sched])

    result = transfer_tools.plan_transfer(
        source_connector_id=src_id,
        source_table="d104_src",
        dest_connector_id=dst_id,
        dest_table="d104_shared",
        sync_mode="full_refresh_append",
    )
    assert result.success is False
    err = result.error or ""
    assert "already write" in err
    assert "Nightly HQ sync" in err
    assert "incremental_upsert" in err


def test_acknowledged_shared_destination_passes_the_gate(
    monkeypatch, connectors
):
    src_id, dst_id = connectors
    sched = _schedule(
        source_connector_id=src_id,
        dest_connector_id=dst_id,
        dest_table="d104_shared",
    )
    monkeypatch.setattr(schedule_store, "_load_all", lambda: [sched])

    result = transfer_tools.plan_transfer(
        source_connector_id=src_id,
        source_table="d104_src",
        dest_connector_id=dst_id,
        dest_table="d104_shared",
        sync_mode="full_refresh_append",
        shared_destination_acknowledged=True,
    )
    # The ownership gate must not be what stops this call.
    assert "already write" not in (result.error or "")
