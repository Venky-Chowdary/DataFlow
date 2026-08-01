"""Wave 83: Zendesk domains + HubSpot epoch wire + composite snapshot window + GTID.

Research anchors
----------------
- Zendesk Tickets API: tagger/dropdown/multiselect use ``custom_field_options[].value``
  tags (not display names); system status/priority/type are closed domains.
- HubSpot datetime = UTC epoch millis; date = YYYY-MM-DD (Census/Airbyte class).
- Debezium DDD-3: composite PK chunk windows still stream-wins per logical row.
- Debezium DBZ-3577: read-only incremental snapshots use GTID set containment
  as watermark (not invent newer via string order).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_zendesk_tagger_multiselect_and_system_enums():
    from connectors.zendesk_writer import zendesk_field_to_carrier

    tagger = zendesk_field_to_carrier(
        {
            "type": "tagger",
            "custom_field_options": [
                {"name": "Apple Pie", "value": "apple"},
                {"name": "Pecan", "value": "pecan"},
            ],
        }
    )
    assert tagger == "ENUM('apple','pecan')"
    assert "Apple" not in tagger

    multi = zendesk_field_to_carrier(
        {
            "type": "multiselect",
            "custom_field_options": [
                {"value": "hd_3000"},
                {"value": "hd_5555"},
            ],
        }
    )
    assert multi.startswith("SET(")
    assert "hd_3000" in multi

    assert "new" in zendesk_field_to_carrier({"type": "status"})
    assert "urgent" in zendesk_field_to_carrier({"type": "priority"})


def test_hubspot_datetime_epoch_millis_wire():
    from connectors.hubspot_writer import (
        coerce_hubspot_date_wire,
        coerce_hubspot_datetime_wire,
    )

    # Known UTC instant: 2024-01-01T00:00:00Z = 1704067200000
    assert coerce_hubspot_datetime_wire("2024-01-01T00:00:00Z") == "1704067200000"
    assert coerce_hubspot_datetime_wire(1704067200) == "1704067200000"  # seconds
    assert coerce_hubspot_datetime_wire(1704067200000) == "1704067200000"
    assert coerce_hubspot_date_wire("2024-01-15T12:00:00Z") == "2024-01-15"
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_hubspot_datetime_wire("not-a-date")


def test_composite_pk_snapshot_window_stream_wins():
    from services.cdc_snapshot_window import SnapshotWindow

    win = SnapshotWindow(window_id="w-comp", primary_key=["tenant_id", "id"])
    win.open_window()
    win.add_snapshot_rows(
        [
            {"tenant_id": "t1", "id": "1", "v": "snap"},
            {"tenant_id": "t1", "id": "2", "v": "snap"},
        ]
    )
    win.apply_stream_event(
        op="u", row={"tenant_id": "t1", "id": "1", "v": "live"}
    )
    emitted = win.close_window()
    by_key = {(r["tenant_id"], r["id"]): r for r in emitted if not r.get("__deleted")}
    assert by_key[("t1", "1")]["v"] == "live"
    assert by_key[("t1", "2")]["v"] == "snap"
    assert win.stats()["stream_overrides"] == 1


def test_gtid_set_containment_watermark():
    from connectors.writer_common import gtid_set_contains, parse_mysql_gtid_set

    executed = "uuid-a:1-100,uuid-b:5-10"
    assert parse_mysql_gtid_set(executed)["uuid-a"] == [(1, 100)]
    assert gtid_set_contains(executed, "uuid-a:50")
    assert gtid_set_contains(executed, "uuid-a:1-50,uuid-b:5-5")
    assert not gtid_set_contains(executed, "uuid-a:101")
    assert not gtid_set_contains(executed, "uuid-c:1")
    # Empty needle is contained (window with no event).
    assert gtid_set_contains(executed, "")
    assert gtid_set_contains("gtid:uuid-a:1-10", "gtid:uuid-a:3")


def test_zendesk_reverse_etl_plan_registered():
    from services.reverse_etl import plan_activation, supported_activation_kinds

    assert "zendesk" in supported_activation_kinds()
    plan = plan_activation(
        destination_kind="zendesk",
        object_name="tickets",
        primary_key="id",
    )
    assert plan.batch_size == 100
    assert any("tagger" in n.lower() or "quarantine" in n.lower() for n in plan.notes)
