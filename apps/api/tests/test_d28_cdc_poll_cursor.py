"""D28 — CDC on a PostgreSQL source with no change log ignored cursor_column.

With ``wal_level=replica`` the run falls back to polling a column. Pilot's
CDC contract deliberately drops a named table cursor (the log is the cursor
when one exists), so the fallback polled the primary key and refused with
"the poll cursor 'order_id' is the primary key" even though the operator had
named ``updated_at``. The named column now rides the contract as the poll
cursor only - never as the snapshot cursor - and the fallback and its
Validate gate use it.
"""

from __future__ import annotations

from services import cdc_log_capture_probe as probe_mod
from services.cdc_capability import CAUSE_SERVER_NOT_CONFIGURED
from services.preflight_population import _cdc_log_capture_gates
from services.sync_cursor import SyncContract
from src.ai.copilot.transfer_tools import _identity_stream_contract


def _contract(**kwargs):
    contracts, error = _identity_stream_contract(
        mode="cdc",
        source_table="orders",
        source_columns=["order_id", "updated_at"],
        mappings=[
            {"source": "order_id", "target": "order_id"},
            {"source": "updated_at", "target": "updated_at"},
        ],
        operator_key="order_id",
        catalog_key="",
        **kwargs,
    )
    assert error == "", error
    return contracts[0]


def test_named_cursor_rides_as_the_poll_cursor_not_the_snapshot_cursor():
    contract = _contract(
        cursor_column="Updated_At", cursor_semantics="modification_timestamp"
    )
    assert contract["cursor_semantics"] == "cdc_position"  # the log stays the cursor
    assert "cursor_field" not in contract
    assert contract["poll_cursor_field"] == "updated_at"
    assert contract["poll_cursor_semantics"] == "modification_timestamp"
    parsed = SyncContract.from_dict(contract)
    assert parsed.poll_cursor_field == "updated_at"
    assert parsed.poll_cursor_semantics == "modification_timestamp"


def test_no_named_cursor_adds_no_poll_fields():
    contract = _contract()
    assert "poll_cursor_field" not in contract


def test_unknown_poll_column_is_refused_up_front():
    contracts, error = _identity_stream_contract(
        mode="cdc",
        source_table="orders",
        source_columns=["order_id"],
        mappings=[{"source": "order_id", "target": "order_id"}],
        operator_key="order_id",
        catalog_key="",
        cursor_column="nope",
    )
    assert contracts == [] and "no column `nope`" in error


def test_validate_gate_names_the_operators_poll_column(monkeypatch):
    monkeypatch.setattr(
        probe_mod,
        "probe_log_capture",
        lambda *a, **k: probe_mod.LogCaptureProbe(
            "postgresql", False, "wal", CAUSE_SERVER_NOT_CONFIGURED, "wal_level=replica"
        ),
    )
    contract = _contract(cursor_column="updated_at")  # semantics undeclared
    gates = _cdc_log_capture_gates(
        [contract],
        source_type="postgresql",
        source_config={"host": "h"},
        source_table="orders",
        catalog_primary_key_columns=["order_id"],
        mappings=[
            {"source": "order_id", "target": "order_id"},
            {"source": "updated_at", "target": "updated_at"},
        ],
    )
    assert gates and gates[0]["status"] == "block"
    message = gates[0]["message"]
    assert "poll cursor 'updated_at' is not declared as a modification timestamp" in message
    assert "is the primary key" not in message
