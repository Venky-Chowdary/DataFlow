"""D48 — a ``business_date`` cursor was advertised but could never be declared.

The tool schemas and the cursor-semantics gate both say a business_date cursor
is accepted with a ``cursor_lookback_days`` re-read window, but no Pilot tool
took that window, so the contract never carried it and the gate always
refused the date cursor. Pilot ``start_transfer`` / ``create_schedule`` now
accept it and stamp it on the stream contract.
"""

from __future__ import annotations

import inspect

import pytest

from src.ai.copilot import schedule_tools, tools as copilot_tools, transfer_tools
from src.ai.copilot.tools import _tool_schema_properties
from services.cursor_semantics import BUSINESS_DATE


@pytest.mark.parametrize(
    "name", ["plan_transfer", "start_transfer", "create_schedule"]
)
def test_tool_schema_offers_the_lookback_window(name):
    assert "cursor_lookback_days" in _tool_schema_properties(name)


def test_handlers_take_the_window():
    for fn in (
        transfer_tools.plan_transfer,
        transfer_tools.start_transfer,
        schedule_tools.create_schedule,
        copilot_tools.DataPilotTools._plan_transfer,
        copilot_tools.DataPilotTools._start_transfer,
        copilot_tools.DataPilotTools._create_schedule,
    ):
        assert "cursor_lookback_days" in inspect.signature(fn).parameters


def test_contract_carries_the_window_and_the_gate_accepts_it():
    contracts, err = transfer_tools._identity_stream_contract(
        mode="incremental_deduped",
        source_table="orders",
        source_columns=["id", "order_date"],
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "order_date", "target": "order_date"},
        ],
        operator_key="id",
        catalog_key="",
        cursor_column="order_date",
        cursor_semantics=BUSINESS_DATE,
        cursor_lookback_days=3,
    )
    assert not err, err
    contract = contracts[0]
    assert contract["cursor_semantics"] == BUSINESS_DATE
    assert contract["cursor_lookback_days"] == 3

    from services.cursor_semantics import evaluate_cursor_semantics
    from services.sync_cursor import SyncContract

    parsed = SyncContract.from_dict(contract)
    assert parsed.cursor_lookback_days == 3
    verdict = evaluate_cursor_semantics(
        declared=BUSINESS_DATE,
        cursor_field="order_date",
        sync_mode="incremental_deduped",
        lookback_days=3,
    )
    assert verdict.status == "ok"
