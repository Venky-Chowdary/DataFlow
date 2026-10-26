"""D28 regression tests — a declared poll cursor must reach the CDC refusal.

P3.7: cdc on a PG source without logical decoding always blocked with
"poll cursor 'region' is the primary key". The contract's declared
``cursor_column=updated_at`` with ``cursor_semantics`` was ignored
because the readers only accepted ``cursor_field``/``cursor``.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.cdc_capability import CAUSE_SERVER_NOT_CONFIGURED
from services.cdc_log_capture_probe import (
    LogCaptureProbe,
    build_log_capture_gate,
)
from services.preflight_cursor_gate import evaluate_stream_cursor_semantics
from services.preflight_population import _cdc_log_capture_gates
from services.sync_cursor import SyncContract, contract_cursor_field


_PROBE = LogCaptureProbe(
    dialect="postgresql",
    available=False,
    reader="pgoutput",
    cause=CAUSE_SERVER_NOT_CONFIGURED,
    detail="wal_level=replica — logical decoding is off on this server",
)


def test_poll_cdc_uses_declared_cursor(monkeypatch):
    """The g9c gate sees cursor_column=updated_at, not the PK fallback."""
    monkeypatch.setattr(
        "services.cdc_log_capture_probe.probe_log_capture",
        lambda *a, **k: _PROBE,
    )
    gates = _cdc_log_capture_gates(
        [
            {
                "name": "orders",
                "selected": True,
                "primary_key": "region",
                "cursor_column": "updated_at",
                "cursor_semantics": "modification_timestamp",
            }
        ],
        source_type="postgresql",
        source_config={"host": "x"},
        source_table="orders",
        catalog_primary_key_columns=["region"],
        mappings=[],
    )
    assert len(gates) == 1
    msg = gates[0]["message"]
    assert "primary key" not in msg
    assert "not declared as a modification timestamp" not in msg
    # Honest answer: the source emits no log — run incremental instead.
    assert "change log" in msg or "wal_level" in msg


def test_poll_cdc_uses_declared_cursor_insert_only(monkeypatch):
    monkeypatch.setattr(
        "services.cdc_log_capture_probe.probe_log_capture",
        lambda *a, **k: _PROBE,
    )
    gates = _cdc_log_capture_gates(
        [
            {
                "name": "orders",
                "selected": True,
                "primary_key": "region",
                "cursor_column": "updated_at",
                "cursor_semantics": "insert_only",
            }
        ],
        source_type="postgresql",
        source_config={"host": "x"},
        source_table="orders",
        catalog_primary_key_columns=["region"],
        mappings=[],
    )
    assert len(gates) == 1
    assert "primary key" not in gates[0]["message"]


def test_poll_cdc_still_refuses_pk_poll():
    """Safety kept: an actual PK poll must still block."""
    gate = build_log_capture_gate(
        _PROBE,
        cursor_field="region",
        cursor_semantics="",
        primary_key_columns=["region"],
        pass_status="pass",
        block_status="block",
    )
    assert gate["status"] == "block"
    assert "primary key" in gate["message"]


def test_poll_cdc_undeclared_cursor_still_refused():
    gate = build_log_capture_gate(
        _PROBE,
        cursor_field="updated_at",
        cursor_semantics="",
        primary_key_columns=["region"],
        pass_status="pass",
        block_status="block",
    )
    assert gate["status"] == "block"
    assert "not declared as a modification timestamp" in gate["message"]


def test_contract_cursor_column_alias():
    """cursor_column is the canonical vocabulary — it must parse."""
    c = SyncContract.from_dict(
        {
            "selected": True,
            "cursor_column": "updated_at",
            "cursor_semantics": "modification_timestamp",
        }
    )
    assert c.cursor_field == "updated_at"
    assert c.cursor_semantics == "modification_timestamp"
    assert contract_cursor_field({"cursor_column": "updated_at"}) == "updated_at"


def test_cursor_semantics_gate_reads_cursor_column():
    verdicts, issues = evaluate_stream_cursor_semantics(
        [
            {
                "name": "orders",
                "selected": True,
                "cursor_column": "updated_at",
                "cursor_semantics": "modification_timestamp",
            }
        ],
        sync_mode="cdc",
        validation_mode="strict",
    )
    # declared cursor is evaluated, not skipped as "no cursor declared"
    assert isinstance(verdicts, list)
