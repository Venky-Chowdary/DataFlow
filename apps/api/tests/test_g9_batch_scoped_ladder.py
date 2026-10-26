"""Gate-9 regression pins: batch-key-scoped L3 digests in the verify ladder.

C07's D75 re-read fix (9e690972) revived the PG ladder path and exposed two
asymmetries in ``_maybe_attach_verification_ladder``:

1. When the destination row list is re-scoped to the written batch's keys
   (keyed upsert into an occupied table), the *destination* report digest was
   dropped so L3 recomputes on the scoped population — but the *source* report
   digest (the engine's own checksum scheme, e.g. uint64 decimal) was still
   forwarded. L3 then compared the engine digest against the recomputed
   sha256 aggregate, which can never match, so every keyed upsert into a
   pre-populated destination false-failed as "mismatched columns".

2. The source re-read covers the whole source table; scoping only the
   destination side to batch keys compares the full source population against
   the delta subset, flagging legitimately-skipped rows. The source rows must
   be scoped to the same key set.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.models import EndpointConfig  # noqa: E402
from src.transfer import reconcile_step as rs  # noqa: E402
from services import verification_ladder as vl  # noqa: E402


def _ep(table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="postgresql",
        host="localhost",
        port=5432,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        schema="public",
        table=table,
    )


def _run_ladder(monkeypatch):
    """Drive _maybe_attach_verification_ladder with fake re-reads; return kwargs."""
    dest_table = "dst_t"
    fake_rows = {
        dest_table: [
            {"id": "1", "amount": "1000.00"},
            {"id": "2", "amount": "2000.50"},
            {"id": "9", "amount": "999.00"},  # pre-existing, not in the batch
        ],
        "src_t": [
            {"id": "1", "amount": "1000.00"},
            {"id": "2", "amount": "2000.50"},
            {"id": "5", "amount": "500.00"},  # not picked up this run
        ],
    }
    monkeypatch.setattr(
        vl, "read_postgres_rows", lambda *, table, **kw: list(fake_rows[table])
    )
    captured: dict = {}
    monkeypatch.setattr(
        vl,
        "run_five_layer_verification",
        lambda **kw: captured.update(kw) or {"layers": {}, "passed": True},
    )

    report = {
        "source_checksum": "1233084724085711357",  # engine uint64 digest
        "target_checksum": "999888777",
        "source_rows": 2,
        "target_rows": 3,
        "records_transferred": 2,
    }
    dest_summary = {
        "sync_mode": "upsert",
        "primary_key_columns": ["id"],
        "target_rows_before": 1,
        "keyed_census": {"unique_batch_keys": 2, "dest_preexisting": 1},
    }
    rs._maybe_attach_verification_ladder(
        report,
        endpoint=_ep(dest_table),
        source_endpoint=_ep("src_t"),
        records=[
            {"id": 1, "amount": 1000},
            {"id": 2, "amount": 2000.5},
        ],
        columns=["id", "amount"],
        dest_summary=dest_summary,
        mappings=[
            {"target": "id", "target_type": "INT4"},
            {"target": "amount", "target_type": "DECIMAL"},
        ],
        validation_mode="balanced",
    )
    return captured


def test_batch_scoped_ladder_drops_engine_digests(monkeypatch):
    kw = _run_ladder(monkeypatch)
    assert kw, "ladder did not run"
    # The re-scoped populations must recompute their digests — the engine's
    # own checksum scheme is not comparable to the sha256 aggregate.
    assert kw["source_checksum"] == ""
    assert kw["target_checksum"] == ""


def test_batch_scoped_ladder_scopes_source_to_batch_keys(monkeypatch):
    kw = _run_ladder(monkeypatch)
    assert kw, "ladder did not run"
    batch_ids = {"1", "2"}
    assert {str(r.get("id")) for r in kw["source_rows"]} == batch_ids
    assert {str(r.get("id")) for r in kw["target_rows"]} == batch_ids


def test_unscoped_route_keeps_report_digests(monkeypatch):
    """A keyed run into an *empty* destination keeps the engine digest pair."""
    monkeypatch.setattr(
        vl, "read_postgres_rows", lambda *, table, **kw: [{"id": "1"}]
    )
    captured: dict = {}
    monkeypatch.setattr(
        vl,
        "run_five_layer_verification",
        lambda **kw: captured.update(kw) or {"layers": {}, "passed": True},
    )
    report = {
        "source_checksum": "srcdigest",
        "target_checksum": "tgtdigest",
        "source_rows": 1,
        "target_rows": 1,
        "records_transferred": 1,
    }
    dest_summary = {
        "sync_mode": "upsert",
        "primary_key_columns": ["id"],
        "target_rows_before": 0,
        "keyed_census": {"unique_batch_keys": 1, "dest_preexisting": 0},
    }
    rs._maybe_attach_verification_ladder(
        report,
        endpoint=_ep("dst_t"),
        source_endpoint=_ep("src_t"),
        records=[{"id": 1}],
        columns=["id"],
        dest_summary=dest_summary,
        mappings=[{"target": "id", "target_type": "INT4"}],
        validation_mode="balanced",
    )
    assert captured["source_checksum"] == "srcdigest"
    assert captured["target_checksum"] == "tgtdigest"
