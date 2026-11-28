"""D437: the recovery a CDC drift failure recommends must run on that destination.

QA round 73: PostgreSQL CDC into SQL Server failed on two drifted rows with
"Run repair_cdc_drift"; the preview was issued (destructive, with an ack) and
the confirm was then refused "Inclusive _df_lsn compare-and-set is not
supported for sqlserver". The rows stayed wrong and the schedule stayed red.

One rule (``drift_repair_route``) now decides for the failure message, the
preview and the confirmed repair. SQL Server, Oracle and generic SQL
destinations repair inside the dest-owned watermark transaction (the live
legs are ``test_cdc_exactly_once_live_engines.py::
test_live_d437_drift_repair_in_the_watermark_transaction``, run by the
cdc-sqlserver and oracle-live jobs); a destination with no guarded repair is
refused at preview with the reason, and its failure message names a recovery
that runs there instead of repair_cdc_drift.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

CURSOR = "pg:source:orders"


def _evidence(dest_type: str, rows=None) -> dict:
    from services.cdc_drift_repair import _build_drift_evidence

    return _build_drift_evidence(
        cursor_key=CURSOR,
        source_table="orders",
        dest_table="orders_copy",
        pk_fields=["id"],
        committed_position={"lsn": "0/100"},
        rows=rows or [{"id": "4"}, {"id": "33"}],
        dest_type=dest_type,
    )


@pytest.mark.parametrize("dest", ["sqlserver", "azure_sql", "oracle", "generic_sql", "postgresql", "mysql", "sqlite"])
def test_every_destination_that_can_hold_cdc_drift_has_a_repair(dest):
    from services.cdc_drift_repair import (
        cdc_drift_operator_message,
        drift_repair_refusal,
        drift_repair_route,
    )

    assert drift_repair_route(dest)
    assert drift_repair_refusal(dest) == ""
    assert "repair_cdc_drift" in cdc_drift_operator_message(_evidence(dest))


@pytest.mark.parametrize("dest", ["snowflake", "duckdb", "redshift", "bigquery", "s3"])
def test_an_unguarded_destination_is_never_told_to_run_repair_cdc_drift(dest):
    from services.cdc_drift_repair import cdc_drift_operator_message, drift_repair_refusal

    message = cdc_drift_operator_message(_evidence(dest))
    assert "Run repair_cdc_drift" not in message
    assert "clear_cursor_resnapshot" in message
    assert "2 rows in orders_copy" in message and "evidenced by id" in message
    pending = cdc_drift_operator_message(
        {**_evidence(dest), "capture_pending": True, "capture_pending_reason": "capture_agent_lagging"}
    )
    assert "run repair_cdc_drift" not in pending and "clear_cursor_resnapshot" in pending
    assert "cannot run on" in drift_repair_refusal(dest)


def test_evidence_from_before_d437_keeps_its_message():
    from services.cdc_drift_repair import cdc_drift_operator_message

    legacy = _evidence("")
    assert "dest_type" not in legacy
    assert "Run repair_cdc_drift" in cdc_drift_operator_message(legacy)


def _job(dest_format: str) -> dict:
    evidence = _evidence(dest_format)
    return {
        "id": "job-d437-12345678",
        "workspace_id": "workspace-a",
        "status": "failed",
        "transfer_request": {
            "source": {"table": "orders", "format": "postgresql"},
            "destination": {"table": "orders_copy", "format": dest_format},
        },
        "extras": {"cdc_drift_repair": evidence},
        "cdc_drift_repair": evidence,
        "cdc_lease_cursor_key": CURSOR,
    }


@pytest.mark.parametrize(("dest", "staged_ok"), [("snowflake", False), ("sqlserver", True)])
def test_the_preview_refuses_where_the_confirm_would(monkeypatch, dest, staged_ok):
    from src.ai.copilot import lifecycle_tools as lt

    staged = []

    class Ledger:
        def put(self, *, kind, payload, preview):
            staged.append(kind)
            return "ack-d437"

    monkeypatch.setattr("src.ai.copilot.ack_ledger.get_ack_ledger", lambda: Ledger())
    monkeypatch.setattr(lt, "resolve_job", lambda job_id="", selector="": (_job(dest), ""))

    result = lt._job_tool("repair_cdc_drift", "job-d437-12345678", "", key_ids=[])

    assert result.success is staged_ok
    assert bool(staged) is staged_ok  # no confirm card and no ack for a refused route
    if not staged_ok:
        assert "cannot run on Snowflake" in str(result.error)
        assert "Request refused" not in str(result.error)


# --- the confirmed repair, end to end, on a watermark-transaction destination ---


def _sqlite(path: str, sql: str, params=()):
    conn = sqlite3.connect(path)
    try:
        rows = conn.execute(sql, params).fetchall()
        conn.commit()
        return rows
    finally:
        conn.close()


def test_confirmed_repair_on_a_generic_sql_destination(monkeypatch, tmp_path: Path):
    """Seeded through the real exactly-once apply; repaired through the real
    run_fenced_drift_repair (only the source scans, lease store and audit are
    stubbed): drifted keys converge, a newer change is refused, the
    watermark stays where it was."""
    from connectors.cdc_eos_sql import apply_change_batch_exactly_once, dest_watermark_view
    from services.cdc_drift_repair import run_fenced_drift_repair
    from services.cdc_engine import ChangeBatch
    from services.cdc_lease import CdcLease
    from services.mongodb_service import MemoryMongoDBService
    from src.transfer.models import EndpointConfig

    dest_cfg = {"type": "sqlite", "database": str(tmp_path / "dest.db")}
    mappings = [
        {"source": "id", "target": "id", "confidence": 1.0},
        {"source": "v", "target": "v", "confidence": 1.0},
    ]
    types = {"id": "string", "v": "string"}
    source_rows = [{"id": "1", "v": "s1"}, {"id": "2", "v": "s2"}, {"id": "3", "v": "s3"}]
    apply_change_batch_exactly_once(
        dest_type="generic_sql", dest_cfg=dest_cfg, dest_table="orders_copy",
        change=ChangeBatch(inserts=[dict(r) for r in source_rows], resume_token={"lsn": "0/100"}),
        mappings=mappings, column_types=types, headers=["id", "v"], pk_target_cols=["id"],
        cursor_key=CURSOR,
    )
    db = dest_cfg["database"]
    _sqlite(db, "UPDATE orders_copy SET v = 'damaged' WHERE id IN ('1', '2')")
    _sqlite(db, "UPDATE orders_copy SET v = 'newer', _df_lsn = '0/200' WHERE id = '3'")

    def dest_values() -> dict[str, tuple]:
        return {r[0]: (r[1], r[2]) for r in _sqlite(db, "SELECT id, v, _df_lsn FROM orders_copy")}

    def drift_scan(**_kw):
        held = dest_values()
        return [r for r in source_rows if held.get(r["id"], (None,))[0] != r["v"]]

    evidence = _evidence("generic_sql", rows=drift_scan())
    assert evidence["drift_count"] == 3
    source = EndpointConfig(kind="database", format="postgresql", database="source", table="orders")
    destination = EndpointConfig(kind="database", format="generic_sql", table="orders_copy")
    store = MemoryMongoDBService()
    store.create_transfer_job({
        "_id": "job-d437", "workspace_id": "workspace-a", "status": "failed",
        "transfer_request": {
            "source": {"table": "orders"}, "destination": {"table": "orders_copy"},
            "stream_contracts": [{"name": "orders", "selected": True}],
            "mappings": mappings, "column_types": types,
        },
        "extras": {"cdc_drift_repair": evidence}, "cdc_drift_repair": evidence,
    })
    import services.cdc_value_digest as value_digest

    monkeypatch.setattr("services.mongodb_service.get_mongodb_service", lambda: store)
    monkeypatch.setattr(
        "src.transfer.models.transfer_request_from_dict",
        lambda _saved: SimpleNamespace(source=source, destination=destination),
    )
    monkeypatch.setattr(
        "src.transfer.adapters.resolve_connector_config",
        lambda endpoint, workspace_id=None: (
            {"type": "postgresql", "database": "source"} if endpoint is source else dict(dest_cfg)
        ),
    )
    monkeypatch.setattr("src.transfer.cdc_transfer._single_stream_cursor_key", lambda **_kw: CURSOR)
    monkeypatch.setattr("services.cdc_lease.lease_view", lambda _key: None)
    monkeypatch.setattr(
        "services.cdc_lease.acquire_lease",
        lambda *a, **kw: CdcLease(cursor_key=a[0], resource=kw["resource"], holder_id=kw["holder_id"], generation=9),
    )
    monkeypatch.setattr("services.cdc_lease.new_holder_id", lambda _job: "repair-holder")
    monkeypatch.setattr("services.cdc_lease.release_lease", lambda *a, **k: None)
    monkeypatch.setattr("services.audit_log.append_audit_event", lambda **_kw: None)
    monkeypatch.setattr(value_digest, "identity_rows_missing_on_dest", drift_scan)
    monkeypatch.setattr(value_digest, "_scan_table", lambda *a, **k: [{"id": r["id"]} for r in source_rows])

    summary = run_fenced_drift_repair(
        "job-d437", [k["key_id"] for k in evidence["keys"]], actor="operator", workspace_id="workspace-a"
    )

    assert summary["outcomes"]["repaired"] == 2
    assert summary["outcomes"]["refused_newer_event"] == 1
    assert summary["outcomes"]["failed"] == 0
    assert dest_values() == {"1": ("s1", "0/100"), "2": ("s2", "0/100"), "3": ("newer", "0/200")}
    view = dest_watermark_view(dest_cfg, CURSOR)
    assert view.committed_lsn == "0/100"  # state convergence, not replay
    assert view.fence_epoch == 9  # the repair's lease generation fenced the stream

    # Once the newer change is gone, the same repair converges the last key.
    _sqlite(db, "UPDATE orders_copy SET _df_lsn = '0/100' WHERE id = '3'")
    again = run_fenced_drift_repair(
        "job-d437", [evidence["keys"][2]["key_id"]], actor="operator", workspace_id="workspace-a"
    )
    assert again["outcomes"]["repaired"] == 1 and again["resolved"] is True
    assert dest_values()["3"] == ("s3", "0/100")
