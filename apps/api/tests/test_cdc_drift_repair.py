from __future__ import annotations

import sqlite3

import pytest


EVIDENCE = {
    "action": "operator_fenced_repair_required",
    "code": "cdc_drift_requires_fenced_repair",
    "cursor_key": "pg:source:orders",
    "source_table": "orders",
    "dest_table": "orders_copy",
    "pk_fields": ["id"],
    "committed_position": "0/100",
    "drift_count": 1,
    "keys": [{"key_id": "a" * 64, "display": {"id": "••••"}}],
    "truncated": False,
    "semantics": "state convergence / reconciliation, not log replay",
}


def _job(**updates):
    evidence = updates.get("cdc_drift_repair", dict(EVIDENCE))
    return {
        "id": "job-drift-12345678",
        "workspace_id": "workspace-a",
        "status": "failed",
        "transfer_request": {
            "source": {"table": "orders"},
            "destination": {"table": "orders_copy"},
        },
        "extras": updates.get("extras", {"cdc_drift_repair": evidence}),
        "cdc_drift_repair": evidence,
        "cdc_lease_cursor_key": EVIDENCE["cursor_key"],
        **updates,
    }


def test_drift_failure_mapping_persists_safe_operator_evidence():
    from services.cdc_drift_repair import CdcDriftRequiresFencedRepair
    from src.transfer.job_failure import _job_failure_fields

    error = CdcDriftRequiresFencedRepair(dict(EVIDENCE))
    details, extras = _job_failure_fields(error)

    assert details["code"] == EVIDENCE["code"]
    assert details["retriable"] is False
    assert details["evidence"] == EVIDENCE
    assert extras["cdc_drift_repair"] == EVIDENCE
    assert "confirmed repair action" in details["message"]
    assert "resume" in details["message"].lower()


def test_drift_failure_extras_reach_the_memory_job_document(monkeypatch):
    from services.cdc_drift_repair import CdcDriftRequiresFencedRepair
    from services.mongodb_service import MemoryMongoDBService
    from src.transfer import job_failure

    store = MemoryMongoDBService()
    job_id = "job-drift-persist"
    store.create_transfer_job({"_id": job_id, "status": "running"})
    monkeypatch.setattr(job_failure, "_release_failed_cdc_slot", lambda *a, **k: None)
    job_failure._fail_runtime_job(
        store, job_id, CdcDriftRequiresFencedRepair(dict(EVIDENCE))
    )

    saved = store.get_job(job_id)
    assert saved["error_details"]["evidence"] == EVIDENCE
    assert saved["cdc_drift_repair"] == EVIDENCE
    assert saved["cdc_lease_cursor_key"] == EVIDENCE["cursor_key"]
    assert saved["extras"]["cdc_drift_repair"] == EVIDENCE


def test_copilot_stages_only_evidenced_drift_keys(monkeypatch):
    from src.ai.copilot import lifecycle_tools as lt

    staged = []

    class Ledger:
        def put(self, *, kind, payload, preview):
            staged.append((kind, payload, preview))
            return "ack-drift"

    monkeypatch.setattr(
        "src.ai.copilot.ack_ledger.get_ack_ledger", lambda: Ledger()
    )
    monkeypatch.setattr(lt, "resolve_job", lambda job_id="", selector="": (_job(), ""))

    result = lt._job_tool("repair_cdc_drift", "job-drift-12345678", "", key_ids=[])

    assert result.success
    kind, payload, preview = staged[0]
    assert kind == "repair_cdc_drift"
    assert payload == {"job_id": "job-drift-12345678", "key_ids": ["a" * 64]}
    assert preview["table"] == "orders_copy"
    assert preview["key_count"] == 1
    assert preview["committed_position"] == "0/100"
    assert result.output["destructive"] is True
    assert result.output["label"] == (
        "Re-copy 1 drifted row(s) from source (state reconciliation, not log replay)"
    )


@pytest.mark.parametrize(
    ("job", "key_ids"),
    [
        (_job(), ["unknown"]),
        (_job(cdc_drift_repair=None), []),
        (_job(extras={}, cdc_drift_repair=dict(EVIDENCE)), []),
        (_job(cdc_drift_repair={**EVIDENCE, "source_table": "not_orders"}), []),
        (_job(cdc_lease_cursor_key="other-cursor"), []),
    ],
)
def test_copilot_refuses_unproven_drift_evidence(monkeypatch, job, key_ids):
    from src.ai.copilot import lifecycle_tools as lt

    monkeypatch.setattr(lt, "resolve_job", lambda job_id="", selector="": (job, ""))

    result = lt._job_tool(
        "repair_cdc_drift", "job-drift-12345678", "", key_ids=key_ids
    )

    assert not result.success
    assert not result.output


def test_drift_ack_requires_job_manage_at_confirm_time():
    from src.ai.copilot.tool_permissions import (
        ACK_KIND_PERMISSIONS,
        TOOL_PERMISSIONS,
        can_confirm_kind,
    )
    from services.rbac import Permission

    assert TOOL_PERMISSIONS["repair_cdc_drift"] == (Permission.JOB_MANAGE, "mutate")
    assert ACK_KIND_PERMISSIONS["repair_cdc_drift"] == Permission.JOB_MANAGE
    assert can_confirm_kind("viewer", "repair_cdc_drift") is False
    assert can_confirm_kind("operator", "repair_cdc_drift") is True


@pytest.mark.parametrize(
    ("job", "key_ids", "message"),
    [
        (_job(), ["unknown"], "Selected CDC drift keys"),
        (_job(cdc_drift_repair=None), [], "no operator-fenced CDC drift evidence"),
        (
            _job(extras={}, cdc_drift_repair=dict(EVIDENCE)),
            [],
            "no operator-fenced CDC drift evidence",
        ),
        (
            _job(cdc_lease_cursor_key="other-cursor"),
            [],
            "does not match the saved job route",
        ),
        (
            _job(cdc_drift_repair={**EVIDENCE, "dest_table": "wrong_table"}),
            [],
            "does not match the saved job route",
        ),
    ],
)
def test_confirm_revalidates_evidence_and_selected_keys(monkeypatch, job, key_ids, message):
    from services.cdc_drift_repair import run_fenced_drift_repair

    class Store:
        def get_job(self, job_id):
            return job

    monkeypatch.setattr(
        "services.mongodb_service.get_mongodb_service", lambda: Store()
    )
    with pytest.raises(ValueError, match=message):
        run_fenced_drift_repair(
            "job-drift-12345678",
            key_ids,
            actor="operator",
            workspace_id="workspace-a",
        )


def test_confirm_workspace_refusal_maps_to_forbidden(monkeypatch):
    import asyncio
    from fastapi import HTTPException
    from src.routers import copilot_router

    monkeypatch.setattr(copilot_router, "_caller", lambda _request: ("operator", "actor"))
    monkeypatch.setattr(
        "services.cdc_drift_repair.run_fenced_drift_repair",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            PermissionError("outside caller workspace")
        ),
    )
    request = type("Request", (), {"headers": {"x-workspace-id": "workspace-b"}})()
    with pytest.raises(HTTPException) as caught:
        asyncio.run(
            copilot_router._run_lifecycle_confirm(
                "repair_cdc_drift",
                {"job_id": "job-drift-12345678", "key_ids": []},
                request,
                None,
            )
        )
    assert caught.value.status_code == 403


def test_confirm_service_refuses_wrong_workspace(monkeypatch):
    from services.cdc_drift_repair import run_fenced_drift_repair

    class Store:
        def get_job(self, _job_id):
            return _job()

    monkeypatch.setattr(
        "services.mongodb_service.get_mongodb_service", lambda: Store()
    )
    with pytest.raises(PermissionError, match="outside the caller's workspace"):
        run_fenced_drift_repair(
            "job-drift-12345678",
            [],
            actor="operator",
            workspace_id="workspace-b",
        )


def test_pilot_confirm_denies_role_without_job_manage(monkeypatch):
    import asyncio

    from fastapi import HTTPException
    from src.routers import copilot_router

    class Ledger:
        def unusable_reason(self, _ack_id):
            return ""

        def peek(self, _ack_id):
            return {"kind": "repair_cdc_drift"}

    monkeypatch.setattr(
        "src.ai.copilot.ack_ledger.get_ack_ledger", lambda: Ledger()
    )
    monkeypatch.setattr(copilot_router, "_caller", lambda _request: ("viewer", "viewer"))
    request = type("Request", (), {"headers": {}, "state": type("State", (), {})()})()
    with pytest.raises(HTTPException) as caught:
        asyncio.run(
            copilot_router.copilot_confirm(
                copilot_router.ConfirmActionRequest(ack_id="ack-drift"),
                request,
                None,
            )
        )
    assert caught.value.status_code == 403


def test_writer_can_compare_and_set_an_equal_committed_lsn():
    from connectors.writer_common import filter_stale_lsn_rows

    connection = sqlite3.connect(":memory:")
    connection.execute("CREATE TABLE target (id TEXT PRIMARY KEY, _df_lsn TEXT)")
    connection.execute("INSERT INTO target VALUES ('k1', '0/100')")

    rows, skipped = filter_stale_lsn_rows(
        connection.cursor(),
        "target",
        None,
        ["id"],
        [("k1", "0/100")],
        ["id", "_df_lsn"],
        quote='"',
        placeholder="?",
        allow_equal_lsn=True,
    )

    assert rows == [("k1", "0/100")]
    assert skipped == 0
    connection.close()


def test_writer_default_sql_guard_bytes_remain_pinned():
    import hashlib

    from connectors.writer_common import (
        mysql_lsn_values_newer_sql,
        postgres_lsn_update_guard_sql,
        sqlite_lsn_update_guard_sql,
    )

    snapshots = {
        "postgres": hashlib.sha256(
            postgres_lsn_update_guard_sql("orders").encode()
        ).hexdigest(),
        "mysql": hashlib.sha256(mysql_lsn_values_newer_sql().encode()).hexdigest(),
        "sqlite": hashlib.sha256(
            sqlite_lsn_update_guard_sql("orders").encode()
        ).hexdigest(),
    }
    assert snapshots == {
        "postgres": "1166560ec07f08e2f8a1c9571029ad47c644e8713c37dc0871c82b59022aa1f4",
        "mysql": "8e6b5d29054f18d62a97374323d0c34e652bbc9677985b92df4858ec10744176",
        "sqlite": "790ae67ab41e119c3e08928203e1305184524a6a0de90ae93de2dff7c5bd44ff",
    }


def test_repair_refuses_a_live_stream_lease(monkeypatch, tmp_path):
    from types import SimpleNamespace

    from connectors import cdc_eos_sql
    from services.cdc_drift_repair import run_fenced_drift_repair
    from services.cdc_lease import CdcLeaseConflict
    from src.transfer.models import EndpointConfig

    job = _job()

    class Store:
        def get_job(self, _job_id):
            return job

    source = EndpointConfig(
        kind="database",
        format="postgresql",
        database="source",
        table="orders",
    )
    destination = EndpointConfig(
        kind="database",
        format="sqlite",
        database=str(tmp_path / "target.sqlite"),
        table="orders_copy",
    )
    monkeypatch.setattr(
        "services.mongodb_service.get_mongodb_service", lambda: Store()
    )
    monkeypatch.setattr(
        "src.transfer.models.transfer_request_from_dict",
        lambda _saved: SimpleNamespace(source=source, destination=destination),
    )
    monkeypatch.setattr(
        "src.transfer.adapters.resolve_connector_config",
        lambda endpoint, workspace_id=None: {"database": "source"}
        if endpoint is source
        else {"database": destination.database},
    )
    monkeypatch.setattr(
        "src.transfer.cdc_transfer._single_stream_cursor_key",
        lambda **_kwargs: EVIDENCE["cursor_key"],
    )
    monkeypatch.setattr(cdc_eos_sql, "dest_watermark_view", lambda *a, **k: SimpleNamespace(fence_epoch=4))
    monkeypatch.setattr("services.cdc_lease.lease_view", lambda _key: None)
    monkeypatch.setattr("services.cdc_lease.new_holder_id", lambda _job: "repair-holder")
    monkeypatch.setattr(
        "services.cdc_lease.acquire_lease",
        lambda *a, **k: (_ for _ in ()).throw(
            CdcLeaseConflict("resource is held", resource="pg_slot:live")
        ),
    )
    monkeypatch.setattr("services.audit_log.append_audit_event", lambda **_kwargs: None)

    with pytest.raises(CdcLeaseConflict, match="stop the stream first"):
        run_fenced_drift_repair(
            "job-drift-12345678",
            [],
            actor="operator",
            workspace_id="workspace-a",
        )


@pytest.mark.parametrize(
    ("newer_destination", "fail_writer"),
    [(False, False), (True, False), (False, True)],
)
def test_confirmed_repair_reconciles_and_refuses_newer_lsn(
    monkeypatch, newer_destination, fail_writer, tmp_path, caplog
):
    from types import SimpleNamespace

    from connectors import cdc_eos_sql
    from services.cdc_drift_repair import _build_drift_evidence, run_fenced_drift_repair
    from services.cdc_lease import CdcLease
    from services.mongodb_service import MemoryMongoDBService
    from src.transfer.models import EndpointConfig

    source_rows = [
        {"id": "row-1", "value": "source-1"},
        {"id": "row-2", "value": "source-2"},
    ]
    evidence = _build_drift_evidence(
        cursor_key=EVIDENCE["cursor_key"],
        source_table="orders",
        dest_table="orders_copy",
        pk_fields=["id"],
        committed_position="0/100",
        rows=source_rows,
    )
    source = EndpointConfig(
        kind="database",
        format="postgresql",
        database="source",
        schema="public",
        table="orders",
    )
    destination = EndpointConfig(
        kind="database",
        format="sqlite",
        database=str(tmp_path / "target.sqlite"),
        table="orders_copy",
    )
    request_data = {
        "source": {"table": "orders"},
        "destination": {"table": "orders_copy"},
        "stream_contracts": [{"name": "orders", "selected": True}],
        "mappings": [{"source": "id", "target": "id"}],
        "column_types": {"id": "string", "value": "string"},
    }
    job_id = f"job-drift-newer-{newer_destination}"
    store = MemoryMongoDBService()
    store.create_transfer_job(
        {
            "_id": job_id,
            "workspace_id": "workspace-a",
            "status": "failed",
            "transfer_request": request_data,
            "extras": {"cdc_drift_repair": evidence},
            "cdc_drift_repair": evidence,
        }
    )
    monkeypatch.setattr(
        "services.mongodb_service.get_mongodb_service", lambda: store
    )
    monkeypatch.setattr(
        "src.transfer.models.transfer_request_from_dict",
        lambda _saved: SimpleNamespace(source=source, destination=destination),
    )
    monkeypatch.setattr(
        "src.transfer.adapters.resolve_connector_config",
        lambda endpoint, workspace_id=None: {
            "type": "postgresql" if endpoint is source else "sqlite",
            "database": "source" if endpoint is source else destination.database,
            "host": "",
            "port": 0,
            "username": "",
            "password": "",
            "schema": "public",
        },
    )
    monkeypatch.setattr(
        "src.transfer.cdc_transfer._single_stream_cursor_key",
        lambda **_kwargs: EVIDENCE["cursor_key"],
    )
    monkeypatch.setattr(
        cdc_eos_sql,
        "dest_watermark_view",
        lambda *a, **k: SimpleNamespace(fence_epoch=8),
    )
    monkeypatch.setattr(
        cdc_eos_sql,
        "open_eos_session",
        lambda **_kwargs: SimpleNamespace(fence_epoch=9),
    )
    monkeypatch.setattr(
        "services.cdc_lease.lease_view", lambda _key: None
    )
    monkeypatch.setattr(
        "services.cdc_lease.acquire_lease",
        lambda *args, **kwargs: CdcLease(
            cursor_key=args[0],
            resource=kwargs["resource"],
            holder_id=kwargs["holder_id"],
            generation=9,
        ),
    )
    monkeypatch.setattr("services.cdc_lease.new_holder_id", lambda _job: "repair-holder")
    monkeypatch.setattr("services.cdc_lease.release_lease", lambda *a, **k: None)
    monkeypatch.setattr("services.audit_log.append_audit_event", lambda **_kwargs: None)

    destination_state = {
        "row-1": {"value": "damaged-1", "_df_lsn": "0/100"},
        "row-2": {
            "value": "damaged-2",
            "_df_lsn": "0/200" if newer_destination else "0/100",
        },
    }

    def drift_scan(**_kwargs):
        return [
            row
            for row in source_rows
            if destination_state[row["id"]]["value"] != row["value"]
        ]

    def fake_write(endpoint, records, *_args, **_kwargs):
        row = records[0]
        if fail_writer and row["id"] == "row-1":
            raise RuntimeError("row-value-sentinel-keep-out")
        current = destination_state[row["id"]]
        if current["_df_lsn"] == "0/200":
            return 0, [], {"rows_skipped": 1}
        destination_state[row["id"]] = {
            "value": row["value"],
            "_df_lsn": row["_df_lsn"],
        }
        return 1, [], {"rows_skipped": 0}

    import services.cdc_value_digest as value_digest
    import src.transfer.adapters as adapters

    monkeypatch.setattr(value_digest, "identity_rows_missing_on_dest", drift_scan)
    monkeypatch.setattr(
        value_digest, "_scan_table", lambda *a, **k: [{"id": "row-1"}, {"id": "row-2"}]
    )
    monkeypatch.setattr(adapters, "write_destination_database", fake_write)

    summary = run_fenced_drift_repair(
        job_id,
        [item["key_id"] for item in evidence["keys"]],
        actor="operator",
        workspace_id="workspace-a",
    )
    assert destination_state["row-1"] == (
        {"value": "damaged-1", "_df_lsn": "0/100"}
        if fail_writer
        else {"value": "source-1", "_df_lsn": "0/100"}
    )
    assert summary["outcomes"]["repaired"] == (
        1 if newer_destination or fail_writer else 2
    )
    assert summary["outcomes"]["refused_newer_event"] == int(newer_destination)
    assert summary["outcomes"]["failed"] == int(fail_writer)
    assert summary["verified"] == (1 if newer_destination or fail_writer else 2)
    assert summary["resolved"] is not (newer_destination or fail_writer)
    if fail_writer:
        failed_key = evidence["keys"][0]["key_id"]
        warning_messages = [
            record.getMessage()
            for record in caplog.records
            if record.name == "services.cdc_drift_repair"
            and record.levelname == "WARNING"
        ]
        assert any(
            EVIDENCE["cursor_key"] in message
            and failed_key in message
            and "RuntimeError" in message
            for message in warning_messages
        )
        assert "row-value-sentinel-keep-out" not in caplog.text


def _seed_image_repair_scenario(tmp_path, monkeypatch):
    import os
    from unittest.mock import patch

    from services.cdc_engine import ChangeBatch
    from src.transfer.cdc_transfer import (
        _single_stream_cursor_key,
        run_cdc_database_transfer,
    )
    from src.transfer.models import EndpointConfig

    source = EndpointConfig(
        kind="database",
        format="postgresql",
        database="drift_source",
        table="orders",
        schema="public",
    )
    dest_path = str(tmp_path / "drift.sqlite")
    destination = EndpointConfig(
        kind="database",
        format="sqlite",
        database=dest_path,
        table="orders_copy",
    )
    source_cfg = {"database": source.database}
    dest_cfg = {"database": dest_path}
    cursor_key = _single_stream_cursor_key(
        src_type="postgresql",
        src_cfg=source_cfg,
        table_name="orders",
        dest_type="sqlite",
        dest_cfg=dest_cfg,
        dest_table="orders_copy",
        stream_name="orders",
    )
    state = {"repair": False}

    class FakeCdc:
        def __init__(self, *args, **kwargs):
            self.sent_position = False

        def is_available(self):
            return True

        def snapshot(self):
            return iter(())

        def poll(self):
            if not state["repair"] and not self.sent_position:
                self.sent_position = True
                yield ChangeBatch(
                    inserts=[{"id": "pk-sentinel-77", "v": "payload-sentinel-88"}],
                    resume_token="slot=drift|phase=streaming|lsn=0/100",
                    table="orders",
                )

        def ack(self, token=None):
            self.acked = token

        def close(self):
            pass

    import services.cdc_value_digest as value_digest
    import services.sync_cursor as sync_cursor
    import src.transfer.cdc_transfer as transfer

    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.setenv("DATAFLOW_CDC_MAX_IDLE_POLLS", "1")
    monkeypatch.setenv("DATAFLOW_CDC_MAX_POLL_ROUNDS", "2")
    monkeypatch.setenv("DATAFLOW_CDC_TXN_HOLD_SLEEP_SEC", "0")
    monkeypatch.setattr(sync_cursor, "STORE_PATH", tmp_path / "sync.json")
    monkeypatch.setattr(sync_cursor, "_mongo_cursors", lambda: None)
    monkeypatch.setattr(sync_cursor, "_stamp_job_watermark", lambda *a, **k: None)
    monkeypatch.setattr(transfer, "PostgreSqlChangeStreamCdc", FakeCdc)
    monkeypatch.setattr(
        value_digest,
        "identity_rows_missing_on_dest",
        lambda **kwargs: (
            [{"id": "pk-sentinel-77", "v": "payload-sentinel-88"}]
            if state["repair"]
            else []
        ),
    )
    monkeypatch.setattr(value_digest, "rows_absent_by_primary_key", lambda **kwargs: [])

    with patch.dict(
        os.environ,
        {
            "DATAFLOW_JOB_STORE": "memory",
            "DATAFLOW_DISABLE_OBJECT_STORE": "1",
            "DATAFLOW_CDC_MAX_IDLE_POLLS": "1",
            "DATAFLOW_CDC_MAX_POLL_ROUNDS": "2",
            "DATAFLOW_CDC_TXN_HOLD_SLEEP_SEC": "0",
        },
    ):
        run_cdc_database_transfer(
            source,
            destination,
            mappings=[
                {"source": "id", "target": "id", "confidence": 1.0},
                {"source": "v", "target": "v", "confidence": 1.0},
            ],
            schema={"id": "string", "v": "string"},
            sync_mode="cdc",
            stream_contracts=[
                {
                    "name": "orders",
                    "selected": True,
                    "primary_key": "id",
                    "cursor_field": "id",
                    "sync_mode": "cdc",
                }
            ],
            delivery_guarantee="exactly_once",
            delivery_pinned=True,
        )
    import sqlite3

    with sqlite3.connect(dest_path) as connection:
        connection.execute(
            "UPDATE orders_copy SET v = ? WHERE id = ?",
            ("damaged-sentinel-99", "pk-sentinel-77"),
        )
    state["repair"] = True
    return source, destination, dest_cfg, cursor_key, state


def test_exactly_once_route_refuses_drift_without_moving_eos_state(
    tmp_path, monkeypatch, caplog
):
    from connectors.cdc_eos_sql import dest_watermark_view
    from services.sync_cursor import get_watermark
    from services.cdc_drift_repair import CdcDriftRequiresFencedRepair
    from src.transfer.cdc_transfer import run_cdc_database_transfer

    source, destination, dest_cfg, cursor_key, _state = _seed_image_repair_scenario(
        tmp_path, monkeypatch
    )
    mappings = [
        {"source": "id", "target": "id", "confidence": 1.0},
        {"source": "v", "target": "v", "confidence": 1.0},
    ]
    contracts = [
        {
            "name": "orders",
            "selected": True,
            "primary_key": "id",
            "cursor_field": "id",
            "sync_mode": "cdc",
        }
    ]
    before_eos = dest_watermark_view(dest_cfg, cursor_key)
    before_watermark = get_watermark(cursor_key)
    import src.transfer.cdc_transfer as transfer

    monkeypatch.setattr(
        transfer,
        "_apply_change_batch",
        lambda *_args, **_kwargs: pytest.fail(
            "exactly-once drift refusal must not apply the missing image"
        ),
    )
    errors = []
    for _ in range(2):
        with pytest.raises(CdcDriftRequiresFencedRepair) as caught:
            run_cdc_database_transfer(
                source,
                destination,
                mappings=mappings,
                schema={"id": "string", "v": "string"},
                sync_mode="cdc",
                stream_contracts=contracts,
                delivery_guarantee="exactly_once",
                delivery_pinned=True,
            )
        errors.append(caught.value)
    after_eos = dest_watermark_view(dest_cfg, cursor_key)
    assert errors[0].evidence["keys"][0]["key_id"] == errors[1].evidence["keys"][0]["key_id"]
    assert after_eos == before_eos
    assert get_watermark(cursor_key) == before_watermark
    assert errors[0].evidence["semantics"] == "state convergence / reconciliation, not log replay"
    rendered = repr(errors[0].evidence) + caplog.text
    assert "pk-sentinel-77" not in rendered
    assert "payload-sentinel-88" not in rendered
    assert "damaged-sentinel-99" not in rendered

    from types import SimpleNamespace

    from services.cdc_lease import CdcLease
    from services.mongodb_service import MemoryMongoDBService
    from src.transfer.models import EndpointConfig

    source_image = {"id": "pk-sentinel-77", "v": "payload-sentinel-88"}
    source_endpoint = EndpointConfig(
        kind="database",
        format="postgresql",
        database="drift_source",
        schema="public",
        table="orders",
    )
    destination_endpoint = EndpointConfig(
        kind="database",
        format="sqlite",
        database=destination.database,
        table="orders_copy",
    )
    job_id = "job-drift-approved-repair"
    request_data = {
        "source": {
            "format": "postgresql",
            "database": "drift_source",
            "schema": "public",
            "table": "orders",
        },
        "destination": {
            "format": "sqlite",
            "database": destination.database,
            "table": "orders_copy",
        },
        "mappings": [
            {"source": "id", "target": "id"},
            {"source": "v", "target": "v"},
        ],
        "column_types": {"id": "string", "v": "string"},
        "stream_contracts": [
            {"name": "orders", "selected": True, "primary_key": "id"}
        ],
    }
    store = MemoryMongoDBService()
    store.create_transfer_job(
        {
            "_id": job_id,
            "workspace_id": "workspace-a",
            "status": "failed",
            "transfer_request": request_data,
            "extras": {"cdc_drift_repair": errors[0].evidence},
            "cdc_drift_repair": errors[0].evidence,
            "cdc_lease_cursor_key": cursor_key,
        }
    )
    monkeypatch.setattr("services.mongodb_service.get_mongodb_service", lambda: store)
    monkeypatch.setattr(
        "src.transfer.models.transfer_request_from_dict",
        lambda _saved: SimpleNamespace(
            source=source_endpoint, destination=destination_endpoint
        ),
    )
    monkeypatch.setattr(
        "src.transfer.adapters.resolve_connector_config",
        lambda endpoint, workspace_id=None: (
            {
                "type": "postgresql",
                "database": "drift_source",
                "host": "",
                "port": 5432,
                "username": "",
                "password": "",
                "schema": "public",
            }
            if endpoint.format == "postgresql"
            else {
                "type": "sqlite",
                "database": destination.database,
                "host": "",
                "port": 0,
                "username": "",
                "password": "",
                "schema": "",
            }
        ),
    )
    monkeypatch.setattr(
        "src.transfer.cdc_transfer._single_stream_cursor_key",
        lambda **_kwargs: cursor_key,
    )

    def current_drift_scan(**_kwargs):
        with sqlite3.connect(destination.database) as connection:
            current = connection.execute(
                "SELECT id, v FROM orders_copy WHERE id = ?",
                ("pk-sentinel-77",),
            ).fetchone()
        return [] if current == tuple(source_image.values()) else [source_image]

    import services.cdc_value_digest as value_digest
    import services.cdc_lease as cdc_lease
    import services.audit_log as audit_log
    from connectors import cdc_eos_sql

    monkeypatch.setattr(value_digest, "identity_rows_missing_on_dest", current_drift_scan)
    monkeypatch.setattr(
        value_digest, "_scan_table", lambda *args, **kwargs: [{"id": "pk-sentinel-77"}]
    )
    lease_releases = []
    monkeypatch.setattr(cdc_lease, "lease_view", lambda _key: None)
    monkeypatch.setattr(
        cdc_lease,
        "acquire_lease",
        lambda *args, **kwargs: CdcLease(
            cursor_key=args[0],
            resource=kwargs["resource"],
            holder_id=kwargs["holder_id"],
            generation=int(kwargs["min_generation"]) + 1,
        ),
    )
    monkeypatch.setattr(cdc_lease, "new_holder_id", lambda _job_id: "repair-holder")
    monkeypatch.setattr(
        cdc_lease,
        "release_lease",
        lambda *args, **kwargs: lease_releases.append((args, kwargs)),
    )
    audit_events = []
    monkeypatch.setattr(
        audit_log, "append_audit_event", lambda **kwargs: audit_events.append(kwargs)
    )

    from services.cdc_drift_repair import run_fenced_drift_repair

    before_repair_eos = dest_watermark_view(dest_cfg, cursor_key)
    summary = run_fenced_drift_repair(
        job_id,
        [item["key_id"] for item in errors[0].evidence["keys"]],
        actor="operator@example.test",
        workspace_id="workspace-a",
    )
    assert summary["verified"] == 1 and summary["resolved"] is True
    assert summary["semantics"] == "state convergence / reconciliation, not log replay"
    after_repair_eos = dest_watermark_view(dest_cfg, cursor_key)
    assert after_repair_eos.committed_lsn == before_repair_eos.committed_lsn
    assert after_repair_eos.apply_checksum == before_repair_eos.apply_checksum
    assert after_repair_eos.apply_seq == before_repair_eos.apply_seq
    assert after_repair_eos.fence_epoch > before_repair_eos.fence_epoch
    saved = store.get_job(job_id)
    assert saved["status"] == "failed"
    assert saved["extras"]["cdc_drift_repair"]["resolved"] is True
    assert saved["extras"]["cdc_drift_repair"]["last_repair"] == summary
    assert [event["action"] for event in audit_events] == [
        "cdc_drift_repair_started",
        "cdc_drift_repair_completed",
    ]
    assert lease_releases
    with sqlite3.connect(destination.database) as connection:
        repaired_row = connection.execute(
            "SELECT v, _df_lsn FROM orders_copy WHERE id = ?",
            ("pk-sentinel-77",),
        ).fetchone()
    assert repaired_row == ("payload-sentinel-88", "0/100")
    resumed = run_cdc_database_transfer(
        source,
        destination,
        mappings=mappings,
        schema={"id": "string", "v": "string"},
        sync_mode="cdc",
        stream_contracts=contracts,
        delivery_guarantee="exactly_once",
        delivery_pinned=True,
    )
    assert resumed[2]["cdc_delivery"] == "exactly_once"


def test_at_least_once_image_repair_reports_writer_skip_truthfully(tmp_path, monkeypatch):
    import sqlite3

    from src.transfer.cdc_transfer import run_cdc_database_transfer

    source, destination, _dest_cfg, _cursor_key, _state = _seed_image_repair_scenario(
        tmp_path, monkeypatch
    )
    result = run_cdc_database_transfer(
        source,
        destination,
        mappings=[
            {"source": "id", "target": "id", "confidence": 1.0},
            {"source": "v", "target": "v", "confidence": 1.0},
        ],
        schema={"id": "string", "v": "string"},
        sync_mode="cdc",
        stream_contracts=[
            {
                "name": "orders",
                "selected": True,
                "primary_key": "id",
                "cursor_field": "id",
                "sync_mode": "cdc",
            }
        ],
        delivery_guarantee="at_least_once",
        delivery_pinned=True,
    )
    image_repair = result[2]["cdc"]["image_repair"]
    assert image_repair == {
        "attempted": 1,
        "written": 0,
        "skipped": 1,
        "status": "repair_not_applied",
    }
    assert any("not applied" in line and "upserted" not in line for line in result[1])
    assert "state convergence / reconciliation, not log replay" in " ".join(result[1])
    rendered_ddl = " ".join(result[1])
    assert "pk-sentinel-77" not in rendered_ddl
    assert "payload-sentinel-88" not in rendered_ddl
    assert "damaged-sentinel-99" not in rendered_ddl
    with sqlite3.connect(destination.database) as connection:
        row = connection.execute(
            "SELECT v FROM orders_copy WHERE id = ?", ("pk-sentinel-77",)
        ).fetchone()
    assert row == ("damaged-sentinel-99",)
