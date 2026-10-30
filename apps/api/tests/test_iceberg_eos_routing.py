from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from services.cdc_effectively_once import classify_sink_delivery, gate_cdc_destination
from services.cdc_exactly_once import (
    EOS_TRANSACTIONAL_DESTS,
    EOS_TXN_WIRED_DESTS,
    ExactlyOnceRouteError,
    PLATFORM_EXACTLY_ONCE_CLAIMED,
    REASON_DEST_NOT_WIRED,
    preflight_delivery_gate,
    select_route_delivery,
)


ICEBERG_REFUSAL = (
    "Iceberg exactly-once requires a catalog-backed destination "
    "(REST/Glue/SQL/Hive/Nessie); filesystem/warehouse-path Iceberg remains "
    "at-least-once."
)


def _catalog_config(tmp_path: Path) -> dict[str, Any]:
    return {
        "type": "iceberg",
        "table": "events",
        "schema": "default",
        "connection_string": f"sqlite:///{(tmp_path / 'catalog.db').as_posix()}",
        "warehouse": str(tmp_path / "warehouse"),
        "extra": {"catalog_type": "sql"},
    }


def _filesystem_config(tmp_path: Path) -> dict[str, Any]:
    return {
        "type": "iceberg",
        "table": "events",
        "schema": "default",
        "warehouse": str(tmp_path / "warehouse"),
        "extra": {"catalog_type": "filesystem"},
    }


def _route_kwargs(dest_cfg: dict[str, Any] | None) -> dict[str, Any]:
    return {
        "sync_mode": "cdc",
        "dest_type": "iceberg",
        "source_type": "postgresql",
        "has_primary_key": True,
        "has_lsn_column": True,
        "dest_cfg": dest_cfg,
    }


def test_catalog_iceberg_auto_route_and_preflight_select_exactly_once(
    tmp_path: Path,
) -> None:
    config = _catalog_config(tmp_path)

    selected = select_route_delivery("auto", **_route_kwargs(config))
    gate = preflight_delivery_gate(
        sync_mode="cdc",
        dest_type="iceberg",
        delivery_guarantee="auto",
        has_primary_key=True,
        has_lsn_column=True,
        source_type="postgresql",
        dest_cfg=config,
    )

    assert selected == "exactly_once"
    assert gate is not None
    assert gate["details"]["delivery_guarantee"] == selected


@pytest.mark.parametrize(
    "dest_type", ["iceberg", "apache_iceberg", "iceberg_rest", "nessie"]
)
def test_catalog_iceberg_aliases_select_exactly_once(
    tmp_path: Path, dest_type: str
) -> None:
    kwargs = _route_kwargs(_catalog_config(tmp_path))
    kwargs["dest_type"] = dest_type

    assert select_route_delivery("auto", **kwargs) == "exactly_once"


def test_filesystem_and_missing_config_remain_at_least_once(tmp_path: Path) -> None:
    assert (
        select_route_delivery("auto", **_route_kwargs(_filesystem_config(tmp_path)))
        == "at_least_once"
    )
    assert select_route_delivery("auto", **_route_kwargs(None)) == "at_least_once"


def test_explicit_exactly_once_pin_on_filesystem_has_readable_refusal(
    tmp_path: Path,
) -> None:
    with pytest.raises(ExactlyOnceRouteError) as raised:
        select_route_delivery(
            "exactly_once", **_route_kwargs(_filesystem_config(tmp_path))
        )

    assert raised.value.reason == REASON_DEST_NOT_WIRED
    assert ICEBERG_REFUSAL in str(raised.value)


def test_catalog_readiness_parses_config_without_loading_catalog_or_network(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from connectors import iceberg_eos

    def unexpected(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("readiness must not load a catalog or open a connection")

    monkeypatch.setattr(iceberg_eos, "load_catalog", unexpected)
    monkeypatch.setattr("socket.create_connection", unexpected)

    assert iceberg_eos.iceberg_eos_catalog_ready(_catalog_config(tmp_path))
    assert not iceberg_eos.iceberg_eos_catalog_ready(
        _filesystem_config(tmp_path)
    )
    assert not iceberg_eos.iceberg_eos_catalog_ready(None)
    assert not iceberg_eos.iceberg_eos_catalog_ready({"type": "iceberg"})


def test_require_exactly_once_gate_refuses_before_writer_or_apply(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from connectors import cdc_eos_sql

    calls: list[tuple[Any, ...]] = []
    monkeypatch.setattr(
        cdc_eos_sql,
        "apply_change_batch_exactly_once",
        lambda *args, **kwargs: calls.append((args, kwargs)),
    )

    with pytest.raises(ExactlyOnceRouteError) as raised:
        gate_cdc_destination(
            dest_type="iceberg",
            has_primary_key=True,
            has_lsn_column=True,
            require_exactly_once=True,
            dest_cfg=_filesystem_config(tmp_path),
        )

    assert ICEBERG_REFUSAL in str(raised.value)
    assert calls == []


def test_cdc_transfer_required_filesystem_iceberg_fails_before_write(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from connectors import cdc_eos_sql, iceberg_writer
    from src.transfer import cdc_transfer, stream_dest_procedure
    from src.transfer.models import EndpointConfig

    dest_config = {
        **_filesystem_config(tmp_path),
        "require_exactly_once": True,
    }
    monkeypatch.setattr(
        cdc_transfer,
        "resolve_connector_config",
        lambda endpoint: dest_config
        if endpoint.format == "iceberg"
        else {"type": endpoint.format, "database": "dataflow"},
    )
    monkeypatch.setattr(
        cdc_transfer, "resolve_driver_type", lambda value: value
    )
    monkeypatch.setattr(
        cdc_transfer, "resolve_dest_table", lambda *_args, **_kwargs: "events"
    )
    monkeypatch.setattr(
        stream_dest_procedure,
        "refuse_cdc_destination_row_apply",
        lambda *_args, **_kwargs: None,
    )

    calls: list[str] = []
    monkeypatch.setattr(
        cdc_eos_sql,
        "apply_change_batch_exactly_once",
        lambda *_args, **_kwargs: calls.append("eos"),
    )
    monkeypatch.setattr(
        iceberg_writer,
        "write_mapped_rows",
        lambda *_args, **_kwargs: calls.append("write"),
    )

    source = EndpointConfig(
        kind="database", format="postgresql", table="source_events"
    )
    destination = EndpointConfig(
        kind="database",
        format="iceberg",
        table="events",
        extra={"require_exactly_once": True},
    )

    with pytest.raises(ExactlyOnceRouteError) as raised:
        cdc_transfer.run_cdc_database_transfer(
            source,
            destination,
            [{"source": "id", "target": "id"}],
            {"id": "INTEGER"},
            sync_mode="cdc",
            stream_contracts=[
                {
                    "name": "source_events",
                    "primary_key": "id",
                    "cursor_field": "id",
                    "cursor_semantics": "cdc_position",
                }
            ],
            delivery_guarantee="auto",
        )

    assert ICEBERG_REFUSAL in str(raised.value)
    assert calls == []


def _cdc_transfer_harness(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    batches: list[Any],
) -> tuple[Any, Any, dict[str, list[Any]]]:
    from types import SimpleNamespace

    from connectors import cdc_eos_sql, iceberg_writer
    from services import cdc_retention_probe, cdc_slot_resume, row_conservation
    from services import source_ha_probe
    from services.cdc_snapshot_mode import SnapshotMode
    from src.transfer import cdc_transfer, stream_dest_procedure
    from src.transfer.models import EndpointConfig

    calls: dict[str, list[Any]] = {"eos": [], "writer": []}

    class FakeReader:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            self._batches = list(batches)

        def is_available(self) -> bool:
            return True

        def poll(self):
            while self._batches:
                yield self._batches.pop(0)

        def capture_has_pending(self) -> bool:
            return False

        def cdc_metadata(self) -> dict[str, Any]:
            return {}

        def ack(self, _token: Any) -> None:
            return None

        def close(self) -> None:
            return None

    monkeypatch.setattr(
        cdc_transfer,
        "resolve_connector_config",
        lambda endpoint: (
            _catalog_config(tmp_path)
            if endpoint.format == "iceberg"
            else {"type": "postgresql", "database": "source", "schema": "public"}
        ),
    )
    monkeypatch.setattr(cdc_transfer, "resolve_driver_type", lambda value: value)
    monkeypatch.setattr(cdc_transfer, "resolve_dest_table", lambda *_args: "events")
    monkeypatch.setattr(cdc_transfer, "PostgreSqlChangeStreamCdc", FakeReader)
    monkeypatch.setattr(
        stream_dest_procedure,
        "refuse_cdc_destination_row_apply",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(cdc_transfer, "get_watermark", lambda _key: None)
    monkeypatch.setattr(cdc_transfer, "set_watermark", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(cdc_transfer, "resolve_snapshot_mode", lambda *_a, **_k: SnapshotMode.NEVER)
    monkeypatch.setattr(
        cdc_transfer,
        "resolve_cdc_snapshot_plan",
        lambda *_a, **_k: {
            "kind": "stream",
            "run_snapshot": False,
            "run_stream": True,
        },
    )
    monkeypatch.setattr(cdc_transfer, "measure_dest_already_keyed", lambda *_a, **_k: False)
    monkeypatch.setattr(cdc_transfer, "adapter_supports_incremental_interleave", lambda *_a: False)
    def fake_iceberg_writer(**kwargs: Any) -> Any:
        calls["writer"].append(kwargs)
        return SimpleNamespace(
            ok=True,
            rows_written=len(kwargs.get("data_rows") or []),
            table_name=kwargs.get("table_name"),
            target_schema="default",
            checksum="writer-checksum",
            driver="iceberg",
            rejected_rows=0,
            rejected_details=[],
            warnings=[],
            coerced_null_rows=0,
            rows_skipped=0,
        )

    monkeypatch.setattr(iceberg_writer, "write_mapped_rows", fake_iceberg_writer)
    monkeypatch.setattr(cdc_transfer, "_stamp_cdc_source_image", lambda summary, **_k: summary)
    monkeypatch.setattr(cdc_transfer, "_cdc_lag_fields", lambda *_a: {})
    monkeypatch.setattr(cdc_transfer, "_stamp_cdc_poll", lambda *_a, **_k: None)
    monkeypatch.setattr(cdc_transfer, "stamp_capture_identity", lambda *_a, **_k: None)
    monkeypatch.setattr(
        cdc_slot_resume, "prepare_resume_for_missing_slot", lambda watermark, *_a, **_k: (watermark, "")
    )
    monkeypatch.setattr(source_ha_probe, "attach_source_ha", lambda *_a, **_k: None)
    monkeypatch.setattr(cdc_retention_probe, "attach_cdc_retention", lambda *_a, **_k: None)
    monkeypatch.setattr(row_conservation, "record_stream_health", lambda *_a, **_k: None)
    monkeypatch.setattr(
        cdc_eos_sql,
        "open_eos_session",
        lambda **_kwargs: SimpleNamespace(resume=None, dest_lsn=None, fence_epoch=0),
    )
    monkeypatch.setattr(
        cdc_eos_sql,
        "apply_change_batch_exactly_once",
        lambda **kwargs: (
            calls["eos"].append(kwargs) or (1, "eos-checksum", {}, 0)
        ),
    )
    monkeypatch.setattr(
        "services.cdc_exactly_once.persist_dest_keyset_on_signal",
        lambda *_a, **_k: None,
    )
    monkeypatch.setattr(
        "services.cdc_snapshot_mode.measure_dest_already_keyed",
        lambda *_a, **_k: False,
    )

    source = EndpointConfig(
        kind="database", format="postgresql", table="source_events"
    )
    destination = EndpointConfig(
        kind="database", format="iceberg", table="events"
    )
    return source, destination, calls


def test_cdc_transfer_pinned_exactly_once_without_primary_key_refuses_before_write(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from src.transfer import cdc_transfer

    source, destination, calls = _cdc_transfer_harness(monkeypatch, tmp_path, [])
    monkeypatch.setattr(cdc_transfer, "_catalog_cdc_primary_key", lambda *_a, **_k: "")

    with pytest.raises(ValueError, match="requires primary_key"):
        cdc_transfer.run_cdc_database_transfer(
            source,
            destination,
            [{"source": "id", "target": "id"}],
            {"id": "INTEGER"},
            stream_contracts=[
                {
                    "name": "source_events",
                    "primary_key": "",
                    "cursor_field": "id",
                    "cursor_semantics": "cdc_position",
                }
            ],
            delivery_guarantee="exactly_once",
            delivery_pinned=True,
        )

    assert calls == {"eos": [], "writer": []}


def test_cdc_transfer_pinned_exactly_once_missing_lsn_refuses_before_apply_or_write(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from services.cdc_exactly_once import REASON_NO_LSN, ExactlyOnceRouteError
    from services.cdc_engine import ChangeBatch
    from src.transfer import cdc_transfer

    batch = ChangeBatch(
        inserts=[{"id": 1, "value": "new"}],
        resume_token={"kind": "test"},
    )
    source, destination, calls = _cdc_transfer_harness(
        monkeypatch, tmp_path, [batch]
    )

    with pytest.raises(ExactlyOnceRouteError) as raised:
        cdc_transfer.run_cdc_database_transfer(
            source,
            destination,
            [
                {"source": "id", "target": "id"},
                {"source": "value", "target": "value"},
            ],
            {"id": "INTEGER", "value": "VARCHAR"},
            stream_contracts=[
                {
                    "name": "source_events",
                    "primary_key": "id",
                    "cursor_field": "id",
                    "cursor_semantics": "cdc_position",
                }
            ],
            delivery_guarantee="exactly_once",
            delivery_pinned=True,
        )

    assert raised.value.reason == REASON_NO_LSN
    assert calls == {"eos": [], "writer": []}


def test_cdc_transfer_auto_without_batch_lsn_uses_writer_not_eos_apply(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from services.cdc_engine import ChangeBatch
    from src.transfer import cdc_transfer

    batch = ChangeBatch(
        inserts=[{"id": 1, "value": "new"}],
        resume_token={"kind": "test"},
    )
    source, destination, calls = _cdc_transfer_harness(
        monkeypatch, tmp_path, [batch]
    )

    cdc_transfer.run_cdc_database_transfer(
        source,
        destination,
        [
            {"source": "id", "target": "id"},
            {"source": "value", "target": "value"},
        ],
        {"id": "INTEGER", "value": "VARCHAR"},
        stream_contracts=[
            {
                "name": "source_events",
                "primary_key": "id",
                "cursor_field": "id",
                "cursor_semantics": "cdc_position",
            }
        ],
        delivery_guarantee="auto",
        delivery_pinned=False,
    )

    assert calls["writer"]
    assert calls["eos"] == []


def test_engine_uses_resolved_saved_catalog_endpoint_for_route(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from services import cdc_exactly_once
    from services import mapping_pipeline
    from src.transfer import adapters
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    catalog = _catalog_config(tmp_path)
    resolved: list[str | None] = []

    def resolve_config(endpoint: EndpointConfig, workspace_id: str | None = None):
        if endpoint.connector_id == "saved-iceberg":
            resolved.append(endpoint.connector_id)
            return {
                "type": "iceberg",
                "table": "events",
                "table_name": "events",
                "schema": "default",
                "connection_string": catalog["connection_string"],
                "warehouse": catalog["warehouse"],
                "catalog_type": "sql",
            }
        return {"type": endpoint.format}

    monkeypatch.setattr(adapters, "resolve_connector_config", resolve_config)
    observed: dict[str, Any] = {}
    select = cdc_exactly_once.select_route_delivery

    def select_and_capture(requested: str | None, **kwargs: Any) -> str:
        observed["dest_cfg"] = kwargs.get("dest_cfg")
        result = select(requested, **kwargs)
        observed["result"] = result
        return result

    monkeypatch.setattr(
        cdc_exactly_once, "select_route_delivery", select_and_capture
    )

    class FakeMongo:
        def get_job(self, _job_id: str) -> None:
            return None

        def create_transfer_job(self, _job: dict[str, Any]) -> None:
            return None

    from src.transfer import engine as engine_module

    monkeypatch.setattr(engine_module, "get_mongodb_service", lambda: FakeMongo())

    class StopAfterRoute(RuntimeError):
        pass

    monkeypatch.setattr(
        mapping_pipeline,
        "assert_mappings_executable",
        lambda _mappings, **_kw: (_ for _ in ()).throw(StopAfterRoute()),
    )
    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="postgresql", table="source_events"
        ),
        destination=EndpointConfig(
            kind="database",
            format="iceberg",
            connector_id="saved-iceberg",
            table="events",
        ),
        sync_mode="cdc",
        stream_contracts=[
            {
                "name": "source_events",
                "primary_key": "id",
                "cursor_semantics": "cdc_position",
            }
        ],
    )

    with pytest.raises(StopAfterRoute):
        UniversalTransferEngine()._execute_tracked_inner(request, "route-test")

    assert resolved == ["saved-iceberg"]
    assert observed["result"] == "exactly_once"
    assert observed["dest_cfg"]["extra"]["catalog_type"] == "sql"


def _router_client(monkeypatch: pytest.MonkeyPatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from src.routers import transfer_router
    from src.transfer import background, contract_engine, engine

    class FakeEngine:
        def _create_pending_job(self, _request: Any) -> str:
            return "iceberg-route-test"

    monkeypatch.setattr(engine, "get_transfer_engine", lambda: FakeEngine())
    monkeypatch.setattr(background, "run_transfer_async", lambda *_args: None)
    monkeypatch.setattr(
        contract_engine, "stamp_request_contract", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(
        transfer_router,
        "_stamp_job_mapping_artifacts",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(transfer_router, "resolve_write_workspace", lambda *_args: "")
    monkeypatch.setattr(transfer_router, "_residency_check", lambda *_args: None)
    monkeypatch.setattr(transfer_router, "_actor_email", lambda *_args: "test@example.com")
    app = FastAPI()
    app.include_router(transfer_router.router, prefix="/api/v1")
    return TestClient(app)


def _saved_iceberg_payload(delivery_guarantee: str) -> dict[str, Any]:
    return {
        "source": {"kind": "database", "format": "postgresql"},
        "destination": {
            "kind": "database",
            "format": "iceberg",
            "connector_id": "saved-iceberg",
        },
        "sync_mode": "cdc",
        "stream_contracts": [
            {
                "name": "source_events",
                "primary_key": "id",
                "cursor_semantics": "cdc_position",
            }
        ],
        "delivery_guarantee": delivery_guarantee,
        "async_mode": True,
    }


def test_router_auto_saved_iceberg_reference_does_not_return_400(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _router_client(monkeypatch)

    response = client.post(
        "/api/v1/transfer/execute",
        json=_saved_iceberg_payload("auto"),
    )

    assert response.status_code != 400, response.text
    assert response.status_code == 200, response.text


def test_router_explicit_pin_saved_iceberg_reference_returns_readable_400(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _router_client(monkeypatch)

    response = client.post(
        "/api/v1/transfer/execute",
        json=_saved_iceberg_payload("exactly_once"),
    )

    assert response.status_code == 400
    assert ICEBERG_REFUSAL in response.text


def test_non_iceberg_wired_sinks_are_unchanged_with_dest_config() -> None:
    from services.cdc_exactly_once import classify_exactly_once_route

    for destination in sorted(EOS_TRANSACTIONAL_DESTS & EOS_TXN_WIRED_DESTS):
        without_config = classify_exactly_once_route(
            dest_type=destination,
            sync_mode="cdc",
            has_primary_key=True,
            write_mode="upsert",
            has_lsn_column=True,
        )
        with_config = classify_exactly_once_route(
            dest_type=destination,
            sync_mode="cdc",
            has_primary_key=True,
            write_mode="upsert",
            has_lsn_column=True,
            dest_cfg={"unrelated": "config"},
        )

        assert with_config == without_config
        assert with_config.eligible


def test_iceberg_effectively_once_fallback_and_claim_flags_stay_false() -> None:
    posture = classify_sink_delivery(
        dest_type="iceberg",
        has_primary_key=True,
        write_mode="upsert",
        has_lsn_column=True,
    )
    from services import cdc_effectively_once

    assert posture["class"] == "effectively_once_eligible"
    assert PLATFORM_EXACTLY_ONCE_CLAIMED is False
    assert cdc_effectively_once.EXACTLY_ONCE_CLAIMED is False
