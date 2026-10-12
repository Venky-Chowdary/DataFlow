from __future__ import annotations

import json
import logging
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from services.logging_config import ContextFilter, JsonFormatter, TextFormatter
from src.routers import query_router

_ERROR = (
    "password=S3NT1NEL-pw postgresql://u:S3NT1NEL-dsn@h/db "
    "row=S3NT1NEL-row SELECT * FROM S3NT1NEL_tbl"
)
_SENTINELS = ("S3NT1NEL-pw", "S3NT1NEL-dsn", "S3NT1NEL-row", "S3NT1NEL_tbl")


class _Capture(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []
        self.addFilter(ContextFilter())

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)

    def rendered(self) -> str:
        return "\n".join(
            [JsonFormatter().format(record) for record in self.records]
            + [TextFormatter().format(record) for record in self.records]
        )


@pytest.fixture
def safe_query_client(monkeypatch):
    from src.main import app

    source = SimpleNamespace(id="source-r2", type="sqlite")
    destination = SimpleNamespace(
        id="destination-r2",
        type="sqlite",
        host="",
        port=0,
        database="",
        schema="",
        username="",
        password="",
        connection_string="sqlite://",
        warehouse="",
        ssl=False,
        auth_mode="",
        auth_role="",
        auth_source="",
        api_key="",
        service_account="",
        endpoint_url="",
        path_style=False,
    )
    monkeypatch.setattr(
        query_router.connector_store,
        "get_connector",
        lambda connector_id, **_kw: (
            destination if connector_id == destination.id else source
        ),
    )
    return TestClient(app), source, destination


@pytest.fixture
def connector_logs():
    loggers = [
        logging.getLogger("dataflow.connector"),
        logging.getLogger("dataflow.errors"),
    ]
    old_levels = [(logger, logger.level, logger.propagate) for logger in loggers]
    capture = _Capture()
    for logger in loggers:
        logger.setLevel(logging.DEBUG)
        logger.propagate = False
        logger.addHandler(capture)
    try:
        yield capture
    finally:
        for logger, old_level, old_propagate in old_levels:
            logger.removeHandler(capture)
            logger.setLevel(old_level)
            logger.propagate = old_propagate


def _assert_safe(response, capture: _Capture, cid: str) -> dict:
    body = response.json()
    rendered = json.dumps(body) + capture.rendered()
    assert response.headers["X-Correlation-ID"] == cid
    assert all(sentinel not in rendered for sentinel in _SENTINELS)
    return body


def _assert_error_event(capture: _Capture, *, event: str, cid: str) -> None:
    records = [
        record
        for record in capture.records
        if getattr(record, "event", None) == event
        and getattr(record, "correlation_id", None) == cid
    ]
    assert records, f"missing {event} error event"
    assert records[-1].outcome == "errored"
    assert records[-1].error_type == "RuntimeError"


def test_query_execute_uses_safe_error_and_correlation_id(
    safe_query_client, connector_logs, monkeypatch
):
    client, source, _destination = safe_query_client

    def fail(*_args, **_kwargs):
        raise RuntimeError(_ERROR)

    monkeypatch.setattr(query_router, "_run_sql_query", fail)
    cid = "query-execute-r2"
    response = client.post(
        "/api/v1/query/execute",
        json={"connector_id": source.id, "query": "SELECT 1"},
        headers={"X-Correlation-ID": cid},
    )
    assert response.status_code == 500
    body = _assert_safe(response, connector_logs, cid)
    assert body == {
        "detail": f"Query execution failed. Reference: {cid}",
        "correlation_id": cid,
    }
    _assert_error_event(connector_logs, event="query.execute.errored", cid=cid)


def test_query_export_query_failure_uses_safe_reference(
    safe_query_client, connector_logs, monkeypatch
):
    client, source, _destination = safe_query_client

    def fail(*_args, **_kwargs):
        raise RuntimeError(_ERROR)

    monkeypatch.setattr(query_router, "_run_sql_query", fail)
    cid = "query-export-query-r2"
    response = client.post(
        "/api/v1/query/export",
        json={"connector_id": source.id, "query": "SELECT 1", "format": "csv"},
        headers={"X-Correlation-ID": cid},
    )
    assert response.status_code == 200
    body = _assert_safe(response, connector_logs, cid)
    assert body["error"] == f"Query execution failed. Reference: {cid}"
    _assert_error_event(connector_logs, event="query.export.errored", cid=cid)


def test_query_export_file_failure_uses_safe_reference(
    safe_query_client, connector_logs, monkeypatch
):
    client, source, _destination = safe_query_client
    monkeypatch.setattr(
        query_router,
        "_run_query",
        lambda *_args, **_kwargs: ([{"id": 1}], ["id"], {"id": "INTEGER"}, False),
    )

    def fail(*_args, **_kwargs):
        raise RuntimeError(_ERROR)

    from src.transfer import adapters

    monkeypatch.setattr(adapters, "write_destination_file", fail)
    cid = "query-export-file-r2"
    response = client.post(
        "/api/v1/query/export",
        json={"connector_id": source.id, "query": "SELECT 1", "format": "csv"},
        headers={"X-Correlation-ID": cid},
    )
    assert response.status_code == 200
    body = _assert_safe(response, connector_logs, cid)
    assert body["error"] == f"Export failed. Reference: {cid}"
    _assert_error_event(connector_logs, event="query.export.errored", cid=cid)


def test_query_export_connector_failure_uses_safe_reference(
    safe_query_client, connector_logs, monkeypatch
):
    client, source, _destination = safe_query_client
    monkeypatch.setattr(
        query_router,
        "_run_query",
        lambda *_args, **_kwargs: ([{"id": 1}], ["id"], {"id": "INTEGER"}, False),
    )

    def fail(*_args, **_kwargs):
        raise RuntimeError(_ERROR)

    from src.transfer import adapters

    monkeypatch.setattr(adapters, "write_destination_database", fail)
    cid = "query-export-connector-r2"
    response = client.post(
        "/api/v1/query/export",
        json={
            "connector_id": source.id,
            "query": "SELECT 1",
            "format": "csv",
            "destination_connector_id": "destination-r2",
            "destination": "target",
            "sync_mode": "insert",
        },
        headers={"X-Correlation-ID": cid},
    )
    assert response.status_code == 200
    body = _assert_safe(response, connector_logs, cid)
    assert body["error"] == f"Export failed. Reference: {cid}"
    _assert_error_event(connector_logs, event="query.export.errored", cid=cid)
