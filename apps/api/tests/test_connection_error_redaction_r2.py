from __future__ import annotations

import logging
from types import SimpleNamespace

from fastapi.testclient import TestClient

from services.logging_config import ContextFilter, JsonFormatter, TextFormatter
from src.transfer.connector_registry import humanize_connection_error


def test_humanized_connection_error_uses_canonical_redaction() -> None:
    raw = (
        "connection failed for postgresql://operator:S3NT1NEL-pw@db.example:5432/"
        "warehouse pwd=S3NT1NEL-pwd"
    )
    message = humanize_connection_error("custom_driver", raw)
    assert "S3NT1NEL" not in message
    # W3 error taxonomy: a connection URI (host, port, user) never reaches the copy.
    assert "db.example" not in message and ":5432" not in message
    assert "the connection string" in message
    assert "pwd=***" in message


def test_humanized_driver_echo_is_capped_after_redaction(monkeypatch) -> None:
    from services.error_taxonomy import driver_sentence

    long_detail = "driver detail " + ("row-value-" * 40)
    message = humanize_connection_error("custom_driver", long_detail)
    # W3 error taxonomy: the driver's sentence is quoted once, capped on a word.
    echoed = driver_sentence(long_detail)
    assert echoed in message
    assert len(echoed) <= 221
    assert echoed.endswith("…")

    dsn_error = (
        "zzqx driver failure for postgresql://operator:S3NT1NEL-pw@db.example:5432/"
        "warehouse pwd=S3NT1NEL-pwd " + ("driver detail " * 30)
    )
    auth_message = humanize_connection_error("custom_driver", dsn_error)
    driver_echo = driver_sentence(dsn_error)
    assert driver_echo in auth_message
    assert len(driver_echo) <= 221
    assert "S3NT1NEL" not in auth_message
    assert "db.example" not in auth_message


def test_saved_connector_probe_response_and_logs_are_redacted(monkeypatch) -> None:
    from services import connector_store
    from src.main import app
    from src.routers import saved_connectors_router
    from src.transfer import connector_registry

    connector = SimpleNamespace(
        id="saved-r2",
        name="Saved PG",
        type="postgresql",
        role="both",
        host="db.example",
        port=5432,
        database="warehouse",
        username="operator",
        password="",
        schema="public",
        connection_string="",
        warehouse="",
        ssl=False,
        auth_mode="",
        auth_role="",
        api_key="",
        service_account="",
        private_key="",
        endpoint_url="",
        path_style=False,
        auth_source="",
        extra={},
    )
    monkeypatch.setattr(saved_connectors_router, "get_connector", lambda *_a, **_k: connector)
    monkeypatch.setattr(saved_connectors_router, "_can_access_connector", lambda *_a, **_k: True)
    monkeypatch.setattr(saved_connectors_router, "mark_tested", lambda *_a, **_k: None)
    monkeypatch.setattr(connector_store, "get_connector", lambda *_a, **_k: connector)

    def fail(*_args, **_kwargs):
        raise RuntimeError(
            "connection failed for postgresql://operator:S3NT1NEL-pw@db.example:5432/"
            "warehouse pwd=S3NT1NEL-pwd"
        )

    monkeypatch.setattr(connector_registry, "run_probe", fail)
    logger = logging.getLogger("dataflow.connector")
    capture = logging.Handler()
    capture.records = []
    capture.addFilter(ContextFilter())
    capture.emit = lambda record: capture.records.append(record)
    old_level = logger.level
    old_propagate = logger.propagate
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    logger.addHandler(capture)
    try:
        response = TestClient(app).post("/api/v1/connectors/saved/saved-r2/test")
    finally:
        logger.removeHandler(capture)
        logger.setLevel(old_level)
        logger.propagate = old_propagate

    assert response.status_code == 200
    assert response.json()["success"] is False
    body = response.text + response.json()["message"]
    rendered = "\n".join(
        [JsonFormatter().format(record) for record in capture.records]
        + [TextFormatter().format(record) for record in capture.records]
    )
    assert "S3NT1NEL" not in body
    assert "S3NT1NEL" not in rendered
    # W3 error taxonomy: no host:port in user copy (the log keeps the redacted URI).
    assert "db.example:5432" not in body
