from __future__ import annotations

import json
import logging
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from services.logging_config import ContextFilter, JsonFormatter, TextFormatter
from src.routers import query_router

_QUERY_ERROR = (
    "syntax error near S3NT1NEL-syntax password=S3NT1NEL-pw "
    "postgresql://u:S3NT1NEL-dsn@h/db"
)
_DRIVER_ERROR = (
    "zzqx driver exception for "
    "postgresql://operator:S3NT1NEL-pw@db.example:5432/warehouse "
    "pwd=S3NT1NEL-pwd " + ("driver detail " * 30)
)


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
def query_error_capture():
    loggers = [
        logging.getLogger("dataflow.errors"),
        logging.getLogger("dataflow.connector"),
    ]
    previous = [(logger, logger.level, logger.propagate) for logger in loggers]
    capture = _Capture()
    for logger in loggers:
        logger.setLevel(logging.DEBUG)
        logger.propagate = False
        logger.addHandler(capture)
    try:
        yield capture
    finally:
        for logger, level, propagate in previous:
            logger.removeHandler(capture)
            logger.setLevel(level)
            logger.propagate = propagate


@pytest.fixture
def query_error_client(monkeypatch):
    from src.main import app

    connector = SimpleNamespace(
        id="query-user-r4",
        type="sqlite",
        host="",
        port=0,
        database="",
        username="",
        password="",
        connection_string="sqlite://",
        schema="",
        warehouse="",
        ssl=False,
    )
    monkeypatch.setattr(
        query_router.connector_store,
        "get_connector",
        lambda *_args, **_kwargs: connector,
    )
    return TestClient(app), connector


def test_user_query_error_detail_is_redacted_capped_and_logged(monkeypatch):
    from services import safe_errors, tracing

    capture = _Capture()
    logger = logging.getLogger("dataflow.r4.user_query")
    old_level, old_propagate = logger.level, logger.propagate
    logger.setLevel(logging.WARNING)
    logger.propagate = False
    logger.addHandler(capture)
    monkeypatch.setattr(tracing, "get_correlation_id", lambda: "r4-user-query-helper")
    monkeypatch.setenv(safe_errors.EXPOSE_ENV, "0")
    try:
        error = safe_errors.user_query_error(
            RuntimeError("syntax error " + ("row-value-" * 80)),
            public_message="Query failed",
            logger=logger,
        )
    finally:
        logger.removeHandler(capture)
        logger.setLevel(old_level)
        logger.propagate = old_propagate

    assert error.status_code == 400
    assert error.headers == {"X-Correlation-ID": "r4-user-query-helper"}
    assert error.detail.startswith("Query failed: ")
    detail_text = error.detail.removeprefix("Query failed: ").rsplit(
        " Reference: r4-user-query-helper", 1
    )[0]
    assert len(detail_text) == 500
    event = capture.records[-1]
    assert event.outcome == "failed"
    assert event.error_type == "RuntimeError"
    assert event.correlation_id == "r4-user-query-helper"
    assert event.levelno == logging.WARNING


def test_sql_query_error_is_visible_only_in_redacted_response(
    query_error_client, query_error_capture, monkeypatch
):
    from connectors import generic_sql

    client, connector = query_error_client

    class BrokenConnection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, *_args, **_kwargs):
            raise RuntimeError(_QUERY_ERROR)

    class BrokenEngine:
        def connect(self):
            return BrokenConnection()

    monkeypatch.setattr(generic_sql, "get_sqlalchemy_engine", lambda *_a, **_k: BrokenEngine())
    monkeypatch.setattr(query_router, "_apply_read_only_session", lambda *_a: None)
    cid = "r4-sql-query"
    response = client.post(
        "/api/v1/query/execute",
        json={"connector_id": connector.id, "query": "SELECT 1"},
        headers={"X-Correlation-ID": cid},
    )

    assert response.status_code == 400
    assert response.headers["X-Correlation-ID"] == cid
    detail = response.json()["detail"]
    assert "Query failed: syntax error near S3NT1NEL-syntax" in detail
    assert "password=***" in detail
    assert "postgresql://u:***@h/db" in detail
    assert "S3NT1NEL-pw" not in response.text
    assert "S3NT1NEL-dsn" not in response.text
    rendered = query_error_capture.rendered()
    assert "S3NT1NEL-syntax" not in rendered
    assert "S3NT1NEL-pw" not in rendered
    assert "S3NT1NEL-dsn" not in rendered
    events = [
        record
        for record in query_error_capture.records
        if getattr(record, "event", None) == "query.user_query.failed"
    ]
    assert events
    assert events[-1].outcome == "failed"
    assert events[-1].error_type == "RuntimeError"
    assert events[-1].correlation_id == cid
    assert events[-1].levelno == logging.WARNING


def test_snowflake_query_error_is_reported_as_user_query_failure(monkeypatch):
    from connectors import snowflake_conn
    from services import tracing

    class BrokenCursor:
        description = []

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, *_args, **_kwargs):
            raise RuntimeError(_QUERY_ERROR)

    class BrokenConnection:
        def cursor(self):
            return BrokenCursor()

        def close(self):
            return None

    monkeypatch.setattr(
        snowflake_conn,
        "get_connection",
        lambda **_kwargs: BrokenConnection(),
    )
    tracing.set_correlation_id("r4-snowflake-query")
    connector = SimpleNamespace(
        type="snowflake",
        host="snowflake.example",
        username="operator",
        password="",
        database="ANALYTICS",
        schema="PUBLIC",
        warehouse="WH",
        connection_string="",
        auth_role="",
        role="",
        private_key="",
    )
    body = query_router.QueryExecuteRequest(
        connector_id="snowflake-r4",
        query="SELECT 1",
    )
    try:
        with pytest.raises(HTTPException) as caught:
            query_router._run_snowflake_query(connector, body)
    finally:
        tracing.set_correlation_id("")

    assert caught.value.status_code == 400
    assert "Query failed: syntax error near S3NT1NEL-syntax" in caught.value.detail
    assert "password=***" in caught.value.detail
    assert "postgresql://u:***@h/db" in caught.value.detail
    assert caught.value.detail.endswith(" Reference: r4-snowflake-query")
    assert caught.value.headers == {"X-Correlation-ID": "r4-snowflake-query"}


def test_snowflake_connection_error_uses_classifier_then_humanizer(monkeypatch):
    from connectors import snowflake_conn
    from services import tracing

    classified_inputs = []
    humanized_inputs = []

    def classify(raw, auth_mode=""):
        classified_inputs.append((raw, auth_mode))
        return "classified Snowflake login failure"

    def humanize(driver, error):
        humanized_inputs.append((driver, error))
        return "humanized Snowflake login failure"

    monkeypatch.setattr(snowflake_conn, "classify_snowflake_connect_error", classify)
    monkeypatch.setattr(query_router, "humanize_connection_error", humanize)
    monkeypatch.setattr(
        snowflake_conn,
        "get_connection",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError(_DRIVER_ERROR)),
    )
    tracing.set_correlation_id("r4-snowflake-connect")
    connector = SimpleNamespace(
        type="snowflake",
        host="snowflake.example",
        username="operator",
        password="",
        database="ANALYTICS",
        schema="PUBLIC",
        warehouse="WH",
        connection_string="",
        auth_role="",
        role="",
        private_key="",
    )
    body = query_router.QueryExecuteRequest(
        connector_id="snowflake-r4",
        query="SELECT 1",
    )
    try:
        with pytest.raises(HTTPException) as caught:
            query_router._run_snowflake_query(connector, body)
    finally:
        tracing.set_correlation_id("")

    assert caught.value.status_code == 400
    assert caught.value.detail == (
        "Could not connect to Snowflake: humanized Snowflake login failure"
    )
    assert classified_inputs and "S3NT1NEL-pw" not in classified_inputs[0][0]
    assert humanized_inputs == [
        ("snowflake", "classified Snowflake login failure")
    ]


def test_sql_engine_creation_failure_uses_connection_humanizer(monkeypatch):
    from connectors import generic_sql

    def fail(*_args, **_kwargs):
        raise ValueError(_DRIVER_ERROR)

    monkeypatch.setattr(generic_sql, "get_sqlalchemy_engine", fail)
    connector = SimpleNamespace(
        type="postgresql",
        host="db.example",
        port=5432,
        database="warehouse",
        username="operator",
        password="",
        connection_string="",
        schema="public",
        warehouse="",
        ssl=False,
    )
    body = query_router.QueryExecuteRequest(
        connector_id="postgres-r4",
        query="SELECT 1",
    )
    with pytest.raises(HTTPException) as caught:
        query_router._run_sql_query(connector, body)

    assert caught.value.status_code == 400
    assert caught.value.detail.startswith("Connection failed: ")
    assert "db.example:5432" in caught.value.detail
    assert "S3NT1NEL" not in caught.value.detail
    assert "pwd=***" in caught.value.detail
    driver_echo = caught.value.detail.removeprefix("Connection failed: ")
    assert len(driver_echo) <= 120


def test_sqlalchemy_missing_driver_runtime_error_is_preserved(monkeypatch):
    from connectors import generic_sql

    missing_driver = (
        "SQLAlchemy dialect/driver for 'postgresql' is not available "
        "(tried 'postgresql'). Install/enable psycopg2-binary "
        "(scheme postgresql+psycopg2://)."
    )

    def fail(*_args, **_kwargs):
        raise RuntimeError(missing_driver)

    monkeypatch.setattr(generic_sql, "get_sqlalchemy_engine", fail)
    connector = SimpleNamespace(
        type="postgresql",
        host="",
        port=0,
        database="",
        username="",
        password="",
        connection_string="",
        schema="",
        warehouse="",
        ssl=False,
    )
    body = query_router.QueryExecuteRequest(
        connector_id="postgres-r4",
        query="SELECT 1",
    )
    with pytest.raises(HTTPException) as caught:
        query_router._run_sql_query(connector, body)

    assert caught.value.status_code == 400
    assert caught.value.detail == missing_driver


def test_schema_introspection_failure_uses_connection_humanizer(
    query_error_client, monkeypatch
):
    from services import connector_probe
    from src.transfer import endpoint_intelligence

    client, _connector = query_error_client
    endpoint = SimpleNamespace(format="postgresql", extra={})
    monkeypatch.setattr(
        connector_probe,
        "endpoint_from_saved_connector",
        lambda *_args, **_kwargs: endpoint,
    )

    def fail(*_args, **_kwargs):
        raise ValueError(_DRIVER_ERROR)

    monkeypatch.setattr(endpoint_intelligence, "introspect_endpoint", fail)
    response = client.post(
        "/api/v1/query/schema",
        json={"connector_id": "query-user-r4"},
    )

    assert response.status_code == 400
    detail = response.json()["detail"]
    assert detail.startswith("Schema introspection failed: Connection failed: ")
    assert "db.example:5432" in detail
    assert "S3NT1NEL" not in detail
    assert "pwd=***" in detail
    driver_echo = detail.removeprefix("Schema introspection failed: Connection failed: ")
    assert len(driver_echo) <= 120


def test_filter_json_error_uses_user_query_error(monkeypatch, query_error_capture):
    from services import tracing

    class FakeMongo:
        def __getitem__(self, _name):
            return self

    monkeypatch.setattr(query_router, "_mongo_client", lambda *_args, **_kwargs: FakeMongo())
    monkeypatch.setattr(
        query_router,
        "_parse_mongodb_json",
        lambda value: (_ for _ in ()).throw(
            json.JSONDecodeError("syntax near S3NT1NEL-filter", value, 0)
        ),
    )
    tracing.set_correlation_id("r4-mongo-json")
    connector = SimpleNamespace(
        type="mongodb",
        connection_string="mongodb://localhost/test",
        database="test",
    )
    body = query_router.QueryExecuteRequest(connector_id="mongo-r4", query="{")
    try:
        with pytest.raises(HTTPException) as caught:
            query_router._run_mongodb_query(connector, body)
    finally:
        tracing.set_correlation_id("")

    assert caught.value.status_code == 400
    assert caught.value.detail.startswith("Invalid MongoDB filter JSON: ")
    assert "S3NT1NEL-filter" in caught.value.detail
    assert caught.value.detail.endswith(" Reference: r4-mongo-json")
    assert caught.value.headers == {"X-Correlation-ID": "r4-mongo-json"}
    assert "S3NT1NEL-filter" not in query_error_capture.rendered()
    event = query_error_capture.records[-1]
    assert event.event == "query.user_query.failed"
    assert event.outcome == "failed"
    assert event.correlation_id == "r4-mongo-json"


def test_parser_value_error_uses_user_query_error(monkeypatch, query_error_capture):
    original_loads = query_router.json.loads
    from services import tracing

    def fail(*_args, **_kwargs):
        raise ValueError("S3NT1NEL-invalid-query")

    monkeypatch.setattr(query_router.json, "loads", fail)
    tracing.set_correlation_id("r4-mongo-parser")
    try:
        with pytest.raises(HTTPException) as caught:
            query_router._parse_mongodb_json("{}")
    finally:
        tracing.set_correlation_id("")
        monkeypatch.setattr(query_router.json, "loads", original_loads)

    assert caught.value.status_code == 400
    assert caught.value.detail.startswith("Invalid MongoDB query: S3NT1NEL-invalid-query")
    assert caught.value.detail.endswith(" Reference: r4-mongo-parser")
    assert caught.value.headers == {"X-Correlation-ID": "r4-mongo-parser"}
    assert "S3NT1NEL-invalid-query" not in query_error_capture.rendered()
    event = query_error_capture.records[-1]
    assert event.event == "query.user_query.failed"
    assert event.outcome == "failed"
    assert event.correlation_id == "r4-mongo-parser"
