from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from services.connector_telemetry import (
    connector_operation,
    driver_family,
    log_retry,
)
from services.logging_config import ContextFilter, JsonFormatter, TextFormatter

_SENTINELS = (
    "S3NT1NEL-pw",
    "postgresql://u:S3NT1NEL@h/db",
    "S3NT1NEL-api-key",
    "S3NT1NEL.host.invalid",
    "SELECT 'S3NT1NEL-row-value' FROM S3NT1NEL_table",
    "S3NT1NEL-row-value",
)


class _Capture(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []
        self.addFilter(ContextFilter())

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)

    def json_lines(self) -> list[str]:
        formatter = JsonFormatter()
        return [formatter.format(record) for record in self.records]

    def text_lines(self) -> list[str]:
        formatter = TextFormatter()
        return [formatter.format(record) for record in self.records]


@pytest.fixture
def event_capture():
    logger = logging.getLogger("dataflow.connector")
    capture = _Capture()
    old_level = logger.level
    old_propagate = logger.propagate
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    logger.addHandler(capture)
    try:
        yield capture
    finally:
        logger.removeHandler(capture)
        logger.setLevel(old_level)
        logger.propagate = old_propagate


def _terminal(capture: _Capture, op: str) -> list[logging.LogRecord]:
    prefix = f"connector.{op}."
    return [
        record
        for record in capture.records
        if str(getattr(record, "event", "")).startswith(prefix)
    ]


def _assert_no_sentinels(capture: _Capture, *extra: str) -> None:
    rendered = "\n".join(capture.json_lines() + capture.text_lines())
    for sentinel in (*_SENTINELS, *extra):
        assert sentinel not in rendered


def _assert_terminal(
    capture: _Capture,
    op: str,
    outcome: str,
    level: int,
) -> logging.LogRecord:
    events = _terminal(capture, op)
    assert len(events) == 1
    record = events[0]
    assert record.event == f"connector.{op}.{outcome}"
    assert record.outcome == outcome
    assert record.levelno == level
    return record


def test_connector_operation_deduplicates_and_filters_fields(event_capture) -> None:
    with connector_operation(
        "probe",
        driver_family="unknown",
        surface="outer",
        connector_id="outer-id",
    ) as outer:
        with connector_operation(
            "probe",
            driver_family="postgresql",
            role="source",
            surface="inner",
            connector_id="inner-id",
        ) as nested:
            assert nested is outer
            nested.update(
                object_count=2,
                password=_SENTINELS[0],
                connection_string=_SENTINELS[1],
                api_key=_SENTINELS[2],
                host=_SENTINELS[3],
                sql=_SENTINELS[4],
                row_value=_SENTINELS[5],
                path="/tmp/S3NT1NEL-file.csv",
                table_name="S3NT1NEL_table",
            )
        with connector_operation(
            "introspect",
            driver_family="sqlite",
            surface="inner",
        ) as different:
            different.succeeded(object_count=1)
        outer.succeeded(row_count=4, column_count=3)

    record = _assert_terminal(event_capture, "probe", "succeeded", logging.INFO)
    assert record.driver_family == "postgresql"
    assert record.role == "source"
    assert record.surface == "outer"
    assert record.connector_id == "outer-id"
    assert record.object_count == 2
    assert record.row_count == 4
    assert record.column_count == 3
    nested_record = _assert_terminal(
        event_capture,
        "introspect",
        "succeeded",
        logging.INFO,
    )
    assert nested_record.object_count == 1
    _assert_no_sentinels(event_capture, "/tmp/S3NT1NEL-file.csv")


def test_connector_operation_emits_success_and_skip_outcomes(event_capture) -> None:
    with connector_operation("query", driver_family="sqlite", role="source") as op:
        op.succeeded(row_count=1)
    record = _assert_terminal(event_capture, "query", "succeeded", logging.INFO)
    assert record.duration_ms >= 0

    event_capture.records.clear()
    with connector_operation("probe", driver_family="csv") as op:
        op.skipped(category="no_input")
    record = _assert_terminal(event_capture, "probe", "skipped", logging.INFO)
    assert record.category == "no_input"
    _assert_no_sentinels(event_capture)

    event_capture.records.clear()
    with connector_operation("probe", driver_family="postgresql"):
        pass
    _assert_terminal(event_capture, "probe", "succeeded", logging.INFO)
    _assert_no_sentinels(event_capture)


def test_connector_operation_rethrows_identical_exception_without_leaking(
    event_capture,
) -> None:
    error = RuntimeError(" | ".join(_SENTINELS))
    with pytest.raises(RuntimeError) as raised:
        with connector_operation("probe", driver_family="postgresql"):
            raise error

    assert raised.value is error
    record = _assert_terminal(event_capture, "probe", "errored", logging.ERROR)
    assert record.error_type == "RuntimeError"
    _assert_no_sentinels(event_capture)


def test_failed_mark_survives_propagating_exception(event_capture) -> None:
    error = RuntimeError(" ".join(_SENTINELS))
    with pytest.raises(RuntimeError) as raised:
        with connector_operation("probe", driver_family="postgresql") as op:
            op.failed(category="probe_failed")
            raise error

    assert raised.value is error
    record = _assert_terminal(event_capture, "probe", "failed", logging.WARNING)
    assert record.category == "probe_failed"
    assert record.error_type == "RuntimeError"
    _assert_no_sentinels(event_capture)


def test_ops_event_guard_handles_a_raising_logging_handler(event_capture) -> None:
    class RaisingHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            raise RuntimeError("handler failure")

    logger = logging.getLogger("dataflow.connector")
    raising = RaisingHandler()
    logger.addHandler(raising)
    try:
        with connector_operation("probe", driver_family="postgresql") as op:
            op.succeeded()
        _assert_terminal(event_capture, "probe", "succeeded", logging.INFO)

        event_capture.records.clear()
        error = RuntimeError("probe failure")
        with pytest.raises(RuntimeError) as raised:
            with connector_operation("probe", driver_family="postgresql"):
                raise error
        assert raised.value is error
        _assert_terminal(event_capture, "probe", "errored", logging.ERROR)
    finally:
        logger.removeHandler(raising)


def test_driver_family_uses_exact_keys_aliases_and_sdk_catalog_ids() -> None:
    from connectors.sdk import list_descriptors
    from src.transfer.connector_capabilities import CATALOG_ID_ALIASES

    allowed = {"postgresql", "csv", "sqlite"} | {
        descriptor.id for descriptor in list_descriptors()
    }
    assert all(driver_family(key) == key for key in allowed)
    assert driver_family("  POSTGRESQL ") == "postgresql"
    assert driver_family("csv") == "csv"
    assert driver_family("sqlite") == "sqlite"
    assert driver_family("http") == "http"
    assert driver_family("sdk_declarative") == "sdk_declarative"
    assert driver_family("singer") == "singer"

    aliases = {
        "amazon_rds_postgresql": "postgresql",
        "mariadb": "mysql",
        "csv___tsv": "csv",
        "excel_workbook": "excel",
    }
    for alias, canonical in aliases.items():
        assert CATALOG_ID_ALIASES[alias] == canonical
        assert driver_family(alias) == canonical
    github = next(
        descriptor for descriptor in list_descriptors() if "github" in descriptor.catalog_ids
    )
    assert driver_family("github") == github.id

    for raw in (
        "S3NT1NEL-api-key",
        "postgres_like_vendor",
        "mongolia",
        "zzqx-unknown",
        *_SENTINELS,
    ):
        assert driver_family(raw) == "unknown"


def test_driver_family_resolution_errors_are_debug_logged(monkeypatch, caplog) -> None:
    from src.transfer import connector_registry

    def fail_registry():
        raise RuntimeError("do not log exception text")

    monkeypatch.setattr(connector_registry, "registered_driver_types", fail_registry)
    with caplog.at_level(logging.DEBUG, logger="dataflow.connector"):
        assert driver_family("unregistered-type") == "unknown"
    assert "RuntimeError" in caplog.text
    assert "do not log exception text" not in caplog.text


def test_log_retry_marks_scheduled_and_exhausted_attempts(event_capture) -> None:
    from services.error_handling import RetryBudget

    budget = RetryBudget(max_attempts=2, base_delay_seconds=0)
    callback = log_retry(
        "http_request",
        driver_family="http",
        budget=budget,
        request_id="request-123",
    )
    budget.attempts_made = 1
    scheduled_error = RuntimeError(" ".join(_SENTINELS))
    scheduled_error.response = SimpleNamespace(status_code=503)
    callback(scheduled_error, 0.125)
    budget.attempts_made = 2
    exhausted_error = RuntimeError(" ".join(_SENTINELS))
    exhausted_error.response = SimpleNamespace(status_code=503)
    callback(exhausted_error, 0.250)

    events = [
        record
        for record in event_capture.records
        if getattr(record, "event", "").startswith("connector.retry.")
    ]
    assert [record.event for record in events] == [
        "connector.retry.scheduled",
        "connector.retry.exhausted",
    ]
    assert [record.outcome for record in events] == ["retrying", "failed"]
    assert [record.levelno for record in events] == [logging.WARNING, logging.ERROR]
    assert [record.attempt for record in events] == [1, 2]
    assert [record.max_attempts for record in events] == [2, 2]
    assert [record.delay_ms for record in events] == [125, 250]
    assert all(record.http_status == 503 for record in events)
    assert all(record.request_id == "request-123" for record in events)
    _assert_no_sentinels(event_capture)


def test_log_retry_resolves_driver_family_only_when_emitting(
    monkeypatch, event_capture
):
    import services.connector_telemetry as telemetry
    from services.error_handling import RetryBudget

    resolved = []
    monkeypatch.setattr(
        telemetry,
        "driver_family",
        lambda raw: resolved.append(raw) or "other",
    )
    budget = RetryBudget(max_attempts=2)
    callback = telemetry.log_retry(
        "read_batch",
        driver_family="zzqx-unknown",
        budget=budget,
    )
    assert resolved == []
    budget.attempts_made = 1
    callback(RuntimeError("S3NT1NEL-host"), 0.25)
    assert resolved == ["zzqx-unknown"]
    events = [
        record
        for record in event_capture.records
        if getattr(record, "event", "").startswith("connector.retry.")
    ]
    assert len(events) == 1
    assert events[0].driver_family == "other"
    _assert_no_sentinels(event_capture)


@pytest.mark.parametrize(
    ("result", "outcome", "expected"),
    [
        ((True, "OK"), "succeeded", (True, "OK")),
        ((False, "safe failure"), "failed", (False, "safe failure")),
        (RuntimeError(" ".join(_SENTINELS)), "errored", None),
    ],
)
def test_run_probe_preserves_results_and_classifies(monkeypatch, event_capture, result, outcome, expected):
    from src.transfer import connector_registry

    def fake_impl(*args):
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(connector_registry, "_run_probe_impl", fake_impl)
    actual = connector_registry.run_probe("postgresql", {"password": _SENTINELS[0]})
    if expected is None:
        assert actual[0] is False
        record = _assert_terminal(event_capture, "probe", "errored", logging.ERROR)
        assert record.error_type == "RuntimeError"
    else:
        assert actual == expected
        record = _assert_terminal(
            event_capture,
            "probe",
            outcome,
            logging.INFO if outcome == "succeeded" else logging.WARNING,
        )
    _assert_no_sentinels(event_capture)


def test_probe_file_source_missing_and_csv(event_capture, tmp_path):
    from src.transfer.connector_registry import probe_file_source

    missing = tmp_path / "S3NT1NEL-missing.csv"
    ok, _ = probe_file_source("csv", str(missing))
    assert not ok
    record = _assert_terminal(event_capture, "probe", "failed", logging.WARNING)
    assert record.category == "not_found"
    _assert_no_sentinels(event_capture, str(missing))

    event_capture.records.clear()
    csv_path = tmp_path / "S3NT1NEL-file.csv"
    csv_path.write_text("S3NT1NEL_table,S3NT1NEL_column\nS3NT1NEL-row-value,1\n")
    ok, _ = probe_file_source("csv", str(csv_path))
    assert ok
    record = _assert_terminal(event_capture, "probe", "succeeded", logging.INFO)
    assert record.row_count == 1
    assert record.column_count == 2
    _assert_no_sentinels(event_capture, str(csv_path))


def test_connectors_test_route_keeps_response_and_redacts_config(
    event_capture, tmp_path
):
    from fastapi.testclient import TestClient
    from src.main import app

    client = TestClient(app)
    response = client.post(
        "/api/v1/connectors/test",
        json={
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 1,
            "username": "user",
            "password": _SENTINELS[0],
        },
    )
    assert response.status_code == 200
    assert set(response.json()) == {"success", "message", "driver", "auth_source"}
    assert response.json()["success"] is False
    _assert_terminal(event_capture, "probe", "failed", logging.WARNING)
    _assert_no_sentinels(event_capture)

    event_capture.records.clear()
    auth_response = client.post(
        "/api/v1/connectors/test",
        json={"type": "postgresql", "host": "127.0.0.1"},
    )
    assert auth_response.status_code == 200
    record = _assert_terminal(event_capture, "probe", "failed", logging.WARNING)
    assert record.category == "auth_config"
    _assert_no_sentinels(event_capture)

    event_capture.records.clear()
    csv_path = tmp_path / "S3NT1NEL-route.csv"
    csv_path.write_text("name\nS3NT1NEL-row-value\n")
    file_response = client.post(
        "/api/v1/connectors/test",
        json={"type": "csv", "connection_string": str(csv_path)},
    )
    assert file_response.status_code == 200
    assert file_response.json()["success"] is True
    assert file_response.json()["details"]["mode"] == "file_source"
    _assert_terminal(event_capture, "probe", "succeeded", logging.INFO)
    _assert_no_sentinels(event_capture, str(csv_path))


def test_saved_probe_decryption_emits_secret_and_terminal_events(
    monkeypatch, event_capture
):
    from services import connector_store
    from services.connector_probe import probe_saved_connector

    conn = SimpleNamespace(
        type="postgresql",
        password="[decryption-failed]",
        connection_string="",
    )
    monkeypatch.setattr(connector_store, "get_connector", lambda *args, **kwargs: conn)
    ok, _, cfg = probe_saved_connector("connector-id")
    assert not ok
    assert cfg == {}
    events = [getattr(record, "event", "") for record in event_capture.records]
    assert events.count("connector.secret.undecryptable") == 1
    secret_record = next(
        record
        for record in event_capture.records
        if record.event == "connector.secret.undecryptable"
    )
    assert secret_record.levelno == logging.ERROR
    assert secret_record.affected_field == "password"
    record = _assert_terminal(event_capture, "probe", "failed", logging.WARNING)
    assert record.category == "secret_undecryptable"
    _assert_no_sentinels(event_capture)


def test_query_sqlite_success_bad_sql_and_invalid_request(
    monkeypatch, event_capture, tmp_path
):
    import sqlite3

    from fastapi import HTTPException
    from services import connector_store
    from src.routers import query_router

    db_path = tmp_path / "S3NT1NEL-query.db"
    db = sqlite3.connect(db_path)
    db.execute('CREATE TABLE "S3NT1NEL_table" ("S3NT1NEL_column" TEXT)')
    db.execute('INSERT INTO "S3NT1NEL_table" VALUES (?)', (_SENTINELS[5],))
    db.commit()
    db.close()
    conn = connector_store.SavedConnector(
        id="query-connector",
        name="query",
        type="sqlite",
        role="source",
        connection_string=f"sqlite:///{db_path}",
    )
    body = query_router.QueryExecuteRequest(
        connector_id=conn.id,
        query='SELECT "S3NT1NEL_column" FROM "S3NT1NEL_table"',
        limit=100,
    )
    rows, columns, *_ = query_router._run_query(conn, body)
    assert len(rows) == 1
    assert len(columns) == 1
    record = _assert_terminal(event_capture, "query", "succeeded", logging.INFO)
    assert record.row_count == 1
    _assert_no_sentinels(event_capture, str(db_path))

    event_capture.records.clear()
    bad_body = query_router.QueryExecuteRequest(
        connector_id=conn.id,
        query="SELECT S3NT1NEL-bad-sql",
        limit=100,
    )
    with pytest.raises(Exception):
        query_router._run_query(conn, bad_body)
    record = _assert_terminal(event_capture, "query", "failed", logging.WARNING)
    assert record.category == "invalid_request"
    _assert_no_sentinels(event_capture, str(db_path))

    event_capture.records.clear()
    error = RuntimeError(" ".join(_SENTINELS))

    def errored(*args, **kwargs):
        raise error

    monkeypatch.setattr(query_router, "_run_sql_query", errored)
    with pytest.raises(RuntimeError) as raised:
        query_router._run_query(conn, body)
    assert raised.value is error
    _assert_terminal(event_capture, "query", "errored", logging.ERROR)
    _assert_no_sentinels(event_capture, str(db_path))

    event_capture.records.clear()

    def invalid(*args, **kwargs):
        raise HTTPException(status_code=400, detail=" ".join(_SENTINELS))

    monkeypatch.setattr(query_router, "_run_sql_query", invalid)
    with pytest.raises(HTTPException):
        query_router._run_query(conn, body)
    record = _assert_terminal(event_capture, "query", "failed", logging.WARNING)
    assert record.category == "invalid_request"
    _assert_no_sentinels(event_capture, str(db_path))


@pytest.mark.parametrize(
    ("behavior", "outcome", "level"),
    [
        ("success", "succeeded", logging.INFO),
        ("false", "failed", logging.WARNING),
        ("raise", "errored", logging.ERROR),
    ],
)
def test_sdk_base_check_classifies_without_changing_return(
    event_capture, behavior, outcome, level
):
    from connectors.sdk import BaseConnector, ConnectorDescriptor

    class FakeConnector(BaseConnector):
        name = "fake_connector"
        descriptor = ConnectorDescriptor(
            id="fake_connector",
            display_name="Fake",
            roles=("source",),
            auth_modes=(),
            sync_modes=("full_refresh",),
            form_fields=(),
            evidence="synthetic-fixture",
        )

        def test_connection(self):
            if behavior == "raise":
                raise RuntimeError(" ".join(_SENTINELS))
            return behavior == "success"

    result = FakeConnector({}).check()
    if behavior == "success":
        assert result == (True, "OK")
    elif behavior == "false":
        assert result == (False, "Connection check failed")
    else:
        assert result[0] is False
        assert result[1] == " ".join(_SENTINELS)
    _assert_terminal(event_capture, "probe", outcome, level)
    _assert_no_sentinels(event_capture)


def test_introspect_endpoint_sqlite_missing_and_file_outcomes(
    event_capture, tmp_path
):
    import sqlite3

    from src.transfer.endpoint_intelligence import introspect_endpoint
    from src.transfer.models import EndpointConfig

    db_path = tmp_path / "S3NT1NEL-introspect.db"
    db = sqlite3.connect(db_path)
    db.execute('CREATE TABLE "S3NT1NEL_table" ("S3NT1NEL_column" TEXT)')
    db.execute('INSERT INTO "S3NT1NEL_table" VALUES (?)', (_SENTINELS[5],))
    db.commit()
    db.close()
    endpoint = EndpointConfig(
        kind="database",
        format="sqlite",
        connection_string=f"sqlite:///{db_path}",
        extra={"introspect_purpose": "source;S3NT1NEL"},
    )
    result = introspect_endpoint(endpoint)
    assert result["connected"] is True
    record = _assert_terminal(
        event_capture,
        "introspect",
        "succeeded",
        logging.INFO,
    )
    assert record.object_count >= 1
    _assert_no_sentinels(event_capture, str(db_path))

    event_capture.records.clear()
    missing = EndpointConfig(
        kind="database",
        format="sqlite",
        connection_string=f"sqlite:///{tmp_path / 'nope' / 'S3NT1NEL-missing.db'}",
    )
    result = introspect_endpoint(missing)
    assert result["connected"] is False
    record = _assert_terminal(
        event_capture,
        "introspect",
        "failed",
        logging.WARNING,
    )
    assert record.category == "not_connected"
    _assert_no_sentinels(event_capture)

    event_capture.records.clear()
    file_result = introspect_endpoint(EndpointConfig(kind="file", format="csv"))
    assert file_result["connected"] is False
    record = _assert_terminal(
        event_capture,
        "introspect",
        "skipped",
        logging.INFO,
    )
    assert record.category == "no_input"
    _assert_no_sentinels(event_capture)


def test_saas_request_emits_retries_and_exhaustion(
    monkeypatch, event_capture
):
    import requests

    from connectors import saas_common
    from services.error_handling import RetryBudget

    responses = [503, 503, 200]

    def fake_request(*args, **kwargs):
        response = requests.Response()
        response.status_code = responses.pop(0)
        response.reason = "unavailable"
        response.url = "https://S3NT1NEL.host.invalid"
        response._content = b"{}"
        return response

    monkeypatch.setattr(saas_common.requests, "request", fake_request)
    result = saas_common.request(
        method="GET",
        url="https://S3NT1NEL.host.invalid",
        retry_budget=RetryBudget(
            max_attempts=3,
            base_delay_seconds=0,
            jitter=False,
        ),
        sleep=lambda _delay: None,
    )
    assert result.status_code == 200
    events = [
        record
        for record in event_capture.records
        if getattr(record, "event", "").startswith("connector.retry.")
    ]
    assert [record.event for record in events] == [
        "connector.retry.scheduled",
        "connector.retry.scheduled",
    ]
    assert [record.attempt for record in events] == [1, 2]
    assert all(record.http_status == 503 for record in events)
    _assert_no_sentinels(event_capture)

    event_capture.records.clear()
    monkeypatch.setattr(
        saas_common.requests,
        "request",
        lambda *args, **kwargs: fake_request(*args, **kwargs),
    )
    responses.extend([503, 503])
    with pytest.raises(requests.HTTPError):
        saas_common.request(
            method="GET",
            url="https://S3NT1NEL.host.invalid",
            retry_budget=RetryBudget(
                max_attempts=2,
                base_delay_seconds=0,
                jitter=False,
            ),
            sleep=lambda _delay: None,
        )
    events = [
        record
        for record in event_capture.records
        if getattr(record, "event", "").startswith("connector.retry.")
    ]
    assert [record.event for record in events] == [
        "connector.retry.scheduled",
        "connector.retry.exhausted",
    ]
    assert [record.outcome for record in events] == ["retrying", "failed"]
    assert [record.attempt for record in events] == [1, 2]
    _assert_no_sentinels(event_capture)


def test_http_requester_emits_retry_fields_with_fixture_server(event_capture):
    from connectors.sdk.declarative.requester import HttpRequester
    from tests.connector_certification.fixture_server import (
        FixtureResponse,
        FixtureServer,
    )

    with FixtureServer() as fixture:
        fixture.add_route(
            "/retry",
            responses=[
                FixtureResponse(status=503, body={"error": "temporary"}),
                FixtureResponse(status=503, body={"error": "temporary"}),
                FixtureResponse(status=200, body={"ok": True}),
            ],
        )
        requester = HttpRequester(
            max_attempts=3,
            base_delay_seconds=0,
            jitter=False,
            sleep=lambda _delay: None,
        )
        result = requester.request("GET", f"{fixture.base_url}/retry")
        assert result.response.status_code == 200

        fixture.add_route(
            "/exhaust",
            responses=[
                FixtureResponse(status=503, body={"error": "temporary"}),
                FixtureResponse(status=503, body={"error": "temporary"}),
            ],
        )
        exhausted = HttpRequester(
            max_attempts=2,
            base_delay_seconds=0,
            jitter=False,
            sleep=lambda _delay: None,
        )
        with pytest.raises(Exception):
            exhausted.request("GET", f"{fixture.base_url}/exhaust")

    events = [
        record
        for record in event_capture.records
        if getattr(record, "event", "").startswith("connector.retry.")
    ]
    assert [record.outcome for record in events] == [
        "retrying",
        "retrying",
        "retrying",
        "failed",
    ]
    assert [record.attempt for record in events] == [1, 2, 1, 2]
    assert all(record.http_status == 503 for record in events)
    assert all(record.request_id for record in events)
    _assert_no_sentinels(event_capture)


def test_stream_read_batch_passes_retry_telemetry(monkeypatch, event_capture):
    import requests

    from services.error_handling import RetryBudget
    from src.transfer import stream

    result = object()
    attempts = 0

    def fake_read(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise requests.ConnectionError(
                "temporarily unavailable S3NT1NEL.host SELECT "
                "'S3NT1NEL-row-value' /tmp/S3NT1NEL-file"
            )
        return result

    original_with_retry = stream.with_retry
    on_transient_callbacks = []

    def capture_retry(fn, *, budget=None, on_transient=None, **kwargs):
        on_transient_callbacks.append(on_transient)
        return original_with_retry(
            fn,
            budget=budget,
            on_transient=on_transient,
            **kwargs,
        )

    monkeypatch.setattr(stream, "_read_batch_impl", fake_read)
    monkeypatch.setattr(stream, "with_retry", capture_retry)
    monkeypatch.setattr(
        stream,
        "RetryBudget",
        lambda **kwargs: RetryBudget(
            max_attempts=2,
            base_delay_seconds=0,
            jitter=False,
        ),
    )
    assert stream._read_batch("postgresql") is result
    events = [
        record
        for record in event_capture.records
        if getattr(record, "event", "").startswith("connector.retry.")
    ]
    assert [record.event for record in events] == ["connector.retry.scheduled"]
    assert events[0].driver_family == "postgresql"
    assert attempts == 2
    assert on_transient_callbacks[0] is not None
    _assert_no_sentinels(event_capture, "S3NT1NEL.host", "S3NT1NEL-file")


def test_engine_source_read_passes_retry_callback(
    monkeypatch, event_capture, tmp_path
):
    import sqlite3

    from tests.test_engine_streaming_sqlite import _FakeMongo
    from src.transfer import engine as engine_module
    from src.transfer.models import EndpointConfig, TransferRequest

    src = tmp_path / "source.db"
    dst = tmp_path / "destination.db"
    connection = sqlite3.connect(src)
    connection.execute("CREATE TABLE source_table (id INTEGER PRIMARY KEY, value TEXT)")
    connection.execute("INSERT INTO source_table VALUES (1, 'S3NT1NEL-row-value')")
    connection.commit()
    connection.close()

    fake_mongo = _FakeMongo()
    monkeypatch.setattr(engine_module, "get_mongodb_service", lambda: fake_mongo)
    original_with_retry = engine_module.with_retry
    callbacks = []

    def capture_retry(fn, *, budget=None, on_transient=None, **kwargs):
        callbacks.append((budget, on_transient))
        return original_with_retry(
            fn,
            budget=budget,
            on_transient=on_transient,
            **kwargs,
        )

    monkeypatch.setattr(engine_module, "with_retry", capture_retry)
    request = TransferRequest(
        source=EndpointConfig(
            kind="database",
            format="sqlite",
            database=str(src),
            table="source_table",
        ),
        destination=EndpointConfig(
            kind="database",
            format="sqlite",
            database=str(dst),
            table="destination_table",
        ),
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        priority_column="id",
    )
    result = engine_module.UniversalTransferEngine().execute_tracked(
        request,
        "000000000000000000000001",
    )
    assert result.success is True, result.error
    read_calls = [
        (budget, callback)
        for budget, callback in callbacks
        if callback is not None
    ]
    assert read_calls
    budget, callback = read_calls[0]
    assert callable(callback)
    assert budget.max_attempts == 3
    assert budget.base_delay_seconds == 0.5
    assert budget.max_delay_seconds == 5.0
    _assert_no_sentinels(event_capture, str(src), str(dst))
