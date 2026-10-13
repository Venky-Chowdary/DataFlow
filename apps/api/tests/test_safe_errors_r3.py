from __future__ import annotations

import asyncio
import json
import logging
import uuid
from types import SimpleNamespace

import pytest
from fastapi import HTTPException, Request
from fastapi.testclient import TestClient

from services.logging_config import ContextFilter, JsonFormatter, TextFormatter
from src.routers import transfer_router

_SENTINEL = (
    "password=S3NT1NEL-pw postgresql://u:S3NT1NEL-dsn@h/db "
    "row=S3NT1NEL-row SELECT * FROM S3NT1NEL_tbl"
)
_TRACEBACK_SECRET = "password=S3NT1NEL-pw postgresql://u:S3NT1NEL-dsn@h/db"
_JSON_EXECUTE = {
    "source": {
        "kind": "database",
        "format": "sqlite",
        "database": "source",
        "table": "source_table",
    },
    "destination": {
        "kind": "database",
        "format": "sqlite",
        "database": "destination",
        "table": "destination_table",
    },
    "skip_preflight": True,
    "async_mode": False,
    "enforce_contract": False,
}
_FORM_EXECUTE = {
    "source_kind": "database",
    "source_format": "sqlite",
    "source_database": "source",
    "source_table": "source_table",
    "dest_kind": "database",
    "dest_format": "sqlite",
    "dest_database": "destination",
    "dest_table": "destination_table",
    "skip_preflight": "true",
    "async_mode": "false",
    "mappings_json": "[]",
}


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
def error_capture():
    logger = logging.getLogger("dataflow.errors")
    old_level = logger.level
    old_propagate = logger.propagate
    capture = _Capture()
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    logger.addHandler(capture)
    try:
        yield capture
    finally:
        logger.removeHandler(capture)
        logger.setLevel(old_level)
        logger.propagate = old_propagate


@pytest.fixture
def api_trace_capture():
    logger = logging.getLogger("dataflow.api")
    old_level = logger.level
    old_propagate = logger.propagate
    capture = _Capture()
    logger.setLevel(logging.ERROR)
    logger.propagate = False
    logger.addHandler(capture)
    try:
        yield capture
    finally:
        logger.removeHandler(capture)
        logger.setLevel(old_level)
        logger.propagate = old_propagate


@pytest.fixture
def error_client():
    from src.main import app

    return TestClient(app, raise_server_exceptions=False)


def _raise(error_type: type[Exception] = ValueError):
    def fail(*_args, **_kwargs):
        raise error_type(_SENTINEL)

    return fail


def _assert_public_response(
    response,
    capture: _Capture,
    *,
    event: str,
    status_code: int,
) -> dict:
    body = response.json()
    serialized = json.dumps(body, sort_keys=True)
    rendered = capture.rendered()
    assert response.status_code == status_code
    assert "S3NT1NEL" not in serialized
    assert "S3NT1NEL" not in response.text
    assert "S3NT1NEL" not in rendered
    cid = response.headers["X-Correlation-ID"]
    assert cid in serialized
    records = [
        record
        for record in capture.records
        if getattr(record, "event", None) == event
        and getattr(record, "correlation_id", None) == cid
    ]
    assert records, f"missing {event} event for correlation id {cid}"
    assert records[-1].outcome == "errored"
    assert records[-1].error_type in {"RuntimeError", "ValueError"}
    return body


def test_public_error_detail_event_and_status_level(monkeypatch, error_capture):
    from services import safe_errors, tracing

    monkeypatch.delenv(safe_errors.EXPOSE_ENV, raising=False)
    monkeypatch.setattr(tracing, "get_correlation_id", lambda: "unit-safe-error")
    detail, cid = safe_errors.public_error(
        RuntimeError(_SENTINEL),
        event="transfer.unit.errored",
        public_message="Request failed.",
        status_code=500,
    )

    assert detail == "Request failed. Reference: unit-safe-error"
    assert cid == "unit-safe-error"
    record = next(
        record
        for record in error_capture.records
        if getattr(record, "event", None) == "transfer.unit.errored"
    )
    assert record.outcome == "errored"
    assert record.error_type == "RuntimeError"
    assert record.correlation_id == cid
    assert record.levelno == logging.ERROR
    assert "S3NT1NEL" not in error_capture.rendered()


def test_public_error_uses_warning_for_client_errors(monkeypatch, error_capture):
    from services import safe_errors, tracing

    monkeypatch.setattr(tracing, "get_correlation_id", lambda: "unit-client-error")
    detail, cid = safe_errors.public_error(
        ValueError(_SENTINEL),
        event="transfer.unit.invalid",
        public_message="Invalid request.",
        status_code=400,
    )

    assert detail == "Invalid request. Reference: unit-client-error"
    assert cid == "unit-client-error"
    record = next(
        record
        for record in error_capture.records
        if getattr(record, "event", None) == "transfer.unit.invalid"
    )
    assert record.levelno == logging.WARNING
    assert record.error_type == "ValueError"


def test_public_error_flag_adds_only_redacted_detail(monkeypatch, error_capture):
    from services import safe_errors, tracing

    monkeypatch.setenv(safe_errors.EXPOSE_ENV, "1")
    monkeypatch.setattr(tracing, "get_correlation_id", lambda: "unit-debug-error")
    detail, cid = safe_errors.public_error(
        RuntimeError(_SENTINEL),
        event="transfer.unit.debug",
        public_message="Request failed.",
    )

    assert cid == "unit-debug-error"
    assert detail.startswith("Request failed. Reference: unit-debug-error Detail: ")
    assert "S3NT1NEL-pw" not in detail
    assert "S3NT1NEL-dsn" not in detail
    assert "password=***" in detail
    assert "postgresql://u:***@h/db" in detail
    assert "S3NT1NEL" not in error_capture.rendered()


def test_public_error_generates_correlation_id_when_context_is_blank(monkeypatch):
    from services import safe_errors, tracing

    monkeypatch.setattr(tracing, "get_correlation_id", lambda: "")
    detail, cid = safe_errors.public_error(
        RuntimeError("failure"),
        event="transfer.unit.errored",
        public_message="Request failed.",
    )

    assert cid
    assert detail == f"Request failed. Reference: {cid}"


def test_safe_http_exception_preserves_status_and_correlation_header(monkeypatch):
    from services import safe_errors, tracing

    monkeypatch.setattr(tracing, "get_correlation_id", lambda: "unit-http-error")
    error = safe_errors.safe_http_exception(
        ValueError(_SENTINEL),
        event="transfer.unit.invalid",
        public_message="Invalid request.",
        status_code=422,
    )

    assert isinstance(error, HTTPException)
    assert error.status_code == 422
    assert error.detail == "Invalid request. Reference: unit-http-error"
    assert error.headers == {"X-Correlation-ID": "unit-http-error"}


@pytest.mark.parametrize("production", [False, True])
def test_middleware_unhandled_error_is_safe_and_independent_of_environment(
    error_client, error_capture, monkeypatch, production
):
    from src import main

    monkeypatch.delenv("DATAFLOW_EXPOSE_ERROR_DETAIL", raising=False)
    monkeypatch.setattr(main, "is_production", lambda: production)
    path = "/__safe-errors-r3/{item}"

    async def fail(item: str):
        raise RuntimeError(_SENTINEL)

    main.app.add_api_route(path, fail, methods=["GET"])
    try:
        response = error_client.get(
            path.replace("{item}", "S3NT1NEL-row"),
            headers={"X-Correlation-ID": uuid.uuid4().hex},
        )
    finally:
        main.app.router.routes[:] = [
            route for route in main.app.router.routes if getattr(route, "path", None) != path
        ]

    body = _assert_public_response(
        response,
        error_capture,
        event="http.request.unhandled",
        status_code=500,
    )
    cid = response.headers["X-Correlation-ID"]
    assert body["error"] == "Internal Server Error"
    assert body["detail"] == f"An unexpected error occurred. Reference: {cid}"
    assert body["correlation_id"] == cid


@pytest.mark.parametrize("production", [False, True])
def test_middleware_debug_detail_is_redacted_not_environment_gated(
    error_client, error_capture, monkeypatch, production
):
    from src import main

    monkeypatch.setenv("DATAFLOW_EXPOSE_ERROR_DETAIL", "1")
    monkeypatch.setattr(main, "is_production", lambda: production)
    path = "/__safe-errors-r3-debug/{item}"

    async def fail(item: str):
        raise RuntimeError(_SENTINEL)

    main.app.add_api_route(path, fail, methods=["GET"])
    try:
        response = error_client.get(
            path.replace("{item}", "S3NT1NEL-row"),
            headers={"X-Correlation-ID": uuid.uuid4().hex},
        )
    finally:
        main.app.router.routes[:] = [
            route for route in main.app.router.routes if getattr(route, "path", None) != path
        ]

    body = response.json()
    cid = response.headers["X-Correlation-ID"]
    assert response.status_code == 500
    assert body["correlation_id"] == cid
    assert body["detail"].startswith(
        f"An unexpected error occurred. Reference: {cid} Detail: "
    )
    assert "S3NT1NEL-pw" not in body["detail"]
    assert "S3NT1NEL-dsn" not in body["detail"]
    assert "password=***" in body["detail"]
    assert "postgresql://u:***@h/db" in body["detail"]
    assert "S3NT1NEL" not in error_capture.rendered()
    assert any(
        getattr(record, "event", None) == "http.request.unhandled"
        and getattr(record, "correlation_id", None) == cid
        for record in error_capture.records
    )


def test_global_exception_handler_uses_safe_errors_directly(
    error_capture, monkeypatch
):
    from services import tracing
    from src import main

    monkeypatch.delenv("DATAFLOW_EXPOSE_ERROR_DETAIL", raising=False)
    cid = "direct-global-r3"
    tracing.set_correlation_id(cid)
    path = "/direct-global/S3NT1NEL-row"
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "headers": [],
        "server": ("test", 80),
        "client": ("test", 1234),
        "root_path": "",
        "route": SimpleNamespace(path="/direct-global/{item}"),
    }
    try:
        response = asyncio.run(
            main.global_exception_handler(Request(scope), RuntimeError(_SENTINEL))
        )
    finally:
        tracing.set_correlation_id("")

    body = json.loads(response.body)
    assert response.status_code == 500
    assert body["detail"] == f"An unexpected error occurred. Reference: {cid}"
    assert body["correlation_id"] == cid
    assert response.headers["X-Correlation-ID"] == cid
    assert "S3NT1NEL" not in response.body.decode()
    assert "S3NT1NEL" not in error_capture.rendered()


@pytest.mark.parametrize(
    ("method", "path", "body"),
    [
        ("GET", "/api/v1/transfer/r3-job/explanation", None),
        ("GET", "/api/v1/transfer/r3-job/mapping-proof", None),
        ("GET", "/api/v1/transfer/r3-job/proof-pack", None),
        ("GET", "/api/v1/transfer/r3-job/certificate", None),
        (
            "POST",
            "/api/v1/transfer/r3-job/rollback/execute",
            {"approved_by": "operator", "reason": "approved rollback"},
        ),
        ("GET", "/api/v1/transfer/r3-job/cdc/snapshots", None),
        (
            "POST",
            "/api/v1/transfer/r3-job/cdc/snapshots",
            {"table": "orders", "primary_key": "id"},
        ),
        ("POST", "/api/v1/transfer/r3-job/cdc/snapshots/r3-signal/cancel", None),
    ],
)
def test_job_lookup_errors_are_safe(
    error_client, error_capture, monkeypatch, method, path, body
):
    from src.services import mongodb_service

    class FailingMongo:
        def get_job(self, *_args, **_kwargs):
            raise RuntimeError(_SENTINEL)

    monkeypatch.setattr(mongodb_service, "get_mongodb_service", lambda: FailingMongo())
    response = error_client.request(
        method,
        path,
        json=body,
        headers={"X-Correlation-ID": uuid.uuid4().hex},
    )

    _assert_public_response(
        response,
        error_capture,
        event="transfer.job_lookup.errored",
        status_code=500,
    )


def test_job_creation_500_errors_are_safe(
    error_client, error_capture, monkeypatch
):
    from src.transfer import engine

    class FailingEngine:
        def _create_pending_job(self, _request):
            raise RuntimeError(_SENTINEL)

    monkeypatch.setattr(engine, "get_transfer_engine", lambda: FailingEngine())
    response = error_client.post(
        "/api/v1/transfer/execute",
        json=_JSON_EXECUTE,
        headers={"X-Correlation-ID": uuid.uuid4().hex},
    )

    _assert_public_response(
        response,
        error_capture,
        event="transfer.job_create.errored",
        status_code=500,
    )


def test_multipart_job_creation_500_errors_are_safe(
    error_client, error_capture, monkeypatch
):
    from src.transfer import engine

    class FailingEngine:
        def _create_pending_job(self, _request):
            raise RuntimeError(_SENTINEL)

    monkeypatch.setattr(engine, "get_transfer_engine", lambda: FailingEngine())
    response = error_client.post(
        "/api/v1/transfer/run",
        data=_FORM_EXECUTE,
        headers={"X-Correlation-ID": uuid.uuid4().hex},
    )

    _assert_public_response(
        response,
        error_capture,
        event="transfer.job_create.errored",
        status_code=500,
    )


def test_shape_recipe_json_error_is_safe(
    error_client, error_capture, monkeypatch
):
    original_loads = transfer_router.json.loads

    def fail_recipe(value, *args, **kwargs):
        if value == "S3NT1NEL-invalid-json":
            raise ValueError(_SENTINEL)
        return original_loads(value, *args, **kwargs)

    monkeypatch.setattr(transfer_router.json, "loads", fail_recipe)
    payload = {**_FORM_EXECUTE, "shape_recipe_json": "S3NT1NEL-invalid-json"}
    response = error_client.post(
        "/api/v1/transfer/run",
        data=payload,
        headers={"X-Correlation-ID": uuid.uuid4().hex},
    )

    _assert_public_response(
        response,
        error_capture,
        event="transfer.shape_recipe.invalid",
        status_code=400,
    )


def test_file_sample_parse_error_is_safe(
    error_client, error_capture, monkeypatch
):
    from src.transfer import adapters

    monkeypatch.setattr(adapters, "parse_file_route_sample", _raise())
    response = error_client.post(
        "/api/v1/transfer/analyze-file",
        files={"file": ("sample.csv", b"col\nvalue\n", "text/csv")},
        headers={"X-Correlation-ID": uuid.uuid4().hex},
    )

    _assert_public_response(
        response,
        error_capture,
        event="transfer.file_sample.invalid",
        status_code=400,
    )


_PLAN_ERROR_CASES = [
    ("plan_update", 404, "transfer.plan_update.errored"),
    ("plan_mappings", 404, "transfer.plan_mappings.errored"),
    ("plan_map", 404, "transfer.plan_map.errored"),
    ("plan_preflight", 400, "transfer.plan_preflight.errored"),
    ("plan_approve", 404, "transfer.plan_approve.errored"),
    ("plan_run_payload", 400, "transfer.plan_run_payload.errored"),
    ("json_plan_merge", 400, "transfer.plan_execute.errored"),
    ("json_stamp_contract", 400, "transfer.plan_execute.errored"),
    ("json_create_pending", 400, "transfer.job_create.invalid"),
    ("form_plan_merge", 400, "transfer.plan_execute.errored"),
    ("form_stamp_contract", 400, "transfer.plan_execute.errored"),
    ("form_create_pending", 400, "transfer.job_create.invalid"),
    ("cdc_list_context", 400, "transfer.cdc_snapshot.list.invalid"),
    ("cdc_list_signals", 400, "transfer.cdc_snapshot.list.invalid"),
    ("cdc_snapshot_request", 400, "transfer.cdc_snapshot.request.invalid"),
    ("cdc_cancel_context", 400, "transfer.cdc_snapshot.cancel.invalid"),
]


@pytest.mark.parametrize(
    ("case", "status_code", "event"),
    _PLAN_ERROR_CASES,
    ids=[case[0] for case in _PLAN_ERROR_CASES],
)
def test_bare_value_error_routes_are_safe(
    error_client,
    error_capture,
    monkeypatch,
    case,
    status_code,
    event,
):
    from services import cdc_job_snapshots, transfer_plan_service
    from src.services import mongodb_service
    from src.transfer import contract_engine, engine

    headers = {"X-Correlation-ID": uuid.uuid4().hex}
    method = "GET"
    path = ""
    request_kwargs: dict = {"headers": headers}
    if case == "plan_update":
        monkeypatch.setattr(transfer_plan_service, "patch_plan", _raise())
        method, path = "PATCH", "/api/v1/transfer/plans/r3-plan"
        request_kwargs["json"] = {"name": "updated"}
    elif case == "plan_mappings":
        monkeypatch.setattr(transfer_plan_service, "sync_plan_mappings", _raise())
        method, path = "POST", "/api/v1/transfer/plans/r3-plan/mappings"
        request_kwargs["json"] = {"mappings": []}
    elif case == "plan_map":
        monkeypatch.setattr(transfer_plan_service, "run_plan_mapping", _raise())
        method, path = "POST", "/api/v1/transfer/plans/r3-plan/map"
        request_kwargs["json"] = {}
    elif case == "plan_preflight":
        monkeypatch.setattr(transfer_plan_service, "run_plan_preflight", _raise())
        method, path = "POST", "/api/v1/transfer/plans/r3-plan/preflight"
        request_kwargs["json"] = {}
    elif case == "plan_approve":
        monkeypatch.setattr(transfer_plan_service, "approve_plan", _raise())
        method, path = "POST", "/api/v1/transfer/plans/r3-plan/approve"
    elif case == "plan_run_payload":
        monkeypatch.setattr(transfer_plan_service, "build_run_payload", _raise())
        method, path = "GET", "/api/v1/transfer/plans/r3-plan/run-payload"
    elif case == "json_plan_merge":
        monkeypatch.setattr(transfer_plan_service, "merge_plan_into_run", _raise())
        method, path = "POST", "/api/v1/transfer/execute"
        request_kwargs["json"] = {**_JSON_EXECUTE, "plan_id": "r3-plan"}
    elif case == "json_stamp_contract":
        monkeypatch.setattr(contract_engine, "stamp_request_contract", _raise())
        method, path = "POST", "/api/v1/transfer/execute"
        request_kwargs["json"] = _JSON_EXECUTE
    elif case == "json_create_pending":
        class FailingEngine:
            def _create_pending_job(self, _request):
                raise ValueError(_SENTINEL)

        monkeypatch.setattr(engine, "get_transfer_engine", lambda: FailingEngine())
        method, path = "POST", "/api/v1/transfer/execute"
        request_kwargs["json"] = _JSON_EXECUTE
    elif case == "form_plan_merge":
        monkeypatch.setattr(transfer_plan_service, "merge_plan_into_run", _raise())
        method, path = "POST", "/api/v1/transfer/run"
        request_kwargs["data"] = {**_FORM_EXECUTE, "plan_id": "r3-plan"}
    elif case == "form_stamp_contract":
        monkeypatch.setattr(contract_engine, "stamp_request_contract", _raise())
        method, path = "POST", "/api/v1/transfer/run"
        request_kwargs["data"] = _FORM_EXECUTE
    elif case == "form_create_pending":
        class FailingEngine:
            def _create_pending_job(self, _request):
                raise ValueError(_SENTINEL)

        monkeypatch.setattr(engine, "get_transfer_engine", lambda: FailingEngine())
        method, path = "POST", "/api/v1/transfer/run"
        request_kwargs["data"] = _FORM_EXECUTE
    elif case == "cdc_list_context":
        monkeypatch.setattr(cdc_job_snapshots, "resolve_job_cdc_snapshot_context", _raise())
        method, path = "GET", "/api/v1/transfer/r3-job/cdc/snapshots"
        monkeypatch.setattr(
            mongodb_service,
            "get_mongodb_service",
            lambda: SimpleNamespace(get_job=lambda *_a, **_k: {"exists": True}),
        )
    elif case == "cdc_list_signals":
        monkeypatch.setattr(
            cdc_job_snapshots,
            "resolve_job_cdc_snapshot_context",
            lambda *_a, **_k: {"source_keys": [], "source_key": "", "table": "", "primary_key": ""},
        )
        monkeypatch.setattr(cdc_job_snapshots, "list_signals_for_job", _raise())
        method, path = "GET", "/api/v1/transfer/r3-job/cdc/snapshots"
        monkeypatch.setattr(
            mongodb_service,
            "get_mongodb_service",
            lambda: SimpleNamespace(get_job=lambda *_a, **_k: {"exists": True}),
        )
    elif case == "cdc_snapshot_request":
        monkeypatch.setattr(cdc_job_snapshots, "request_snapshot_for_job", _raise())
        method, path = "POST", "/api/v1/transfer/r3-job/cdc/snapshots"
        request_kwargs["json"] = {}
        monkeypatch.setattr(
            mongodb_service,
            "get_mongodb_service",
            lambda: SimpleNamespace(get_job=lambda *_a, **_k: {"exists": True}),
        )
    elif case == "cdc_cancel_context":
        monkeypatch.setattr(cdc_job_snapshots, "resolve_job_cdc_snapshot_context", _raise())
        method, path = "POST", "/api/v1/transfer/r3-job/cdc/snapshots/r3-signal/cancel"
        monkeypatch.setattr(
            mongodb_service,
            "get_mongodb_service",
            lambda: SimpleNamespace(get_job=lambda *_a, **_k: {"exists": True}),
        )

    response = error_client.request(method, path, **request_kwargs)
    _assert_public_response(
        response,
        error_capture,
        event=event,
        status_code=status_code,
    )


def test_product_authored_error_text_is_kept_with_credentials_redacted(monkeypatch):
    from services.read_options import ReadOptionsError

    def fail(_raw):
        raise ReadOptionsError(
            "encoding option was refused for postgresql://operator:S3NT1NEL-dsn@db.example:5432/app"
        )

    monkeypatch.setattr("services.read_options.parse_read_options_payload", fail)
    with pytest.raises(HTTPException) as caught:
        transfer_router._validated_read_options(None)

    assert "encoding option was refused" in caught.value.detail
    assert "postgresql://operator:***@db.example:5432/app" in caught.value.detail
    assert "S3NT1NEL-dsn" not in caught.value.detail


def test_global_handler_request_scope_uses_route_template(error_capture, monkeypatch):
    from services import tracing
    from src import main

    monkeypatch.delenv("DATAFLOW_EXPOSE_ERROR_DETAIL", raising=False)
    cid = "global-template-r3"
    tracing.set_correlation_id(cid)
    path = "/global/S3NT1NEL-row"
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "headers": [],
        "server": ("test", 80),
        "client": ("test", 1234),
        "root_path": "",
        "route": SimpleNamespace(path="/global/{item}"),
    }
    try:
        response = asyncio.run(
            main.global_exception_handler(Request(scope), RuntimeError(_SENTINEL))
        )
    finally:
        tracing.set_correlation_id("")

    record = next(
        record
        for record in error_capture.records
        if getattr(record, "event", None) == "http.request.unhandled"
    )
    assert record.correlation_id == cid
    assert record.method == "POST"
    assert record.route_template == "/global/{item}"
    assert "S3NT1NEL" not in error_capture.rendered()
    assert "S3NT1NEL" not in response.body.decode()


def test_middleware_logs_redacted_traceback_without_changing_response(
    error_client, error_capture, api_trace_capture
):
    from src import main

    cid = "middleware-trace-r4"
    path = "/__safe-errors-r4-traceback"

    async def _raise_r4_middleware_error():
        raise RuntimeError(_TRACEBACK_SECRET)

    main.app.add_api_route(path, _raise_r4_middleware_error, methods=["GET"])
    try:
        response = error_client.get(path, headers={"X-Correlation-ID": cid})
    finally:
        main.app.router.routes[:] = [
            route
            for route in main.app.router.routes
            if getattr(route, "path", None) != path
        ]

    expected = {
        "error": "Internal Server Error",
        "detail": f"An unexpected error occurred. Reference: {cid}",
        "correlation_id": cid,
    }
    assert response.status_code == 500
    assert response.content == json.dumps(
        expected, separators=(",", ":")
    ).encode()
    assert response.headers["X-Correlation-ID"] == cid
    assert "S3NT1NEL-pw" not in response.text
    assert "S3NT1NEL-dsn" not in response.text
    trace_records = [
        record
        for record in api_trace_capture.records
        if getattr(record, "event", None) == "http.request.unhandled.traceback"
    ]
    assert trace_records
    assert trace_records[-1].correlation_id == cid
    rendered = api_trace_capture.rendered()
    assert "Traceback" in rendered
    assert "_raise_r4_middleware_error" in rendered
    assert "S3NT1NEL-pw" not in rendered
    assert "S3NT1NEL-dsn" not in rendered
    assert "S3NT1NEL" not in error_capture.rendered()


def test_global_handler_logs_redacted_traceback_without_changing_response(
    error_capture, api_trace_capture, monkeypatch
):
    from services import tracing
    from src import main

    monkeypatch.delenv("DATAFLOW_EXPOSE_ERROR_DETAIL", raising=False)
    cid = "global-trace-r4"
    tracing.set_correlation_id(cid)
    path = "/global-trace/S3NT1NEL-row"
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "headers": [],
        "server": ("test", 80),
        "client": ("test", 1234),
        "root_path": "",
        "route": SimpleNamespace(path="/global-trace/{item}"),
    }

    def _raise_r4_global_error():
        raise RuntimeError(_TRACEBACK_SECRET)

    try:
        try:
            _raise_r4_global_error()
        except RuntimeError as exc:
            response = asyncio.run(
                main.global_exception_handler(Request(scope), exc)
            )
    finally:
        tracing.set_correlation_id("")

    expected = {
        "error": "Internal Server Error",
        "detail": f"An unexpected error occurred. Reference: {cid}",
        "correlation_id": cid,
    }
    assert response.status_code == 500
    assert response.body == json.dumps(
        expected, separators=(",", ":")
    ).encode()
    assert response.headers["X-Correlation-ID"] == cid
    trace_records = [
        record
        for record in api_trace_capture.records
        if getattr(record, "event", None) == "http.request.unhandled.traceback"
    ]
    assert trace_records
    assert trace_records[-1].correlation_id == cid
    rendered = api_trace_capture.rendered()
    assert "Traceback" in rendered
    assert "_raise_r4_global_error" in rendered
    assert "S3NT1NEL-pw" not in rendered
    assert "S3NT1NEL-dsn" not in rendered
    assert "S3NT1NEL" not in error_capture.rendered()
