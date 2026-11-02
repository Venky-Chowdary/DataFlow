from __future__ import annotations

import json
import logging
import sys
from uuid import uuid4

import pytest

from services import (
    audit_log,
    evidence_chain,
    log_redaction,
    otel_logging,
    secret_config,
    tracing,
)
from services.logging_config import JsonFormatter, TextFormatter


SENSITIVE_KEYS = (
    "client_secret",
    "access_token",
    "refresh_token",
    "id_token",
    "scim_token",
    "SAMLResponse",
    "cookie",
    "set-cookie",
    "Set-Cookie",
    "password_hash",
    "apiKey",
    "api_key",
    "connection_string",
    "dsn",
    "account_key",
    "AccountKey",
    "aws_secret_access_key",
    "pwd",
    "sig",
    "X-Amz-Signature",
    "X-Amz-Security-Token",
    "authorization",
    "Authorization",
    "webhook_url",
    "secret_value",
    "code_verifier",
    "service_account_json",
    "serviceAccountKey",
)

TEXT_CASES = (
    ("postgresql URI credentials", "postgresql://u:{sentinel}@db:5432/x"),
    (
        "Mongo SRV URI credentials",
        "mongodb+srv://u:{sentinel}@cluster0.ab.mongodb.net/db",
    ),
    ("Redis URI credentials with empty user", "redis://:{sentinel}@h:6379"),
    ("ODBC password", "Server=x;Uid=u;Pwd={sentinel};"),
    (
        "Azure account key",
        "DefaultEndpointsProtocol=https;AccountName=a;AccountKey={sentinel}==;"
        "EndpointSuffix=core.windows.net",
    ),
    (
        "S3 presigned signature",
        "https://s3.example.test/x?X-Amz-Signature={sentinel}&X-Amz-Expires=60",
    ),
    ("Azure SAS signature", "https://storage.example.test/x?sv=2022&sig={sentinel}"),
    (
        "OIDC callback code",
        "GET /api/v1/auth/sso/oidc/callback?code={sentinel}&state=abc HTTP/1.1",
    ),
    ("Bearer authorization", "Authorization: Bearer {sentinel}"),
    ("Basic authorization", "Authorization: Basic {sentinel}"),
    ("DataFlow API key", "dfk_{sentinel}"),
    ("client secret assignment", "client_secret={sentinel}"),
    ("access token assignment", "access_token={sentinel}"),
    ("quoted refresh token assignment", '"refresh_token": "{sentinel}"'),
    ("SAML response assignment", "SAMLResponse={sentinel}"),
    ("Cookie assignment", "Cookie: session={sentinel}"),
    ("Set-Cookie assignment", "Set-Cookie: df_session={sentinel}; HttpOnly"),
    ("AWS secret key assignment", "aws_secret_access_key = {sentinel}"),
    (
        "JWT-shaped token",
        "eyJabcdefgh.{sentinel}abcdefgh.abcdefgh",
    ),
    (
        "Slack webhook",
        "https://hooks.slack.com/services/T00000000/B00000000/{sentinel}",
    ),
    (
        "Teams webhook",
        "https://acme.webhook.office.com/webhookb2/{sentinel}",
    ),
    (
        "Discord webhook",
        "https://discord.com/api/webhooks/12345/{sentinel}",
    ),
)

SAFE_METADATA = {
    "token_count": 1234,
    "prompt_tokens": 50,
    "request_count": 3,
    "duration_ms": 12.5,
    "api_key_id": "key-abc123",
    "service_account_id": "sa-123",
    "service_account_email": "etl@example.test",
    "token_type": "Bearer",
    "secret_name": "prod-db",
    "has_token": True,
    "workspace_id": "ws-1",
    "status": "Login failed — unknown email",
}

SAFE_TEXT = (
    "Processed 1234 rows in 56 ms (token_count=900, request_count=3)",
    "/api/v1/workspace/api-keys",
    "https://example.com/path?page=2",
)


def _sentinel() -> str:
    return f"SENTINEL{uuid4().hex}"


def _audit_details(sentinel: str) -> dict[str, object]:
    key_values = {key: sentinel for key in SENSITIVE_KEYS}
    text_values = [
        template.format(sentinel=sentinel) for _, template in TEXT_CASES
    ]
    return {
        "key_values": key_values,
        "nested": {"key_values": key_values},
        "rows": [{"key_values": key_values}],
        "text_values": text_values,
    }


def _record_with_sentinels() -> tuple[logging.LogRecord, tuple[str, ...]]:
    message_secret = _sentinel()
    extra_secret = _sentinel()
    exception_secret = _sentinel()
    nested_extra_secret = _sentinel()
    stack_secret = _sentinel()
    try:
        raise RuntimeError(f"client_secret={exception_secret}")
    except RuntimeError:
        exc_info = sys.exc_info()

    record = logging.LogRecord(
        name="dataflow.redaction.test",
        level=logging.ERROR,
        pathname=__file__,
        lineno=1,
        msg="request failed refresh_token=%s",
        args=(message_secret,),
        exc_info=exc_info,
    )
    record.access_token = extra_secret
    record.config = {"inner": {"client_secret": nested_extra_secret}}
    record.stack_info = f"stack token={stack_secret}"
    return record, (
        message_secret,
        extra_secret,
        exception_secret,
        nested_extra_secret,
        stack_secret,
    )


@pytest.mark.parametrize("key", SENSITIVE_KEYS)
def test_sensitive_values_are_redacted_at_top_level_nested_and_in_lists(key):
    sentinel = _sentinel()
    top_level = log_redaction.redact_log_value(key, sentinel)
    nested = log_redaction.redact_log_value("metadata", {key: sentinel})
    in_list = log_redaction.redact_log_value("items", [{key: sentinel}])

    for value in (top_level, nested, in_list):
        assert sentinel not in json.dumps(value, default=str)


@pytest.mark.parametrize(
    ("key", "expected"),
    (
        ("token_count", False),
        ("request_count", False),
        ("api_key_id", False),
        ("token_type", False),
        ("secret_name", False),
        ("key_prefix", False),
        ("aws_access_key_id", False),
        ("url_kind", False),
        ("aws_secret_access_key", True),
        ("secret_params", True),
        ("password_invalid", True),
        ("clientSecretMs", True),
        ("apikeyitems", True),
        ("token_lens", True),
        ("token_count", False),
        ("api_key_id", False),
        ("secret_name", False),
        ("service_account_json", True),
        ("serviceAccountKey", True),
        ("service_account_id", False),
        ("service_account_email", False),
        ("tokenType", False),
        ("key_prefix", False),
        ("expires_at", False),
        ("expires at", False),
        ("url kind", False),
        ("AccountKey", True),
        ("X-Amz-Signature", True),
        ("webhook_url", True),
        ("password_hash", True),
    ),
)
def test_sensitive_key_predicate_uses_normalization_and_safe_suffixes(key, expected):
    assert log_redaction.is_sensitive_key(key) is expected


@pytest.mark.parametrize(
    ("value", "expected"),
    (
        (None, False),
        (False, False),
        (7, False),
        (12.5, False),
        ("secret", True),
        (b"secret", True),
        ({}, True),
        ([], True),
        ((), True),
    ),
)
def test_sensitive_value_rule_preserves_scalars(value, expected):
    assert log_redaction.is_redactable_value(value) is expected


@pytest.mark.parametrize(("name", "template"), TEXT_CASES, ids=[case[0] for case in TEXT_CASES])
def test_log_text_redacts_supported_secret_patterns(name, template):
    del name
    sentinel = _sentinel()
    rendered = template.format(sentinel=sentinel)
    redacted = log_redaction.redact_log_text(rendered)
    assert sentinel not in json.dumps(redacted)


@pytest.mark.parametrize("scheme", ("Bearer", "Basic"))
def test_authorization_header_keeps_scheme_and_masks_credential(scheme):
    sentinel = _sentinel()

    assert (
        log_redaction.redact_log_text(f"Authorization: {scheme} {sentinel}")
        == f"Authorization: {scheme} ***"
    )


def test_secret_shapes_redaction_does_not_interpret_field_assignments():
    resource = "api_key:key-123"

    assert log_redaction.redact_secret_shapes(resource) == resource
    assert log_redaction.redact_log_text(resource) == "api_key:***"


def test_recursive_log_value_redaction_handles_bytes_tuples_and_non_string_keys():
    sentinel = _sentinel()
    value = {
        17: (
            {"password": sentinel.encode("utf-8")},
            {"client_secret": sentinel},
        )
    }

    cleaned = log_redaction.redact_log_value("metadata", value)

    assert cleaned == {"17": [{"password": "***"}, {"client_secret": "***"}]}
    assert sentinel not in json.dumps(cleaned)


def test_safe_values_and_text_remain_unchanged_across_redaction_helpers():
    assert log_redaction.redact_log_value("metadata", SAFE_METADATA) == SAFE_METADATA
    assert audit_log._redact(SAFE_METADATA) == SAFE_METADATA
    for value in SAFE_TEXT:
        assert log_redaction.redact_log_text(value) == value
        assert audit_log._redact(value) == value


@pytest.mark.parametrize(
    ("text", "expected"),
    (
        ("tokens: 1234", "tokens: 1234"),
        ("tokens: 12.5", "tokens: 12.5"),
        ("prompt_tokens=50", "prompt_tokens=50"),
        ("completion_tokens: 7", "completion_tokens: 7"),
        ("LLM used tokens: 1234", "LLM used tokens: 1234"),
        ("token_pin=123", "token_pin=***"),
        ("password=1234", "password=***"),
        ("pwd=9876", "pwd=***"),
        ("token=42", "token=***"),
        ("client_secret=123456", "client_secret=***"),
    ),
)
def test_numeric_assignment_counts_are_preserved_but_numeric_secrets_are_redacted(
    text, expected
):
    assert log_redaction.redact_log_text(text) == expected


@pytest.mark.parametrize(
    ("text", "expected"),
    (
        ("(using password: YES)", "(using password: YES)"),
        ("(using password: NO)", "(using password: NO)"),
        ("password=abc)", "password=***)"),
        ("pwd=x]", "pwd=***]"),
        ("[token=SENTINEL]", "[token=***]"),
        ("password=yesterday", "password=***"),
    ),
)
def test_assignment_redaction_keeps_closers_and_mysql_boolean_flags(text, expected):
    assert log_redaction.redact_log_text(text) == expected


@pytest.mark.parametrize(
    ("text", "expected"),
    (
        ("password=123456", "password=***"),
        ("pin=4821", "pin=***"),
        ("otp=999111", "otp=***"),
        ("secret=42", "secret=***"),
        ("token=42", "token=***"),
        ("token_count=5", "token_count=5"),
        ("prompt_tokens=50", "prompt_tokens=50"),
        ("has_token=True", "has_token=True"),
        ("spinner=3", "spinner=3"),
        ("options=2", "options=2"),
        ("password=true", "password=true"),
        ("password: false", "password: false"),
        ("password=True", "password=True"),
        ("password=false", "password=false"),
        ("password=truest", "password=***"),
        ("token=null", "token=null"),
    ),
)
def test_structured_numeric_secret_keys_share_text_redaction_rule(text, expected):
    assert log_redaction.redact_log_text(text) == expected


def test_structured_numeric_secret_values_use_the_shared_key_rule():
    sensitive_values = {
        "password": 123456,
        "pin": 4821,
        "otp": 999111,
        "secret": 42,
        "token": 42,
    }
    safe_values = {
        "token_count": 5,
        "prompt_tokens": 50,
        "has_token": True,
        "spinner": 3,
        "options": 2,
        "password_is_set": None,
    }
    source = {**sensitive_values, **safe_values}
    assert log_redaction.redact_log_value("metadata", source) == {
        **dict.fromkeys(sensitive_values, "***"),
        **safe_values,
    }
    assert audit_log._redact(source) == {
        **dict.fromkeys(sensitive_values, "[REDACTED]"),
        **safe_values,
    }
    assert tracing.redact_attributes(source) == {
        **dict.fromkeys(sensitive_values, "***"),
        **safe_values,
    }
    assert log_redaction.redact_log_value("password", True) is True
    assert audit_log._redact({"password": True}) == {"password": True}


def test_secret_config_delegates_key_sensitivity_and_preserves_legacy_output():
    sentinel = _sentinel()
    config = {
        "aws_secret_access_key": sentinel,
        "service_account": sentinel,
        "password": "",
        "token_count": 5,
        "schema": "public",
    }

    assert secret_config.redact_config(config) == {
        "aws_secret_access_key": "***",
        "service_account": "***",
        "password": "",
        "token_count": 5,
        "schema": "public",
    }
    url = secret_config.redact_url(
        f"https://example.test/?aws_secret_access_key={sentinel}&token_count=5"
    )
    assert sentinel not in url
    assert "token_count=5" in url


@pytest.mark.parametrize(
    ("template", "expected_prefix"),
    (
        ("ghp_{sentinel}", "ghp_***"),
        ("github_pat_{sentinel}{sentinel}", "github_pat_***"),
        ("sk_live_{sentinel}", "sk_live_***"),
        ("rk_test_{sentinel}", "rk_test_***"),
        ("whsec_{sentinel}", "whsec_***"),
        ("xoxb-{sentinel}", "xoxb-***"),
        ("xoxp-{sentinel}", "xoxp-***"),
        ("AKIA{aws_suffix}", "AKIA***"),
        ("ASIA{aws_suffix}", "ASIA***"),
        ("AIza{google_suffix}", "AIza***"),
        ("https://example.test/?key={sentinel}", "https://example.test/?key=***"),
        ("https://example.test/?apikey={sentinel}", "https://example.test/?apikey=***"),
        ("https://example.test/?api_key={sentinel}", "https://example.test/?api_key=***"),
        (
            "https://example.test/?access_token={sentinel}",
            "https://example.test/?access_token=***",
        ),
    ),
)
def test_vendor_token_shapes_are_redacted(template, expected_prefix):
    sentinel = _sentinel()
    rendered = template.format(
        sentinel=sentinel,
        aws_suffix=sentinel[:16].upper(),
        google_suffix=(sentinel * 2)[:35],
    )

    redacted = log_redaction.redact_secret_shapes(rendered)

    assert sentinel not in redacted
    assert redacted == expected_prefix


@pytest.mark.parametrize(
    "text",
    (
        "ghost_town",
        "sk_live",
        "risk_test_case",
        "xoxo",
        "AKIAB",
        "monkey=banana",
        "/keys?page=2",
        "skeleton_key",
        "ASIAN market",
    ),
)
def test_vendor_token_shapes_leave_nonmatching_text_unchanged(text):
    assert log_redaction.redact_secret_shapes(text) == text
    assert log_redaction.redact_log_text(text) == text


def test_json_formatter_redacts_message_extras_and_exception():
    record, sentinels = _record_with_sentinels()
    rendered = JsonFormatter().format(record)
    payload = json.loads(rendered)

    for sentinel in sentinels:
        assert sentinel not in json.dumps(payload)
    assert payload["access_token"] == "***"
    assert "client_secret=***" in payload["exception"]


def test_text_formatter_redacts_message_exception_and_stack():
    record, sentinels = _record_with_sentinels()
    rendered = TextFormatter().format(record)

    for sentinel in sentinels:
        assert sentinel not in rendered


def test_otel_redacted_record_cleans_attributes_and_preserves_original():
    record, sentinels = _record_with_sentinels()
    original_message = record.getMessage()
    original_exc_info = record.exc_info

    cloned = otel_logging._redacted_record(record)
    serialized = json.dumps(
        {
            "body": cloned.getMessage(),
            "attributes": {
                key: value
                for key, value in cloned.__dict__.items()
                if key.startswith("exception.")
                or key in {"access_token", "config", "stack_info"}
            },
        },
        default=str,
    )

    for sentinel in sentinels:
        assert sentinel not in serialized
    assert cloned.exc_info is None
    assert cloned.exc_text is None
    assert cloned.__dict__["exception.type"] == "RuntimeError"
    assert cloned.__dict__["exception.message"] == "client_secret=***"
    assert "client_secret=***" in cloned.__dict__["exception.stacktrace"]
    assert cloned.stack_info == "stack token=***"
    assert record.getMessage() == original_message
    assert record.exc_info is original_exc_info
    assert record.access_token == sentinels[1]
    assert record.stack_info == f"stack token={sentinels[4]}"


def test_otel_logging_handler_exports_redacted_body_and_exception_attributes():
    try:
        from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
        from opentelemetry.sdk._logs.export import SimpleLogRecordProcessor
        from opentelemetry.sdk._logs.export import InMemoryLogExporter
    except ImportError as exc:
        pytest.skip(f"OpenTelemetry SDK logging is not installed: {exc}")

    class RedactingLoggingHandler(LoggingHandler):
        def emit(self, record: logging.LogRecord) -> None:
            super().emit(otel_logging._redacted_record(record))

    record, sentinels = _record_with_sentinels()
    exporter = InMemoryLogExporter()
    provider = LoggerProvider()
    provider.add_log_record_processor(SimpleLogRecordProcessor(exporter))
    handler = RedactingLoggingHandler(
        level=logging.NOTSET,
        logger_provider=provider,
    )
    logger = logging.getLogger(f"redaction-test-{uuid4().hex}")
    logger.propagate = False
    logger.addHandler(handler)
    try:
        logger.handle(record)
        exported = exporter.get_finished_logs()
    finally:
        logger.removeHandler(handler)
        provider.shutdown()

    assert exported
    serialized = json.dumps(
        [
            {
                "body": item.log_record.body,
                "attributes": dict(item.log_record.attributes or {}),
            }
            for item in exported
        ],
        default=str,
    )
    for sentinel in sentinels:
        assert sentinel not in serialized
    assert "exception.type" in serialized


def test_tracing_attributes_use_canonical_redaction_without_losing_safe_scalars():
    sentinel = _sentinel()
    cleaned = tracing.redact_attributes(
        {
            "client_secret": sentinel,
            "error": f"request failed access_token={sentinel}",
            "token_count": 50,
            "has_token": True,
            "nested": {"password": sentinel},
        }
    )

    assert cleaned["client_secret"] == "***"
    assert cleaned["error"] == "request failed access_token=***"
    assert cleaned["token_count"] == 50
    assert cleaned["has_token"] is True
    assert cleaned["nested"] == "<dict>"
    assert sentinel not in json.dumps(cleaned)


def test_audit_file_redacts_before_hashing_and_preserves_chain(
    tmp_path, monkeypatch
):
    path = tmp_path / "audit.jsonl"
    monkeypatch.setattr(audit_log, "STORE_PATH", path)
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    sentinel = _sentinel()

    event = audit_log.append_audit_event(
        action="redaction.test",
        resource="redaction.test",
        details=_audit_details(sentinel),
    )

    raw = path.read_text(encoding="utf-8")
    stored = json.loads(raw)
    assert sentinel not in raw
    assert event["resource"] == "redaction.test"
    for key in SENSITIVE_KEYS:
        assert stored["details"]["key_values"][key] == "[REDACTED]"
        assert stored["details"]["nested"]["key_values"][key] == "[REDACTED]"
        assert stored["details"]["rows"][0]["key_values"][key] == "[REDACTED]"
    assert evidence_chain.verify_chain()["verified"] is True
    assert evidence_chain.verify_chain()["findings"] == []


def test_audit_file_preserves_typed_resource_identifiers(tmp_path, monkeypatch):
    path = tmp_path / "audit.jsonl"
    monkeypatch.setattr(audit_log, "STORE_PATH", path)
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    resources = ("api_key:key-123", "scim_token:sa-1", "webhook:wh-1")

    for resource in resources:
        audit_log.append_audit_event(action="resource.test", resource=resource)

    stored = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert [event["resource"] for event in stored] == list(resources)
    assert evidence_chain.verify_chain()["verified"] is True
    assert evidence_chain.verify_chain()["findings"] == []


def test_audit_file_resource_redacts_secret_shapes_without_losing_chain(
    tmp_path, monkeypatch
):
    path = tmp_path / "audit.jsonl"
    monkeypatch.setattr(audit_log, "STORE_PATH", path)
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    sentinels = tuple(_sentinel() for _ in range(4))
    resources = (
        f"postgresql://u:{sentinels[0]}@h/db",
        f"/cb?code={sentinels[1]}&state=x",
        f"dfk_{sentinels[2]}",
        f"Bearer {sentinels[3]}",
    )

    for resource in resources:
        audit_log.append_audit_event(action="resource.test", resource=resource)

    raw = path.read_text(encoding="utf-8")
    stored = [json.loads(line) for line in raw.splitlines()]
    assert all(sentinel not in raw for sentinel in sentinels)
    assert [event["resource"] for event in stored] == [
        "postgresql://u:***@h/db",
        "/cb?code=***&state=x",
        "dfk_***",
        "Bearer ***",
    ]
    assert evidence_chain.verify_chain()["verified"] is True
    assert evidence_chain.verify_chain()["findings"] == []


def test_audit_mongo_store_receives_redacted_event(tmp_path, monkeypatch):
    class _Cursor:
        def sort(self, *_args, **_kwargs):
            return self

        def limit(self, *_args, **_kwargs):
            return self

        def __iter__(self):
            return iter(())

    class _Collection:
        def __init__(self):
            self.docs = []

        def find(self, *_args, **_kwargs):
            return _Cursor()

        def insert_one(self, document):
            self.docs.append(document)

    collection = _Collection()
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: collection)
    monkeypatch.setattr(audit_log, "_maybe_anchor_tip", lambda _event: None)
    monkeypatch.setattr(audit_log, "_maybe_opportunistic_audit_purge", lambda: None)
    sentinel = _sentinel()

    audit_log.append_audit_event(
        action="redaction.test",
        resource="redaction.test",
        details=_audit_details(sentinel),
    )

    assert len(collection.docs) == 1
    assert sentinel not in json.dumps(collection.docs[0], default=str)


def test_audit_mongo_resource_redacts_secret_shapes(tmp_path, monkeypatch):
    class _Cursor:
        def sort(self, *_args, **_kwargs):
            return self

        def limit(self, *_args, **_kwargs):
            return self

        def __iter__(self):
            return iter(())

    class _Collection:
        def __init__(self):
            self.docs = []

        def find(self, *_args, **_kwargs):
            return _Cursor()

        def insert_one(self, document):
            self.docs.append(document)

    collection = _Collection()
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: collection)
    monkeypatch.setattr(audit_log, "_maybe_anchor_tip", lambda _event: None)
    monkeypatch.setattr(audit_log, "_maybe_opportunistic_audit_purge", lambda: None)
    sentinels = tuple(_sentinel() for _ in range(4))
    resources = (
        f"postgresql://u:{sentinels[0]}@h/db",
        f"/cb?code={sentinels[1]}&state=x",
        f"dfk_{sentinels[2]}",
        f"Bearer {sentinels[3]}",
    )

    for resource in resources:
        audit_log.append_audit_event(action="resource.test", resource=resource)

    serialized = json.dumps(collection.docs, default=str)
    assert len(collection.docs) == len(resources)
    assert all(sentinel not in serialized for sentinel in sentinels)


def test_audit_redaction_preserves_list_and_string_caps():
    sentinel = _sentinel()
    value = {
        "rows": [{"index": index} for index in range(55)],
        "description": "x" * 500 + f" client_secret={sentinel}",
    }

    redacted = audit_log._redact(value)

    assert len(redacted["rows"]) == 50
    assert len(redacted["description"]) == 513
    assert sentinel not in json.dumps(redacted)


def test_failed_password_login_does_not_log_email(monkeypatch, caplog):
    from src.services import auth_service

    email = f"{_sentinel()}@example.test"
    monkeypatch.setattr(
        auth_service,
        "_load_users",
        lambda: [{"email": email, "password_hash": "not-used"}],
    )
    monkeypatch.setattr(auth_service, "verify_password", lambda *_args: False)

    with caplog.at_level(logging.INFO, logger="src.services.auth_service"):
        assert auth_service.authenticate(email, "incorrect-password") is None

    rendered = json.dumps([record.getMessage() for record in caplog.records])
    assert email not in rendered


def test_exception_tracebacks_are_redacted_across_log_sinks():
    logger = logging.getLogger("dataflow.r4.traceback-redaction")
    old_level, old_propagate = logger.level, logger.propagate
    capture = logging.Handler()
    capture.records = []
    capture.emit = lambda record: capture.records.append(record)
    logger.setLevel(logging.ERROR)
    logger.propagate = False
    logger.addHandler(capture)

    def _raise_r4_sensitive_error():
        raise RuntimeError(
            "password=S3NT1NEL-pw postgresql://u:S3NT1NEL-dsn@h/db"
        )

    try:
        try:
            _raise_r4_sensitive_error()
        except RuntimeError as exc:
            logger.error("x", exc_info=exc)
    finally:
        logger.removeHandler(capture)
        logger.setLevel(old_level)
        logger.propagate = old_propagate

    record = capture.records[0]
    json_exception = json.loads(JsonFormatter().format(record))["exception"]
    text_traceback = TextFormatter().format(record)
    otel_record = otel_logging._redacted_record(record)
    otel_message = getattr(otel_record, "exception.message")
    otel_traceback = getattr(otel_record, "exception.stacktrace")

    for rendered in (json_exception, text_traceback, otel_message, otel_traceback):
        assert "Traceback" in rendered or rendered == otel_message
        assert "_raise_r4_sensitive_error" in rendered or rendered == otel_message
        assert "S3NT1NEL-pw" not in rendered
        assert "S3NT1NEL-dsn" not in rendered

    assert "password=***" in otel_message
    assert "Traceback" in otel_traceback
