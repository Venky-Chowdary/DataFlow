from __future__ import annotations

import json
import logging
import sys
from uuid import uuid4

import pytest

from services import audit_log, evidence_chain, log_redaction, otel_logging, tracing
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
    assert log_redaction._redacts_value(value) is expected


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
        resource=f"client_secret={sentinel}",
        details=_audit_details(sentinel),
    )

    raw = path.read_text(encoding="utf-8")
    stored = json.loads(raw)
    assert sentinel not in raw
    assert event["resource"] == "client_secret=***"
    for key in SENSITIVE_KEYS:
        assert stored["details"]["key_values"][key] == "[REDACTED]"
        assert stored["details"]["nested"]["key_values"][key] == "[REDACTED]"
        assert stored["details"]["rows"][0]["key_values"][key] == "[REDACTED]"
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
        resource=f"client_secret={sentinel}",
        details=_audit_details(sentinel),
    )

    assert len(collection.docs) == 1
    assert sentinel not in json.dumps(collection.docs[0], default=str)


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
