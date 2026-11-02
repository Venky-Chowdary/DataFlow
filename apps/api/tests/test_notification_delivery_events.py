"""Notification delivery emits ``notification.delivery.*`` without the URL.

Slack/Teams webhook URLs carry their secret in the path, so neither the
delivery event nor the transport failure log may contain the URL, recipients,
or the driver's error text.
"""

from __future__ import annotations

import logging

import pytest

from services import notification_service
from services.logging_config import ContextFilter, JsonFormatter, TextFormatter
from services.notification_store import NotificationChannel

SECRET = "T0SECRETPATHTOKEN9f2b"
URL = f"https://hooks.example.com/services/{SECRET}"


class _Capture(logging.Handler):
    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.records: list[logging.LogRecord] = []
        self.addFilter(ContextFilter())

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)

    def events(self) -> list[logging.LogRecord]:
        return [
            r for r in self.records
            if str(getattr(r, "event", "")).startswith("notification.delivery.")
        ]


@pytest.fixture
def capture():
    log = logging.getLogger("services.notification_service")
    handler = _Capture()
    old_level = log.level
    log.addHandler(handler)
    log.setLevel(logging.DEBUG)
    try:
        yield handler
    finally:
        log.removeHandler(handler)
        log.setLevel(old_level)


def _assert_secret_free(records: list[logging.LogRecord]) -> None:
    for record in records:
        for fmt in (JsonFormatter(), TextFormatter()):
            rendered = fmt.format(record)
            assert SECRET not in rendered
            assert "hooks.example.com" not in rendered


def _channel(kind: str = "webhook", enabled: bool = True) -> NotificationChannel:
    return NotificationChannel(
        id="ch-1", workspace_id="ws-9", kind=kind, label="ops", enabled=enabled,
        config={"url": URL, "webhook_url": URL},
    )


def test_transport_failure_log_never_contains_webhook_url(capture, monkeypatch):
    class _Opener:
        def open(self, req, timeout=None):  # noqa: ANN001
            raise OSError(f"connection refused for {req.full_url}")

    monkeypatch.setattr(notification_service, "_allowed_url", lambda url: True)
    monkeypatch.setattr(notification_service.urllib.request, "build_opener", lambda *a: _Opener())
    result = notification_service._http_post(URL, {"text": "hi"})
    assert result["ok"] is False
    assert capture.records, "transport failure must still be logged"
    _assert_secret_free(capture.records)


def test_successful_webhook_delivery_event(capture, monkeypatch):
    monkeypatch.setattr(
        notification_service, "_http_post", lambda *a, **k: {"ok": True, "status": 200, "body": URL}
    )
    assert notification_service.send_to_channel(_channel(), {"text": "done"})["ok"] is True
    (record,) = capture.events()
    assert record.getMessage() == "notification.delivery.succeeded"
    assert (record.outcome, record.levelno) == ("succeeded", logging.INFO)
    assert (record.channel_kind, record.channel_id, record.workspace_id) == ("webhook", "ch-1", "ws-9")
    assert record.http_status == 200
    assert record.duration_ms >= 0
    _assert_secret_free(capture.records)


def test_failed_delivery_is_warning_with_reason(capture, monkeypatch):
    monkeypatch.setattr(
        notification_service, "_http_post", lambda *a, **k: {"ok": False, "error": f"refused {URL}"}
    )
    assert notification_service.send_to_channel(_channel(), {"text": "x"})["ok"] is False
    (record,) = capture.events()
    assert record.getMessage() == "notification.delivery.failed"
    assert (record.outcome, record.levelno, record.reason) == ("failed", logging.WARNING, "not_delivered")
    _assert_secret_free(capture.records)


def test_disabled_channel_is_skipped(capture):
    result = notification_service.send_to_channel(_channel(enabled=False), {"text": "x"})
    assert result == {"ok": False, "error": "Channel disabled"}
    (record,) = capture.events()
    assert (record.getMessage(), record.outcome, record.reason) == (
        "notification.delivery.skipped", "skipped", "channel_disabled",
    )


def test_unknown_kind_does_not_log_kind_text(capture):
    notification_service.send_to_channel(_channel(kind="pager_" + SECRET), {"text": "x"})
    (record,) = capture.events()
    assert (record.channel_kind, record.reason) == ("unknown", "unknown_kind")
    _assert_secret_free(capture.records)


def test_dispatch_exception_logs_type_and_reraises_same(capture, monkeypatch):
    boom = RuntimeError(URL)

    def _raise(*a, **k):
        raise boom

    monkeypatch.setattr(notification_service, "_dispatch_to_channel", _raise)
    with pytest.raises(RuntimeError) as info:
        notification_service.send_to_channel(_channel(), {"text": "x"})
    assert info.value is boom
    (record,) = capture.events()
    assert (record.outcome, record.levelno, record.error_type) == ("failed", logging.WARNING, "RuntimeError")
    _assert_secret_free(capture.records)
