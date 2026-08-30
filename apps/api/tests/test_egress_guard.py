"""SSRF deny-list for operator-supplied webhook / notification URLs."""

from __future__ import annotations

from services.egress_guard import egress_url_allowed, host_is_blocked


def test_public_https_is_allowed():
    assert egress_url_allowed("https://hooks.slack.com/services/T000/B000/xxx")
    assert egress_url_allowed("https://example.com/hook")


def test_loopback_and_link_local_are_blocked():
    assert host_is_blocked("127.0.0.1")
    assert host_is_blocked("localhost")
    assert host_is_blocked("169.254.169.254")
    assert host_is_blocked("[::1]")
    assert not egress_url_allowed("http://127.0.0.1/latest/meta-data")
    assert not egress_url_allowed("http://169.254.169.254/latest/meta-data")
    assert not egress_url_allowed("http://localhost:8080/hook")


def test_rfc1918_and_non_http_are_blocked():
    assert not egress_url_allowed("http://10.0.0.8/alert")
    assert not egress_url_allowed("http://192.168.1.50/hook")
    assert not egress_url_allowed("http://172.16.4.4/hook")
    assert not egress_url_allowed("file:///etc/passwd")
    assert not egress_url_allowed("ftp://example.com/x")
    assert not egress_url_allowed("")


def test_smtp_refuses_private_host(monkeypatch):
    from services.notification_service import _smpt_send

    def boom(*_a, **_k):
        raise AssertionError("SMTP must not connect to a blocked host")

    monkeypatch.setattr("smtplib.SMTP", boom)
    result = _smpt_send(
        ["ops@example.com"],
        "hi",
        "body",
        smtp_cfg={"host": "169.254.169.254", "port": 25, "from": "x@example.com"},
    )
    assert result["ok"] is False
    assert "not allowed" in result["error"]


def test_notification_http_post_refuses_private_before_socket(monkeypatch):
    from services.notification_service import _http_post

    def boom(*_a, **_k):
        raise AssertionError("urlopen must not run for a blocked host")

    monkeypatch.setattr("urllib.request.build_opener", boom)
    result = _http_post("http://127.0.0.1/steal", {"text": "hi"})
    assert result["ok"] is False
    assert "not allowed" in result["error"]
