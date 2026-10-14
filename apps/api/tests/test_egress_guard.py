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
    assert not egress_url_allowed("http://127.0.0.1/latest/meta-data", allowlist=())
    assert not egress_url_allowed("http://169.254.169.254/latest/meta-data", allowlist=())
    assert not egress_url_allowed("http://localhost:8080/hook", allowlist=())


def test_rfc1918_and_non_http_are_blocked():
    assert not egress_url_allowed("http://10.0.0.8/alert", allowlist=())
    assert not egress_url_allowed("http://192.168.1.50/hook", allowlist=())
    assert not egress_url_allowed("http://172.16.4.4/hook", allowlist=())
    assert not egress_url_allowed("file:///etc/passwd", allowlist=())
    assert not egress_url_allowed("ftp://example.com/x", allowlist=())
    assert not egress_url_allowed("", allowlist=())


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


def test_each_private_range_and_a_dns_rebind_are_refused():
    """Default deny covers RFC1918, loopback, link-local, CGNAT, and metadata.

    A name that was public at the preview check and private on the dial the
    driver uses is refused before that answer is returned (R11-002).
    """
    import socket

    from services.egress_guard import (
        EgressRefused,
        classify_host,
        egress_allowlist_error,
        metadata_endpoint_refusal,
        pinned_egress,
        redirect_target_allowed,
    )

    denied = {
        "10.1.2.3": "10.x",
        "172.16.5.5": "172.16/12",
        "172.31.255.255": "172.16/12",
        "192.168.50.1": "192.168/16",
        "127.0.0.1": "loopback",
        "::1": "loopback",
        "169.254.169.254": "cloud metadata and link-local",
        "fe80::1": "cloud metadata and link-local",
        "fd00:ec2::254": "cloud metadata and link-local",
        "100.100.100.200": "cloud metadata and link-local",
        "100.64.0.1": "carrier-grade NAT",
        "fd00::1": "IPv6 unique-local",
    }
    for host, phrase in denied.items():
        assert phrase in (classify_host(host, allowlist=()) or ""), host

    assert classify_host("8.8.8.8", allowlist=()) is None
    assert classify_host("192.168.1.9", allowlist=["192.168.1.0/24"]) is None
    assert classify_host("10.0.0.9", allowlist=["192.168.1.0/24"])
    assert classify_host("169.254.169.254", allowlist=["169.254.169.254/32"])
    assert classify_host("fd00:ec2::254", allowlist=["fd00::/8"])
    assert classify_host("100.100.100.200", allowlist=["100.64.0.0/10"])
    assert "cannot be allowed" in egress_allowlist_error(["169.254.169.254"])
    assert "whole address space" in egress_allowlist_error(["0.0.0.0/0"])
    assert egress_allowlist_error(["10.0.0.0/8"]) == ""
    assert redirect_target_allowed("http://169.254.169.254/latest") is False
    assert redirect_target_allowed("http://192.168.1.1/next") is False

    calls = {"n": 0}
    real = socket.getaddrinfo

    def fake(host, port, *args, **kwargs):
        calls["n"] += 1
        ip = "8.8.8.8" if calls["n"] == 1 else "169.254.169.254"
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, port or 0))]

    socket.getaddrinfo = fake
    try:
        assert metadata_endpoint_refusal("rebind.example", allowlist=()) == ""
        try:
            with pinned_egress(allowlist=()):
                socket.getaddrinfo("rebind.example", 5432)
        except EgressRefused as exc:
            assert "cloud metadata and link-local" in str(exc)
        else:
            raise AssertionError("rebind dial was allowed")
    finally:
        socket.getaddrinfo = real

    import inspect

    from services.notification_service import _NoRedirect, _send_mailgun

    assert "urlopen" not in inspect.getsource(_send_mailgun)
    assert (
        _NoRedirect().redirect_request(
            None, None, 302, "Found", {}, "http://169.254.169.254/latest"
        )
        is None
    )


def test_embedded_file_url_is_not_a_dial_target(tmp_path):
    """duckdb:/// and sqlite:/// have an empty host. They must not be resolved.

    A long absolute path used to be classified as a DNS name. IDNA then raised
    "label empty or too long", and the desktop-lab DuckDB seed failed with
    Destination unreachable.
    """
    import socket

    from services.egress_guard import _hosts_from_values, metadata_endpoint_refusal, pinned_egress

    duck = tmp_path / ("segment_" * 12 + "lab.duckdb")
    sqlite = tmp_path / "lab.db"
    duck_url = f"duckdb:///{duck}"
    sqlite_url = f"sqlite:///{sqlite}"
    bare = str(tmp_path / "192.168.1.50" / "lab.db")
    assert _hosts_from_values(duck_url, sqlite_url, bare, "") == []
    assert metadata_endpoint_refusal("", duck_url, sqlite_url, bare) == ""
    # A nested authority is still a dial, including inside a JDBC wrapper.
    assert "192.168.1.9" in _hosts_from_values("jdbc:postgresql://192.168.1.9:5432/db")
    assert "192.168/16" in metadata_endpoint_refusal("jdbc:postgresql://192.168.1.9:5432/db")
    assert "db.internal" in _hosts_from_values("jdbc:postgresql://db.internal:5432/db")
    assert "cloud metadata" in metadata_endpoint_refusal("jdbc:postgresql://db.internal:5432/db")

    calls: list[str] = []
    real = socket.getaddrinfo

    def wrap(host, port, *args, **kwargs):
        calls.append(str(host))
        return real(host, port, *args, **kwargs)

    socket.getaddrinfo = wrap
    try:
        with pinned_egress(allowlist=()):
            assert metadata_endpoint_refusal("", duck_url) == ""
        from connectors.generic_sql import get_connection

        conn = get_connection(
            database=str(duck),
            connection_string=duck_url,
            db_type="duckdb",
        )
        try:
            cur = conn.cursor()
            cur.execute("select 1")
            assert cur.fetchone()[0] == 1
        finally:
            conn.close()
    finally:
        socket.getaddrinfo = real
    assert calls == [] or all("duckdb" not in item and "sqlite" not in item for item in calls)


def test_notification_http_post_refuses_private_before_socket(monkeypatch):
    from services.notification_service import _http_post

    def boom(*_a, **_k):
        raise AssertionError("urlopen must not run for a blocked host")

    monkeypatch.setattr("urllib.request.build_opener", boom)
    result = _http_post("http://127.0.0.1/steal", {"text": "hi"})
    assert result["ok"] is False
    assert "not allowed" in result["error"]
