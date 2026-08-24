"""Phase D1 — tenant IP allowlist must not trust left-most XFF by default."""

from __future__ import annotations

from services.client_ip import resolve_client_ip


def test_default_ignores_xff_spoof():
    # TRUSTED_PROXY_COUNT=0 (default): socket peer only.
    ip = resolve_client_ip(
        x_forwarded_for="1.2.3.4, 10.0.0.1",
        x_real_ip="1.2.3.4",
        remote_addr="9.9.9.9",
        trusted_proxies=0,
    )
    assert ip == "9.9.9.9"


def test_one_trusted_proxy_takes_rightmost_untrusted():
    # XFF: attacker, real_client, our_edge_proxy — trust 1 → real_client
    ip = resolve_client_ip(
        x_forwarded_for="8.8.8.8, 203.0.113.50, 10.0.0.2",
        remote_addr="10.0.0.2",
        trusted_proxies=1,
    )
    assert ip == "203.0.113.50"


def test_insufficient_hops_fail_closed_to_remote():
    ip = resolve_client_ip(
        x_forwarded_for="8.8.8.8",
        remote_addr="10.0.0.2",
        trusted_proxies=2,
    )
    assert ip == "10.0.0.2"
