"""Phase D1 — client IP under the trusted-proxy boundary (ProxyFix semantics).

Each proxy APPENDS the address of the peer it received the request from
(nginx ``$proxy_add_x_forwarded_for`` = incoming XFF + ``$remote_addr``;
Werkzeug ``ProxyFix(x_for=N)`` reads ``hops[-N]``). A proxy never appends its
own address, so with N trusted proxies the client is ``hops[-N]``.

The round-8 version of this file put the last proxy's own address at the
right of the header ("8.8.8.8, 203.0.113.50, 10.0.0.2" with remote 10.0.0.2)
and so asserted ``hops[-(N+1)]``. Behind a real single proxy that returned the
socket peer (len(hops) <= N) or a client-supplied value — QA round 11.
"""

from __future__ import annotations

import pytest

from services.client_ip import client_ip_from_request, resolve_client_ip


def test_default_ignores_xff_spoof():
    # TRUSTED_PROXY_COUNT=0 (default): socket peer only.
    ip = resolve_client_ip(
        x_forwarded_for="1.2.3.4, 10.0.0.1",
        x_real_ip="1.2.3.4",
        remote_addr="9.9.9.9",
        trusted_proxies=0,
    )
    assert ip == "9.9.9.9"


def test_one_trusted_proxy_takes_the_hop_it_appended():
    # Client sends "8.8.8.8"; our proxy (remote 10.0.0.2) appends the client
    # it saw, 203.0.113.50. hops[-1] is the client.
    ip = resolve_client_ip(
        x_forwarded_for="8.8.8.8, 203.0.113.50",
        remote_addr="10.0.0.2",
        trusted_proxies=1,
    )
    assert ip == "203.0.113.50"


def test_one_proxy_without_client_header():
    # The common case: the client sends no XFF, the proxy writes one entry.
    ip = resolve_client_ip(x_forwarded_for="203.0.113.50", remote_addr="10.0.0.2", trusted_proxies=1)
    assert ip == "203.0.113.50"


def test_railway_chain_three_proxies():
    # browser -> edge (appends client) -> nginx (appends edge) -> edge (appends nginx) -> API
    ip = resolve_client_ip(
        x_forwarded_for="6.6.6.6, 203.0.113.77, 100.64.0.7, 34.86.1.2",
        remote_addr="10.250.0.3",
        trusted_proxies=3,
    )
    assert ip == "203.0.113.77"


def test_insufficient_hops_fail_closed_to_remote():
    ip = resolve_client_ip(
        x_forwarded_for="8.8.8.8",
        remote_addr="10.0.0.2",
        trusted_proxies=2,
    )
    assert ip == "10.0.0.2"


@pytest.mark.parametrize(
    "hop,expected",
    [("203.0.113.5:443", "203.0.113.5"), ("[2001:db8::1]:443", "2001:db8::1"), ("2001:db8::1", "2001:db8::1")],
)
def test_ports_are_stripped(hop, expected):
    assert resolve_client_ip(x_forwarded_for=hop, remote_addr="10.0.0.2", trusted_proxies=1) == expected


def test_env_hop_count(monkeypatch):
    class _Req:
        headers = {"x-forwarded-for": "6.6.6.6, 203.0.113.9"}

        class client:  # noqa: N801 - mimics starlette's Address
            host = "10.0.0.2"

    monkeypatch.delenv("DATAFLOW_TRUSTED_PROXY_COUNT", raising=False)
    monkeypatch.setenv("DATAWRAP_TRUSTED_PROXY_COUNT", "1")
    assert client_ip_from_request(_Req()) == "203.0.113.9"
    monkeypatch.setenv("DATAWRAP_TRUSTED_PROXY_COUNT", "0")
    assert client_ip_from_request(_Req()) == "10.0.0.2"
