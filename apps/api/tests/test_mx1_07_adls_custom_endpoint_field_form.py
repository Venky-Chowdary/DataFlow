"""QA MX1-07 — ADLS against a tunneled Azurite works only via connection_string.

The host/port/username/password form built ``https://<account>.blob.core.windows.net``
for any host that was not literally localhost/127.0.0.1 or port 10000, so the
shared-key signature was computed for — and sent to — public Azure. Live:
a second Azurite is published on 172.17.0.1:41000 (non-local host, non-10000 port).
"""

from __future__ import annotations

import socket

import pytest

from connectors.adls_common import _account_url, blob_service_client

_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="  # Azurite public dev key
_CFG = {"type": "adls", "host": "172.17.0.1", "port": 41000, "username": "devstoreaccount1", "password": _KEY}


def _up(host: str, port: int) -> bool:
    try:
        socket.create_connection((host, port), timeout=1).close()
        return True
    except OSError:
        return False


def test_field_form_custom_endpoint_is_path_style():
    assert _account_url(_CFG) == "http://172.17.0.1:41000/devstoreaccount1"
    assert _account_url({**_CFG, "ssl": True, "username": "acct2", "host": "blob.tunnel.example"}) == (
        "https://blob.tunnel.example:41000/acct2"
    )


def test_real_azure_account_url_unchanged():
    assert _account_url({"username": "prodacct", "password": "k"}) == "https://prodacct.blob.core.windows.net"
    assert _account_url({"host": "prodacct", "username": "prodacct", "port": 443}) == (
        "https://prodacct.blob.core.windows.net"
    )


@pytest.mark.skipif(not _up("172.17.0.1", 41000), reason="tunnel Azurite not running on 172.17.0.1:41000")
def test_field_form_signs_and_lists_against_tunneled_azurite():
    client = blob_service_client(_CFG)
    container = client.get_container_client("mx107")
    if not container.exists():
        container.create_container()
    container.upload_blob("probe.txt", b"ok", overwrite=True)
    names = [c["name"] for c in client.list_containers()]
    assert "mx107" in names
    assert container.download_blob("probe.txt").readall() == b"ok"
