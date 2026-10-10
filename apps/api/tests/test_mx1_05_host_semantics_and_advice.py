"""QA MX1-05 — qdrant refused ``host="http://<tunnel>"`` (it dialed
``http://http://<tunnel>:6333``) while other drivers accept a URL host, and every
failed probe said "use the public proxy if this is Railway" — for any host.
"""

from __future__ import annotations

import pytest

from connectors import milvus_writer, qdrant_writer, weaviate_writer
from src.ai.copilot.connector_create import probe_failure_advice


@pytest.mark.parametrize("mod, default", [(qdrant_writer, 6333), (milvus_writer, 19530), (weaviate_writer, 8080)])
def test_vector_http_base_url_accepts_scheme_in_host(mod, default):
    assert mod._base_url("http://tunnel.example", 7001, False) == "http://tunnel.example:7001"
    assert mod._base_url("https://tunnel.example:7443", 0, False) == "https://tunnel.example:7443"
    assert mod._base_url("https://tunnel.example/", 0, False) == f"https://tunnel.example:{default}"
    assert mod._base_url("tunnel.example", 0, True) == f"https://tunnel.example:{default}"
    assert mod._base_url("", 0, False) == f"http://localhost:{default}"


def test_probe_advice_mentions_railway_only_for_railway_hosts():
    assert "Railway" not in probe_failure_advice("postgresql", host="no-such-host.invalid")
    assert "Railway" in probe_failure_advice("postgresql", host="tokaido.proxy.rlwy.net")
    assert "Railway" in probe_failure_advice("mysql", connection_string="mysql://u:p@mysql.railway.internal/db")
    assert probe_failure_advice("postgresql").startswith("Fix host/port/user/password")
