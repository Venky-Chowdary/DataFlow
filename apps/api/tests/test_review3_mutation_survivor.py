"""Security review round 3 (surviving mutant ``sql_driver_host_blank``).

``data_residency._sql_driver_host`` adds the host ``connectors.sql_dsn.
resolve_sql_endpoint`` dials to the addresses residency judges. Every probe
the reviewer had also tripped the path-``@`` or ``parse_url_authority``
checks, so a mutant that made it return ``""`` survived. Here only the
driver resolver disagrees with the fields: the URL names an in-region host,
and the resolver (stubbed to stand for any future resolver rule the field
parsers do not mirror) dials another. The destination must be refused, and
the resolver's host must be among the judged addresses.
"""

from __future__ import annotations

import pytest

EU = "x.eu-west-1.rds.amazonaws.com"


@pytest.fixture
def fenced(tmp_path, monkeypatch):
    from services import tenant_store

    for name in ("RESIDENCY_STRICT", "DEFAULT_REGION"):
        monkeypatch.delenv(f"DATAWRAP_{name}", raising=False)
        monkeypatch.delenv(f"DATAFLOW_{name}", raising=False)
    monkeypatch.setattr(tenant_store, "STORE_PATH", tmp_path / "tenants.json")
    tenant_store.create_tenant(workspace_id="ws-eu", name="EU", data_region="eu-west-1")


def _endpoint(fmt, url):
    from src.transfer.models import EndpointConfig

    return EndpointConfig.from_dict("database", {"format": fmt, "host": EU, "connection_string": url, "table": "t"})


@pytest.mark.parametrize("fmt,url", [
    ("postgresql", f"postgresql://u:p@{EU}:5432/db"),
    ("mysql", f"mysql://u:p@{EU}:3306/db"),
])
def test_only_the_driver_resolver_disagrees(fenced, monkeypatch, fmt, url):
    import connectors.sql_dsn as sql_dsn
    from services import data_residency as dr

    endpoint = _endpoint(fmt, url)
    assert dr.residency_refusal("ws-eu", endpoint) is None  # the fields alone: in region
    real = sql_dsn.resolve_sql_endpoint
    monkeypatch.setattr(sql_dsn, "resolve_sql_endpoint", lambda **kw: {**real(**kw), "host": "52.1.2.3"})
    hosts, _unreadable = dr._addresses(endpoint)
    assert "52.1.2.3" in hosts
    assert dr.residency_refusal("ws-eu", endpoint)
