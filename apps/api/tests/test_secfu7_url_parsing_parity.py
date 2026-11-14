"""Security follow-up 7: the egress and residency judges read a connection URL as the driver does.

PR 403 review 5 found that SQLAlchemy keeps ``#...`` inside a URL's query
while ``urllib.parse.urlparse`` calls it a fragment. The same class of
disagreement remained in the other URL readers that judge where a connector
dials:

* ``data_residency._url_hosts`` read query keys (``host=``, ``alt_hosts=``,
  ``odbc_connect=`` ...) with ``urlparse``, so ``?a=b#&host=<foreign>`` hid a
  host the driver dials;
* ``egress_guard`` took the authority host from ``urlparse`` only. Drivers that
  percent-decode the host (redis-py, libpq URIs, urllib3) dialled
  ``%31%36%39.254.169.254`` as the metadata address while the guard judged
  ``39.254.169.254`` (an embedded-IPv4 scan) or nothing at all;
* a bracketed name (``[metadata.google.internal]``), which SQLAlchemy and the
  last-``@`` parser read as that name, made ``urlparse`` raise ``ValueError``
  inside the guard instead of being judged.
"""

from __future__ import annotations

import pytest

from services import egress_guard as eg

_EU = "h.eu-west-1.rds.amazonaws.com"


# --- residency: the query as SQLAlchemy reads it ----------------------------


@pytest.mark.parametrize(
    "url",
    [
        f"postgresql://u:p@{_EU}/db?sslmode=require#&host=52.1.2.3",
        f"postgresql://u:p@{_EU}/db#?host=52.1.2.3",
        f"mysql+pymysql://u:p@{_EU}/db?charset=utf8#&host=52.1.2.3",
    ],
)
def test_residency_reads_query_hosts_after_a_hash(url):
    import sqlalchemy as sa

    from services.data_residency import _url_hosts

    driver_query = dict(sa.engine.url.make_url(url).query)
    assert "52.1.2.3" in " ".join(str(v) for v in driver_query.values()), "the driver sees the host"
    hosts = _url_hosts(url)
    assert hosts is None or "52.1.2.3" in hosts, hosts


@pytest.mark.parametrize(
    "fmt, url",
    [
        ("postgresql", f"postgresql://u:p@{_EU}/db?sslmode=require#&host=52.1.2.3"),
        ("mysql", f"mysql+pymysql://u:p@{_EU}/db?charset=utf8#&host=52.1.2.3"),
        ("clickhouse", f"clickhouse+native://u:p@{_EU}:9000/db?secure=1#&alt_hosts=52.1.2.3:9000"),
    ],
)
def test_residency_refuses_a_hash_hidden_foreign_host(monkeypatch, tmp_path, fmt, url):
    from services import tenant_store
    from services.data_residency import destination_region, residency_refusal
    from src.transfer.models import EndpointConfig

    for name in ("RESIDENCY_STRICT", "DEFAULT_REGION"):
        monkeypatch.delenv(f"DATAWRAP_{name}", raising=False)
        monkeypatch.delenv(f"DATAFLOW_{name}", raising=False)
    monkeypatch.setattr(tenant_store, "STORE_PATH", tmp_path / "tenants.json")
    tenant_store.create_tenant(workspace_id="ws-secfu7", name="EU", data_region="eu-west-1")
    endpoint = EndpointConfig(
        kind="database", format=fmt, host=_EU, database="db", table="t", connection_string=url,
    )
    assert destination_region(endpoint, workspace_id="ws-secfu7").region != "eu-west-1"
    assert residency_refusal("ws-secfu7", endpoint)


# --- egress: the authority as the drivers read it ---------------------------


@pytest.mark.parametrize(
    "url, dialled",
    [
        ("redis://:p@%31%36%39.254.169.254:6379/0", "169.254.169.254"),
        ("redis://:p@%6c%6f%63%61%6c%68%6f%73%74:6379/0", "localhost"),
        ("postgresql://u:p@%31%32%37.0.0.1/db", "127.0.0.1"),
        ("postgresql://u:p@[metadata.google.internal]/db", "metadata.google.internal"),
        ("postgresql://u:p@[::1]:5432/db", "::1"),
        ("mongodb://u:p@h.example:27017,%31%32%37.0.0.1:27017/db", "127.0.0.1"),
    ],
)
def test_egress_judges_the_host_the_driver_dials(url, dialled):
    assert dialled in eg._hosts_from_values(url)


def test_redis_py_and_urllib3_decode_the_host_they_dial():
    """The premise: these drivers dial the percent-decoded host."""
    from redis.connection import parse_url
    from urllib3.util import parse_url as urllib3_parse_url

    assert parse_url("redis://:p@%31%36%39.254.169.254:6379/0")["host"] == "169.254.169.254"
    assert urllib3_parse_url("http://%31%32%37.0.0.1:8080/").host == "127.0.0.1"


@pytest.mark.parametrize(
    "data",
    [
        {"name": "r", "type": "redis", "host": "", "connection_string": "redis://:p@%31%36%39.254.169.254:6379/0"},
        {"name": "p", "type": "postgresql", "host": "", "connection_string": "postgresql://u:p@[metadata.google.internal]/db"},
        {"name": "p", "type": "postgresql", "host": "postgresql://u:p@%31%36%39.254.169.254/db"},
    ],
)
def test_saving_a_metadata_target_spelled_for_the_driver_is_refused(data):
    refusal = eg.connector_save_refusal(dict(data))
    assert refusal, data


def test_a_bracketed_name_never_raises_inside_the_guard():
    assert eg.egress_url_allowed("http://[metadata.google.internal]/x", allowlist=()) is False
    eg._hosts_from_values("postgresql://u:p@[not a host]/db")  # no exception


def test_ordinary_urls_are_unchanged():
    assert eg._hosts_from_values(f"postgresql://u:p@{_EU}:5432/db?sslmode=require") == [_EU]
    assert eg._hosts_from_values("redis://:p@cache.example.com:6379/0") == ["cache.example.com"]
