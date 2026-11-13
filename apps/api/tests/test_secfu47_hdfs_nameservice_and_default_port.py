"""Follow-up review 5 (L3): an HDFS namenode must be an address, and the default port is normalised.

Commit 2e84a95f9 allowed the namenode the connector's warehouse named and
judged it with the egress check. A name that is not an IP and does not resolve
(an HA nameservice ``hdfs://nameservice1/...`` or a ``viewfs://clusterX/...``
mount table) passed: the egress check does not refuse an unresolved name, and
libhdfs resolves it through the API host's own Hadoop configuration
(``hdfs-site.xml`` / ``core-site.xml``), like the refused ``hdfs:///``. And
``hdfs://nn`` (connector) vs ``hdfs://nn:8020`` (table) were different
namenodes, so a legitimate table was refused.

Now a namenode host must be an IP literal or a DNS name that resolves (the
connector cannot supply HA configuration, so a nameservice is refused), and a
missing ``hdfs`` port is 8020 on both sides.
"""

from __future__ import annotations

import socket

import pytest

_PUBLIC = "93.184.216.34"


@pytest.fixture(autouse=True)
def dns(monkeypatch):
    for prefix in ("DATAFLOW", "DATAWRAP"):
        for name in ("EGRESS_ENFORCE_NEW", "EGRESS_ALLOWLIST", "SOURCE_PATH_ROOTS"):
            monkeypatch.delenv(f"{prefix}_{name}", raising=False)
    names = {"nn.example.com": _PUBLIC}

    def getaddrinfo(host, port, *args, **kwargs):
        if host in names:
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (names[host], port or 0))]
        try:
            socket.inet_aton(str(host))
        except OSError:
            raise socket.gaierror(socket.EAI_NONAME, "unknown name") from None
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (host, port or 0))]

    monkeypatch.setattr(socket, "getaddrinfo", getaddrinfo)


def _verdict(warehouse: str, netloc: str | None, **props) -> str:
    from connectors.iceberg_hdfs_guard import NAMENODES, hdfs_refusal, own_namenodes

    scheme = warehouse.split("://", 1)[0].lower()
    try:
        return hdfs_refusal({NAMENODES: own_namenodes({"warehouse": warehouse}), **props}, netloc, scheme=scheme)
    except TypeError:  # before the fix: no scheme parameter
        return hdfs_refusal({NAMENODES: own_namenodes({"warehouse": warehouse}), **props}, netloc)


@pytest.mark.parametrize("warehouse,netloc", [
    ("hdfs://nameservice1/wh", "nameservice1"),
    ("viewfs://clusterX/wh", "clusterX"),
    ("hdfs://nameservice1:8020/wh", "nameservice1:8020"),
])
def test_a_nameservice_or_mount_table_is_refused(warehouse, netloc):
    assert _verdict(warehouse, netloc)


@pytest.mark.parametrize("warehouse,netloc", [
    ("hdfs://nn.example.com/wh", "nn.example.com:8020"),
    ("hdfs://nn.example.com:8020/wh", "nn.example.com"),
    ("hdfs://NN.example.com:8020/wh", "nn.example.com:8020"),
    (f"hdfs://{_PUBLIC}/wh", f"{_PUBLIC}:8020"),
])
def test_the_default_port_is_normalised(warehouse, netloc):
    assert _verdict(warehouse, netloc) == ""


def test_a_named_port_still_has_to_match():
    assert _verdict("hdfs://nn.example.com:9000/wh", "nn.example.com:8020")


def test_hdfs_host_property_with_a_nameservice_is_refused():
    assert _verdict("hdfs://nameservice1/wh", None, **{"hdfs.host": "nameservice1"})
