"""Wave 50: PostgreSQL INET / CIDR / MACADDR bind — fail-closed network types."""

from __future__ import annotations

import sys
from ipaddress import IPv4Address, IPv4Interface, IPv4Network
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_coerce_inet_address_and_interface():
    from connectors.sql_bind import coerce_inet_wire, normalize_sql_bind_value

    assert coerce_inet_wire("192.168.0.1") == IPv4Address("192.168.0.1")
    assert coerce_inet_wire("192.168.0.1/24") == IPv4Interface("192.168.0.1/24")
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_inet_wire(3232235521)  # int invent
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_inet_wire("not-an-ip")
    assert normalize_sql_bind_value("10.0.0.1", "INET") == IPv4Address("10.0.0.1")


def test_coerce_cidr_strict_network():
    from connectors.sql_bind import coerce_cidr_wire, normalize_sql_bind_value

    assert coerce_cidr_wire("192.168.0.0/24") == IPv4Network("192.168.0.0/24")
    # Host bits set → Postgres CIDR rejects; we refuse invent.
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_cidr_wire("192.168.0.1/24")
    assert normalize_sql_bind_value("10.0.0.0/8", "CIDR") == IPv4Network("10.0.0.0/8")


def test_coerce_macaddr_formats():
    from connectors.sql_bind import coerce_macaddr_wire, normalize_sql_bind_value

    assert coerce_macaddr_wire("08:00:2b:01:02:03") == "08:00:2b:01:02:03"
    assert coerce_macaddr_wire("08002b010203") == "08002b010203"
    assert coerce_macaddr_wire("0800.2b01.0203") == "0800.2b01.0203"
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_macaddr_wire("gg:hh:ii:jj:kk:ll")
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_macaddr_wire(12345)
    assert normalize_sql_bind_value("aa:bb:cc:dd:ee:ff", "MACADDR") == "aa:bb:cc:dd:ee:ff"
