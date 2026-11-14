"""SEC-2: every connector dial refuses loopback and private targets by default.

The transfer/introspect read path was an audit-only egress site unless
``DATAFLOW_EGRESS_ENFORCE_NEW`` was set, and no deployment set it: a
workspace editor introspected and exported the platform's own Redis on
127.0.0.1, and Elasticsearch, InfluxDB and ClickHouse dialled loopback.
With the production defaults (no allowlist, no flag) each driver must now be
refused exactly like ``/connectors/test`` refuses, before any dial.
"""

from __future__ import annotations

import pytest

from tests.tenancy_world import tenancy_world  # noqa: F401

REFUSED = "Refused connector host"


@pytest.fixture
def editor(tenancy_world, monkeypatch):  # noqa: F811
    for name in ("DATAFLOW_EGRESS_ALLOWLIST", "DATAWRAP_EGRESS_ALLOWLIST",
                 "DATAFLOW_EGRESS_ENFORCE_NEW", "DATAWRAP_EGRESS_ENFORCE_NEW"):
        monkeypatch.delenv(name, raising=False)
    return tenancy_world.a


def test_enforcement_is_on_unless_explicitly_opted_out(monkeypatch):
    from services.egress_guard import enforce_new_points

    for name in ("DATAFLOW_EGRESS_ENFORCE_NEW", "DATAWRAP_EGRESS_ENFORCE_NEW"):
        monkeypatch.delenv(name, raising=False)
    assert enforce_new_points() is True
    monkeypatch.setenv("DATAFLOW_EGRESS_ENFORCE_NEW", "0")
    assert enforce_new_points() is False


_ENGINES = [("redis", 6379), ("elasticsearch", 9200), ("influxdb", 8086), ("clickhouse", 8123),
            ("cassandra", 9042), ("mongodb", 27017), ("postgresql", 5432), ("mysql", 3306)]


@pytest.mark.parametrize("host", ["127.0.0.1", "10.0.0.1", "0.0.0.0"])
@pytest.mark.parametrize("fmt,port", _ENGINES)
def test_introspect_refuses_internal_targets(editor, fmt, port, host):
    src = {"kind": "database", "format": fmt, "host": host, "port": port, "database": "0",
           "table": "w1sec_t", "username": "w1sec", "password": "x"}
    r = editor.post("/api/v1/transfer/introspect",
                    json={"source": src, "destination": {"kind": "file_export", "format": "csv"}})
    source = (r.json() or {}).get("source") or {}
    assert REFUSED in str(source.get("error")), (fmt, host, r.status_code, r.text[:300])
    assert source.get("connected") is not True and not source.get("objects"), r.text[:300]


def test_redis_export_of_the_platform_keyspace_is_refused(editor):
    import redis

    client = redis.Redis(host="127.0.0.1", port=6379)
    try:
        client.hset("w1sec_ssrf:1", mapping={"id": "1", "secret": "platform-internal-value"})
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"no local Redis: {exc}")
    try:
        src = {"kind": "database", "format": "redis", "host": "127.0.0.1", "port": 6379, "database": "0",
               "table": "w1sec_ssrf"}
        body = {"source": src, "destination": {"kind": "file_export", "format": "csv", "table": "w1sec_out"},
                "mappings": [{"source": "id", "target": "id", "confidence": 1.0},
                             {"source": "secret", "target": "secret", "confidence": 1.0},
                             {"source": "redis_key", "target": "redis_key", "confidence": 1.0},
                             {"source": "redis_type", "target": "redis_type", "confidence": 1.0}],
                "async_mode": False}
        r = editor.post("/api/v1/transfer/execute", json=body)
        assert "platform-internal-value" not in r.text
        payload = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
        detail = payload.get("detail") if isinstance(payload.get("detail"), dict) else payload
        assert not (detail.get("success") and detail.get("records_transferred")), r.text[:400]
        assert REFUSED in r.text, r.text[:400]
    finally:
        client.delete("w1sec_ssrf:1")


def test_the_deployment_allowlist_still_admits_loopback(monkeypatch):
    from services.egress_guard import EgressRefused, assert_cfg_dial_allowed

    for name in ("DATAFLOW_EGRESS_ENFORCE_NEW", "DATAWRAP_EGRESS_ENFORCE_NEW", "DATAWRAP_EGRESS_ALLOWLIST"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.delenv("DATAFLOW_EGRESS_ALLOWLIST", raising=False)
    with pytest.raises(EgressRefused):
        assert_cfg_dial_allowed({"type": "redis", "host": "127.0.0.1"}, db_type="redis", point="redis_client")
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8")
    assert_cfg_dial_allowed({"type": "redis", "host": "127.0.0.1"}, db_type="redis", point="redis_client")
    # Metadata and link-local are never admitted, allowlist or not.
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "169.254.0.0/16,127.0.0.0/8")
    with pytest.raises(EgressRefused):
        assert_cfg_dial_allowed({"type": "redis", "host": "169.254.169.254"}, db_type="redis", point="redis_client")
