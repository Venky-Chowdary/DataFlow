"""QA MX1-03 — a SASL Kafka connector "times out": the reader built a plaintext
consumer with no SASL settings, so topic listing / schema / reads against a
SASL broker never authenticated. The producer side already mapped SASL; the
consumer now uses the same shared mapping (``copy_kafka_common.kafka_client_kwargs``).
Live SASL broker verification is BLOCKED-infra (no broker in this environment).
"""

from __future__ import annotations

from connectors.kafka_reader import _consumer_configs


def test_consumer_maps_sasl_username_password():
    kw = _consumer_configs(
        {"host": "broker.example.com", "port": 9093, "username": "svc", "password": "s3",
         "security_protocol": "SASL_PLAINTEXT", "extra": {"sasl_mechanism": "SCRAM-SHA-256"}},
        consumer_timeout_ms=2000,
    )
    assert kw["security_protocol"] == "SASL_PLAINTEXT"
    assert kw["sasl_mechanism"] == "SCRAM-SHA-256"
    assert (kw["sasl_plain_username"], kw["sasl_plain_password"]) == ("svc", "s3")
    assert kw["consumer_timeout_ms"] == 2000


def test_consumer_sasl_defaults_to_ssl_plain_and_plaintext_without_login():
    kw = _consumer_configs({"host": "b", "username": "svc", "api_key": "k"})
    assert (kw["security_protocol"], kw["sasl_mechanism"]) == ("SASL_SSL", "PLAIN")
    assert "ssl_context" in kw
    plain = _consumer_configs({"host": "b"})
    assert not any(k.startswith("sasl") or k == "security_protocol" for k in plain)


# --- One SASL mapping for consumer, producer and privilege probe -----------

import pytest  # noqa: E402

_SASL_KEYS = ("security_protocol", "sasl_mechanism", "sasl_plain_username", "sasl_plain_password")


def _sasl_view(kwargs: dict) -> dict:
    view = {k: kwargs[k] for k in _SASL_KEYS if k in kwargs}
    view["ssl_context"] = "ssl_context" in kwargs
    return view


def _producer_kwargs(monkeypatch, cfg: dict) -> dict:
    kafka = pytest.importorskip("kafka")
    seen: dict = {}

    def fake_producer(**kwargs):
        seen.update(kwargs)
        return object()

    monkeypatch.setattr(kafka, "KafkaProducer", fake_producer)
    from connectors.kafka_writer import _producer

    _producer(cfg)
    return seen


def _probe_kwargs(monkeypatch, cfg: dict) -> dict:
    admin_mod = pytest.importorskip("kafka.admin")
    seen: dict = {}

    class _Stop(Exception):
        pass

    def fake_admin(**kwargs):
        seen.update(kwargs)
        raise _Stop

    monkeypatch.setattr(admin_mod, "KafkaAdminClient", fake_admin)
    from services.destination_privilege_probe import _probe_kafka

    # Same argument mapping as probe_destination_privileges(engine="kafka").
    with pytest.raises(_Stop):
        _probe_kafka(
            host=cfg.get("host", ""),
            port=cfg.get("port") or 9092,
            connection_string="",
            username=cfg.get("username", ""),
            password=cfg.get("password") or cfg.get("api_key", ""),
            security_protocol=cfg.get("schema", ""),
            sasl_mechanism=cfg.get("database") or "PLAIN",
            topic="t",
            table_exists=True,
        )
    return seen


@pytest.mark.parametrize(
    "cfg",
    [
        # Saved connector form: protocol in ``schema``, mechanism in ``database``.
        {"host": "b", "port": 9093, "username": "svc", "password": "s3",
         "schema": "SASL_PLAINTEXT", "database": "SCRAM-SHA-256"},
        {"host": "b", "username": "svc", "api_key": "k"},
        {"host": "b", "username": "svc", "password": "s3", "schema": "sasl_ssl",
         "database": "SCRAM-SHA-512"},
        {"host": "b"},
    ],
    ids=["scram256-plaintext", "api-key-default", "scram512-ssl", "no-login"],
)
def test_consumer_producer_and_probe_map_sasl_identically(monkeypatch, cfg):
    consumer = _sasl_view(_consumer_configs(dict(cfg)))
    producer = _sasl_view(_producer_kwargs(monkeypatch, dict(cfg)))
    probe = _sasl_view(_probe_kwargs(monkeypatch, dict(cfg)))
    assert consumer == producer == probe


def test_non_mechanism_database_does_not_become_the_sasl_mechanism(monkeypatch):
    cfg = {"host": "b", "username": "svc", "password": "s3", "database": "orders"}
    assert _sasl_view(_producer_kwargs(monkeypatch, cfg))["sasl_mechanism"] == "PLAIN"


def test_sasl_mapping_has_one_owner():
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    for rel in ("connectors/kafka_writer.py", "services/destination_privilege_probe.py"):
        assert "sasl_plain_username" not in (root / rel).read_text(), rel
