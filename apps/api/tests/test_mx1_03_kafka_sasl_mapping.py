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
