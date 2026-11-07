"""MX1-03: a wrong SASL password or a plaintext/TLS mismatch must come back as a
classified error in under a second, not a 30 s "Unable to bootstrap".

kafka-python 3.0.11 never surfaces SaslAuthenticationFailedError for this case
(the broker's error-58 frame arrives with the close and is dropped), so
``copy_kafka_common.kafka_sasl_check`` runs one SASL exchange itself with the
library's own request and mechanism classes. Live cases need the local broker
(DATAFLOW_LIVE_KAFKA_SASL_BOOTSTRAP + DATAFLOW_LIVE_KAFKA_SASL_PASSWORD).
"""
from __future__ import annotations

import os
import socket
import threading
import time

import pytest

from services import copy_kafka_common as kc

SENTINEL = "S3ntinel-pw"


def _cfg(port: int, **over):
    cfg = {"host": "127.0.0.1", "port": port, "username": "svc", "password": SENTINEL,
           "security_protocol": "SASL_PLAINTEXT"}
    cfg.update(over)
    return cfg


class _FakePeer:
    """TCP peer that reads the first bytes and then replies/closes/stays silent."""

    def __init__(self, behaviour: str):
        self.behaviour = behaviour
        self.sock = socket.socket()
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(1)
        self.port = self.sock.getsockname()[1]
        self.accepted = 0
        threading.Thread(target=self._serve, daemon=True).start()

    def _serve(self):
        try:
            conn, _ = self.sock.accept()
        except OSError:
            return
        self.accepted += 1
        with conn:
            conn.settimeout(3)
            try:
                conn.recv(4096)
                if self.behaviour == "tls_alert":
                    conn.sendall(b"\x15\x03\x03\x00\x02\x02\x46")  # TLS fatal alert
                elif self.behaviour == "silent":
                    time.sleep(2)
            except OSError:
                pass

    def close(self):
        self.sock.close()


def test_no_credentials_means_no_check_and_no_socket():
    peer = _FakePeer("close")
    try:
        assert kc.kafka_sasl_check({"host": "127.0.0.1", "port": peer.port}) is None
        time.sleep(0.05)
        assert peer.accepted == 0
    finally:
        peer.close()


def test_mechanisms_the_check_cannot_run_are_skipped():
    assert kc.kafka_sasl_check(_cfg(1, sasl_mechanism="GSSAPI")) is None
    assert kc.kafka_sasl_check(_cfg(1, sasl_mechanism="OAUTHBEARER")) is None


def test_plaintext_sasl_against_tls_listener_names_security_protocol():
    peer = _FakePeer("tls_alert")
    try:
        t = time.monotonic()
        check = kc.kafka_sasl_check(_cfg(peer.port))
        assert time.monotonic() - t < 1.0
    finally:
        peer.close()
    assert check.outcome == "tls_mismatch"
    assert "security_protocol=SASL_SSL" in check.message
    assert SENTINEL not in check.message


def test_silent_peer_is_inconclusive_within_the_bound():
    peer = _FakePeer("silent")
    try:
        t = time.monotonic()
        check = kc.kafka_sasl_check(_cfg(peer.port), timeout=0.5)
        assert time.monotonic() - t < 1.5
    finally:
        peer.close()
    assert check.outcome == "inconclusive"
    assert SENTINEL not in check.message


def test_refused_port_is_inconclusive():
    s = socket.socket(); s.bind(("127.0.0.1", 0)); port = s.getsockname()[1]; s.close()
    check = kc.kafka_sasl_check(_cfg(port))
    assert check.outcome == "inconclusive"


def test_check_is_pinned_to_the_kafka_python_it_was_built_for():
    import kafka

    lo, hi = kc.KAFKA_SASL_CHECK_KAFKA_PYTHON
    ver = tuple(int(p) for p in kafka.__version__.split(".")[:2])
    assert lo <= ver < hi, (
        f"kafka-python {kafka.__version__} is outside {lo}..{hi}: re-verify "
        "kafka_sasl_check against the new client (and whether it now raises "
        "SaslAuthenticationFailedError itself), then move the pin."
    )


def test_test_kafka_returns_classified_error_without_building_a_producer(monkeypatch):
    from connectors import kafka_writer

    built = []
    monkeypatch.setattr(kafka_writer, "_producer", lambda cfg: built.append(cfg))
    monkeypatch.setattr(
        kc, "kafka_sasl_check",
        lambda cfg, timeout=5.0: kc.KafkaSaslCheck("auth_failed", "Kafka broker rejected the SASL PLAIN login"),
    )
    ok, msg = kafka_writer.test_kafka(host="b", port=9092, username="svc", password=SENTINEL)
    assert (ok, built) == (False, [])
    assert "rejected the SASL" in msg


def test_test_kafka_forwards_the_configured_security_protocol(monkeypatch):
    from connectors import kafka_writer

    seen = {}

    def fake_check(cfg, timeout=5.0):
        seen.update(kc.kafka_sasl_kwargs(cfg))
        return kc.KafkaSaslCheck("ok", "")

    monkeypatch.setattr(kc, "kafka_sasl_check", fake_check)
    monkeypatch.setattr(kafka_writer, "_producer", lambda cfg: type("P", (), {"close": lambda s: None})())
    kafka_writer.test_kafka(host="b", username="svc", password=SENTINEL,
                            security_protocol="SASL_PLAINTEXT")
    assert seen["security_protocol"] == "SASL_PLAINTEXT"


def _bootstrap_timeout():
    from kafka.errors import KafkaTimeoutError

    return KafkaTimeoutError("Unable to bootstrap from b:9092")


def test_bootstrap_failure_is_classified_and_chains_the_original(monkeypatch):
    original = _bootstrap_timeout()
    monkeypatch.setattr(kc, "kafka_sasl_check",
                        lambda cfg, timeout=5.0: kc.KafkaSaslCheck("auth_failed", "rejected"))
    with pytest.raises(kc.KafkaSaslCheckError) as info:
        with kc.kafka_classified_bootstrap(_cfg(1)):
            raise original
    assert info.value.check.outcome == "auth_failed"
    assert info.value.__cause__ is original


@pytest.mark.parametrize("outcome", ["ok", "inconclusive", None])
def test_unclassified_bootstrap_failure_reraises_the_same_instance(monkeypatch, outcome):
    original = _bootstrap_timeout()
    monkeypatch.setattr(kc, "kafka_sasl_check",
                        lambda cfg, timeout=5.0: outcome and kc.KafkaSaslCheck(outcome, ""))
    with pytest.raises(type(original)) as info:
        with kc.kafka_classified_bootstrap(_cfg(1)):
            raise original
    assert info.value is original


def test_happy_path_and_non_bootstrap_errors_never_run_the_check(monkeypatch):
    calls = []
    monkeypatch.setattr(kc, "kafka_sasl_check", lambda cfg, timeout=5.0: calls.append(cfg))
    with kc.kafka_classified_bootstrap(_cfg(1)):
        pass
    with pytest.raises(ValueError):
        with kc.kafka_classified_bootstrap(_cfg(1)):
            raise ValueError("not a bootstrap failure")
    assert calls == []


# --- live: local SASL_PLAINTEXT / PLAIN broker ------------------------------

_LIVE_BOOT = os.environ.get("DATAFLOW_LIVE_KAFKA_SASL_BOOTSTRAP", "")
_LIVE_PW = os.environ.get("DATAFLOW_LIVE_KAFKA_SASL_PASSWORD", "")
_LIVE_USER = os.environ.get("DATAFLOW_LIVE_KAFKA_SASL_USER", "admin")
live = pytest.mark.skipif(not (_LIVE_BOOT and _LIVE_PW), reason="no live SASL Kafka broker configured")


def _live_cfg(**over):
    host, _, port = _LIVE_BOOT.partition(":")
    cfg = {"host": host, "port": int(port or 9092), "username": _LIVE_USER,
           "password": _LIVE_PW, "security_protocol": "SASL_PLAINTEXT", "sasl_mechanism": "PLAIN"}
    cfg.update(over)
    return cfg


def _probe(cfg):
    from src.transfer.connector_registry import run_probe

    t = time.monotonic()
    ok, msg = run_probe("kafka", cfg)
    return ok, msg, time.monotonic() - t


@live
def test_live_wrong_password_is_classified_in_under_a_second():
    ok, msg, took = _probe(_live_cfg(password=SENTINEL))
    assert not ok and took < 1.5, (msg, took)
    assert msg.startswith("Kafka SASL: broker") and "rejected the PLAIN login" in msg
    assert SENTINEL not in msg


@live
def test_live_sasl_without_protocol_against_plaintext_broker_names_security_protocol():
    cfg = _live_cfg()
    cfg.pop("security_protocol")
    ok, msg, took = _probe(cfg)
    assert not ok and took < 1.5, (msg, took)
    assert msg.startswith("Kafka SASL: broker")
    assert "without security_protocol" in msg and "security_protocol=SASL_PLAINTEXT" in msg
    assert _LIVE_PW not in msg


@live
def test_live_correct_login_passes():
    ok, msg, took = _probe(_live_cfg())
    assert ok, msg
    assert took < 5


_LIVE_MECHS = os.environ.get("DATAFLOW_LIVE_KAFKA_SASL_MECHANISMS", "PLAIN").split(",")
_SCRAM = [m for m in ("SCRAM-SHA-256", "SCRAM-SHA-512") if m in _LIVE_MECHS]


def _scram_cfg(mech, **over):
    env = mech.replace("-", "_")
    user = os.environ.get(f"DATAFLOW_LIVE_KAFKA_SASL_USER_{env}", "")
    pw = os.environ.get(f"DATAFLOW_LIVE_KAFKA_SASL_PASSWORD_{env}", "")
    if not (user and pw):
        pytest.skip(f"no {mech} credentials configured")
    return _live_cfg(**{"sasl_mechanism": mech, "username": user, "password": pw, **over})


@live
@pytest.mark.skipif(bool(_SCRAM), reason="broker enables SCRAM")
def test_live_scram_against_plain_only_broker_is_mechanism_unsupported():
    check = kc.kafka_sasl_check(_live_cfg(sasl_mechanism="SCRAM-SHA-256"))
    assert check.outcome == "mechanism_unsupported"
    assert "SCRAM-SHA-256" in check.message and "PLAIN" in check.message


@live
@pytest.mark.parametrize("mech", _SCRAM or ["SCRAM-SHA-256"])
def test_live_scram_login_passes(mech):
    if not _SCRAM:
        pytest.skip("broker does not enable SCRAM")
    ok, msg, took = _probe(_scram_cfg(mech))
    assert ok, msg
    assert took < 5
    assert kc.kafka_sasl_check(_scram_cfg(mech)).outcome == "ok"


@live
@pytest.mark.parametrize("mech", _SCRAM or ["SCRAM-SHA-256"])
def test_live_scram_wrong_password_is_classified_in_under_a_second(mech):
    if not _SCRAM:
        pytest.skip("broker does not enable SCRAM")
    ok, msg, took = _probe(_scram_cfg(mech, password=SENTINEL))
    assert not ok and took < 1.5, (msg, took)
    assert msg.startswith("Kafka SASL: broker") and f"rejected the {mech} login" in msg
    assert SENTINEL not in msg


@live
def test_live_reader_classifies_after_bootstrap_failure_and_happy_path_skips_check(monkeypatch):
    from connectors import kafka_reader

    real = kc.kafka_sasl_check
    calls = []
    monkeypatch.setattr(kc, "kafka_sasl_check", lambda cfg, timeout=5.0: calls.append(1) or real(cfg, timeout))
    kafka_reader.list_topics(_live_cfg())
    assert calls == []
    with pytest.raises(kc.KafkaSaslCheckError) as info:
        kafka_reader.list_topics(_live_cfg(password=SENTINEL))
    assert info.value.check.outcome == "auth_failed"
    assert calls == [1]
    assert SENTINEL not in str(info.value)
