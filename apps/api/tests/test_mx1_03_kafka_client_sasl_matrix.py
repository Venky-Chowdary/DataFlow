"""MX1-03 bug class: every Kafka client must take SASL from the shared mapping.

commit_kafka_offsets (e1064410) and the Debezium bridge behind read_topic_batch
built bare ``KafkaConsumer(bootstrap_servers=...)`` clients that never
authenticate on a SASL broker. The AST guard fails on any Kafka client built
outside ``_consumer_configs`` / ``kafka_client_kwargs`` or outside
``kafka_classified_bootstrap``. Live cases need DATAFLOW_LIVE_KAFKA_SASL_*.
"""
from __future__ import annotations

import ast
import json
import os
import time
import uuid
from pathlib import Path

import pytest

API = Path(__file__).resolve().parents[1]
CLIENTS = {"KafkaConsumer", "KafkaProducer", "KafkaAdminClient",
           "AIOKafkaConsumer", "AIOKafkaProducer", "AIOKafkaAdminClient"}
CONFLUENT = {"Consumer", "Producer", "AdminClient", "DeserializingConsumer", "SerializingProducer"}
HELPERS = {"_consumer_configs", "kafka_client_kwargs"}
# Kafka client constructions allowed outside the shared helper. Keep empty.
ALLOWLIST: set[str] = set()


def _call_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return ""


def _uses_helper(expr: ast.AST) -> bool:
    return any(isinstance(n, ast.Call) and _call_name(n.func) in HELPERS for n in ast.walk(expr))


def kafka_client_violations(source: str, filename: str) -> tuple[int, list[str]]:
    tree = ast.parse(source)
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            root = node.module.split(".")[0]
            for alias in node.names:
                if (root in {"kafka", "aiokafka"} and alias.name in CLIENTS) or (
                    root == "confluent_kafka" and alias.name in CONFLUENT
                ):
                    names.add(alias.asname or alias.name)
    found, bad = 0, []

    def visit(node, func, withs):
        nonlocal found
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            func = node
        if isinstance(node, (ast.With, ast.AsyncWith)):
            withs = withs + [any(
                isinstance(i.context_expr, ast.Call)
                and _call_name(i.context_expr.func) == "kafka_classified_bootstrap"
                for i in node.items
            )]
        if isinstance(node, ast.Call):
            fname = _call_name(node.func)
            is_client = (isinstance(node.func, ast.Name) and fname in names) or (
                isinstance(node.func, ast.Attribute) and fname in CLIENTS
            )
            if is_client:
                found += 1
                where = f"{filename}:{node.lineno}"
                star = [k.value for k in node.keywords if k.arg is None]
                ok_kwargs = False
                for value in star:
                    if _uses_helper(value):
                        ok_kwargs = True
                    elif isinstance(value, ast.Name) and func is not None:
                        ok_kwargs = any(
                            isinstance(a, (ast.Assign, ast.AnnAssign))
                            and any(isinstance(t, ast.Name) and t.id == value.id
                                    for t in (a.targets if isinstance(a, ast.Assign) else [a.target]))
                            and a.value is not None and _uses_helper(a.value)
                            for a in ast.walk(func)
                        )
                if where not in ALLOWLIST:
                    if not ok_kwargs:
                        bad.append(f"{where} {fname}: kwargs not from {sorted(HELPERS)}")
                    if not any(withs):
                        bad.append(f"{where} {fname}: not inside kafka_classified_bootstrap")
        for child in ast.iter_child_nodes(node):
            visit(child, func, withs)

    visit(tree, None, [])
    return found, bad


def _sources():
    for path in API.rglob("*.py"):
        rel = path.relative_to(API).as_posix()
        if rel.startswith(("tests/", ".venv/", "venv/")) or "/node_modules/" in rel:
            continue
        yield rel, path.read_text(encoding="utf-8", errors="replace")


def test_every_kafka_client_uses_the_shared_sasl_helper_and_classified_bootstrap():
    total, violations = 0, []
    for rel, src in _sources():
        if "kafka" not in src.lower():
            continue
        n, bad = kafka_client_violations(src, rel)
        total += n
        violations += bad
    assert total >= 8, f"guard found only {total} Kafka clients; is the scan broken?"
    assert violations == [], "\n".join(violations)


def test_guard_catches_a_bare_client():
    bad_src = (
        "from kafka import KafkaConsumer as KC\n"
        "def f(cfg):\n"
        "    return KC(bootstrap_servers='b:9092')\n"
        "def g(cfg):\n"
        "    import kafka\n"
        "    return kafka.KafkaProducer(**cfg)\n"
    )
    n, bad = kafka_client_violations(bad_src, "x.py")
    assert n == 2 and len(bad) == 4


def test_bootstrap_keeps_an_explicit_port():
    from connectors.kafka_reader import _bootstrap

    assert _bootstrap({"bootstrap_servers": "b1:19092"}) == "b1:19092"
    assert _bootstrap({"host": "b1", "port": 19092}) == "b1:19092"
    assert _bootstrap({"host": "b1:9093,b2:9093"}) == "b1:9093,b2:9093"


def test_debezium_bridge_consumer_maps_sasl(monkeypatch):
    import kafka

    from connectors.kafka_debezium_bridge import KafkaDebeziumConsumer

    seen: dict = {}

    class _Consumer:
        DEFAULT_CONFIG = kafka.KafkaConsumer.DEFAULT_CONFIG

        def __init__(self, *topics, **kw):
            seen["topics"] = topics
            seen.update(kw)

    monkeypatch.setattr(kafka, "KafkaConsumer", _Consumer)
    KafkaDebeziumConsumer({"topic": "t", "host": "b", "port": 9093, "username": "svc",
                           "password": "s3", "security_protocol": "SASL_PLAINTEXT",
                           "group_id": "g"}).connect()
    assert seen["topics"] == ("t",)
    assert seen["security_protocol"] == "SASL_PLAINTEXT"
    assert (seen["sasl_plain_username"], seen["sasl_plain_password"]) == ("svc", "s3")
    assert (seen["group_id"], seen["enable_auto_commit"], seen["auto_offset_reset"]) == ("g", False, "earliest")


def test_read_topic_batch_hands_sasl_to_the_bridge(monkeypatch):
    from connectors import kafka_reader

    seen: dict = {}

    class _Bridge:
        def __init__(self, config):
            seen.update(config)

        def poll_rows(self, **_):
            return iter(())

        def has_assignment(self):
            return True

        def pending_offsets(self):
            return []

        def exhausted(self):
            return False

        def end_offsets_snapshot(self):
            return []

        def close(self):
            pass

    monkeypatch.setattr(kafka_reader, "KafkaDebeziumConsumer", _Bridge)
    kafka_reader.read_topic_batch(
        cfg={"host": "b", "port": 9093, "username": "svc", "password": "s3",
             "security_protocol": "SASL_PLAINTEXT"},
        topic="t",
    )
    from services.copy_kafka_common import kafka_sasl_kwargs

    assert kafka_sasl_kwargs(seen)["security_protocol"] == "SASL_PLAINTEXT"
    assert seen["topic"] == "t"


# --- live: every converted path against the local SASL broker ---------------

_LIVE_BOOT = os.environ.get("DATAFLOW_LIVE_KAFKA_SASL_BOOTSTRAP", "")
_LIVE_PW = os.environ.get("DATAFLOW_LIVE_KAFKA_SASL_PASSWORD", "")
live = pytest.mark.skipif(not (_LIVE_BOOT and _LIVE_PW), reason="no live SASL Kafka broker configured")


def _live_cfg(**over):
    host, _, port = _LIVE_BOOT.partition(":")
    cfg = {"host": host, "port": int(port or 9092), "security_protocol": "SASL_PLAINTEXT",
           "sasl_mechanism": "PLAIN", "username": os.environ.get("DATAFLOW_LIVE_KAFKA_SASL_USER", "admin"),
           "password": _LIVE_PW}
    cfg.update(over)
    return cfg


def _produce(cfg, topic, rows):
    from connectors.kafka_writer import _producer

    producer = _producer(cfg)
    try:
        for row in rows:
            producer.send(topic, value=row, key=str(row["id"]))
        producer.flush(10)
    finally:
        producer.close()


@pytest.fixture
def live_topic():
    topic = f"mx103_matrix_{uuid.uuid4().hex[:10]}"
    _produce(_live_cfg(), topic, [{"id": i, "amount": i * 10} for i in range(1, 4)])
    return topic


@live
def test_live_dest_precount_counts_over_sasl(live_topic):
    from services.dest_precount import _kafka_topic_record_count

    assert _kafka_topic_record_count(_live_cfg(), topic=live_topic) == 3


@live
def test_live_privilege_probe_over_sasl(live_topic):
    from services.destination_privilege_probe import _probe_kafka

    cfg = _live_cfg()
    res = _probe_kafka(host=cfg["host"], port=cfg["port"], connection_string="",
                       username=cfg["username"], password=cfg["password"],
                       security_protocol="SASL_PLAINTEXT", sasl_mechanism="PLAIN",
                       topic=live_topic, table_exists=True)
    assert res.status not in {"unavailable", "error", "unknown"}, res
    assert res.can_write is not False, res


@live
def test_live_wrong_password_is_classified_on_every_converted_path(live_topic):
    from connectors import kafka_reader
    from services import dest_precount
    from services.copy_kafka_common import KafkaSaslCheckError

    bad = _live_cfg(password="wrong-" + uuid.uuid4().hex[:6])
    for call in (
        lambda: kafka_reader.read_topic_batch(cfg=bad, topic=live_topic, limit=5),
        lambda: dest_precount._kafka_topic_record_count(bad, topic=live_topic),
    ):
        t = time.monotonic()
        with pytest.raises(KafkaSaslCheckError) as info:
            call()
        assert info.value.check.outcome == "auth_failed"
        assert bad["password"] not in json.dumps(str(info.value))
        assert time.monotonic() - t < 45
