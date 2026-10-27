"""Shared Kafka identity-COPY helpers.

Dest COUNT is ``destination_row_count`` / log-end minus log-start
watermarks — never producer ack, never poll count, never leftover
compaction MERGE. Same bootstrap+topic declines. Cross-endpoint
consume+produce declines (identity COPY stays on one cluster; MirrorMaker 2
/ Cluster Linking is not this path). Occupancy is counted **before**
delete. Desktop-lab Kafka / Redpanda on :9092 is not a customer-tenant
PRODUCTION_SKU.
"""

from __future__ import annotations

import logging
import re
import socket
import ssl
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

from services.copy_fast_path import FastPathResult, FastPathUnavailable

logger = logging.getLogger(__name__)

_KAFKA_FAMILY = frozenset({
    "kafka",
    "apache_kafka",
    "confluent_kafka",
    "confluent",
    "redpanda",
    "msk",
    "amazon_msk",
})

# Mapping stamps are opaque — identity COPY does not decode JSON / Avro.
_KAFKA_COPY_SAFE_TYPES = frozenset({
    "json",
    "string",
    "text",
    "keyword",
    "bytes",
    "binary",
    "long",
    "integer",
    "int",
    "boolean",
    "bool",
    "number",
    "double",
    "float",
})

_TOPIC_OK = re.compile(r"^[a-zA-Z0-9._-]+$")
_TOPIC_MAX = 249


def kafka_family_name(name: str) -> str:
    n = (name or "").strip().lower()
    if n in _KAFKA_FAMILY:
        return "kafka"
    return n


def kafka_type_is_copy_safe(declared: str) -> bool:
    raw = (declared or "").strip().lower()
    if not raw:
        return True
    base = raw.split("<", 1)[0].split("(", 1)[0].strip()
    return base in _KAFKA_COPY_SAFE_TYPES


def kafka_topic(table: str) -> str:
    name = (table or "").strip()
    if not name:
        raise FastPathUnavailable("Kafka topic required")
    if any(ch in name for ch in "*,?[]\\/ "):
        raise FastPathUnavailable("Kafka COPY refuses glob characters in the topic")
    if not _TOPIC_OK.match(name) or len(name) > _TOPIC_MAX:
        raise FastPathUnavailable("Kafka COPY refuses this topic name")
    if name.startswith("__") or name in {".", ".."}:
        raise FastPathUnavailable("Kafka COPY refuses internal topic names")
    return name


def kafka_endpoint_key(cfg: dict[str, Any]) -> str:
    from connectors.kafka_reader import _bootstrap

    raw = str(_bootstrap(cfg) or "").strip().lower()
    raw = raw.replace("localhost", "127.0.0.1")
    return raw or "127.0.0.1:9092"


def kafka_object_id(cfg: dict[str, Any], topic: str) -> tuple[str, str]:
    return (kafka_endpoint_key(cfg), kafka_topic(topic))


def kafka_proxy_fail_closed(cfg: dict[str, Any]) -> bool:
    from connectors.write_resilience import is_public_proxy_host

    return any(
        is_public_proxy_host(str(cfg.get(key) or ""))
        for key in ("host", "connection_string", "dsn")
    )


def kafka_client_kwargs(cfg: dict[str, Any]) -> dict[str, Any]:
    """Shared kafka-python kwargs. No JSON serializers — identity is bytes."""
    from connectors.kafka_reader import _bootstrap

    bootstrap = _bootstrap(cfg)
    kwargs: dict[str, Any] = {
        "bootstrap_servers": bootstrap,
        "request_timeout_ms": 30000,
    }
    kwargs.update(kafka_sasl_kwargs(cfg))
    return kwargs


# The saved connector form keeps the mechanism in ``database``. Only a real
# SASL mechanism name is read from there — a database/cluster label is not.
_SASL_MECHANISMS = frozenset(
    {"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "GSSAPI", "OAUTHBEARER"}
)


def kafka_sasl_kwargs(cfg: dict[str, Any]) -> dict[str, Any]:
    """kafka-python SASL kwargs from a connector config — the one mapping.

    Consumer, producer and privilege probe all authenticate through this, so a
    broker that accepts the reader cannot reject the writer or the probe. Empty
    when there is no username+password (plaintext / mTLS-less dev brokers).
    """
    extra = cfg.get("extra") if isinstance(cfg.get("extra"), dict) else {}
    username = str(cfg.get("username") or extra.get("username") or "")
    password = str(cfg.get("password") or cfg.get("api_key") or extra.get("password") or "")
    if not (username and password):
        return {}
    declared = kafka_security_options(cfg)
    security = declared.get("security_protocol", "")
    out: dict[str, Any] = {
        "security_protocol": security if security in {"SASL_SSL", "SASL_PLAINTEXT"} else "SASL_SSL",
        "sasl_mechanism": declared.get("sasl_mechanism") or "PLAIN",
        "sasl_plain_username": username,
        "sasl_plain_password": password,
    }
    if out["security_protocol"] == "SASL_SSL":
        out["ssl_context"] = ssl.create_default_context()
    return out


def is_sasl_mechanism(name: Any) -> bool:
    """True when a form's ``database`` slot holds a SASL mechanism, not a topic."""
    return str(name or "").strip().upper() in _SASL_MECHANISMS


_SECURITY_PROTOCOLS = frozenset({"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"})


def kafka_security_options(cfg: dict[str, Any]) -> dict[str, str]:
    """The security protocol / SASL mechanism a Kafka connector declares.

    Read from wherever the connector keeps them (top level, ``extra``, or the
    form's ``schema`` / ``database`` slots) and returned as explicit keys. A
    path that rebuilds a Kafka cfg from host/port/credentials splats this in;
    leaving it out made SASL_PLAINTEXT connectors fall back to SASL_SSL on
    introspect, write and Gate-8 read-back while Test passed (D83).
    """
    extra = cfg.get("extra") if isinstance(cfg.get("extra"), dict) else {}
    out: dict[str, str] = {}
    protocol = _configured_security_protocol(cfg)
    if protocol in _SECURITY_PROTOCOLS:
        out["security_protocol"] = protocol
    database = str(cfg.get("database") or "").strip().upper()
    mechanism = str(
        extra.get("sasl_mechanism")
        or cfg.get("sasl_mechanism")
        or (database if is_sasl_mechanism(database) else "")
    ).strip()
    if mechanism:
        out["sasl_mechanism"] = mechanism
    return out


def _configured_security_protocol(cfg: dict[str, Any]) -> str:
    extra = cfg.get("extra") if isinstance(cfg.get("extra"), dict) else {}
    return str(
        cfg.get("security_protocol") or extra.get("security_protocol") or cfg.get("schema") or ""
    ).upper()


# kafka-python 3.0.x drops the broker's SaslAuthenticate error frame (it arrives
# with the connection close), so a wrong password only ever surfaces as a
# bootstrap timeout. kafka_sasl_check runs one SASL exchange with the library's
# own request/mechanism classes to classify it. Pinned: re-verify on upgrade.
KAFKA_SASL_CHECK_KAFKA_PYTHON = ((3, 0), (3, 1))
_CHECKABLE_MECHANISMS = frozenset({"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"})


@dataclass(frozen=True)
class KafkaSaslCheck:
    """outcome: ok | auth_failed | tls_mismatch | mechanism_unsupported | inconclusive."""

    outcome: str
    message: str


class KafkaSaslCheckError(RuntimeError):
    def __init__(self, check: KafkaSaslCheck):
        super().__init__(check.message)
        self.check = check


class _TlsPeer(Exception):
    pass


def _kafka_python_supported() -> bool:
    try:
        import kafka

        ver = tuple(int(p) for p in str(kafka.__version__).split(".")[:2])
    except Exception:  # noqa: BLE001 - absent/odd client: skip the check
        return False
    lo, hi = KAFKA_SASL_CHECK_KAFKA_PYTHON
    return lo <= ver < hi


def _sasl_round_trip(sock, protocol, request, deadline: float, *, plaintext: bool):
    protocol.send_request(request)
    sock.sendall(protocol.send_bytes())
    first = True
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise socket.timeout()
        sock.settimeout(remaining)
        data = sock.recv(65536)
        if not data:
            raise ConnectionError("broker closed the connection")
        # A TLS listener answers plaintext with a TLS record (alert/handshake).
        if plaintext and first and data[:1] in (b"\x15", b"\x16") and data[1:2] == b"\x03":
            raise _TlsPeer()
        first = False
        responses = protocol.receive_bytes(data)
        if responses:
            return responses[0][1]


def kafka_sasl_check(cfg: dict[str, Any], timeout: float = 5.0) -> KafkaSaslCheck | None:
    """Classify a SASL login on one blocking socket in well under ``timeout``.

    None when the check does not apply (no SASL login, a mechanism it cannot
    run, or an unpinned kafka-python). Messages never carry credentials.
    """
    sasl = kafka_sasl_kwargs(cfg)
    mechanism = str(sasl.get("sasl_mechanism") or "").upper()
    if not sasl or mechanism not in _CHECKABLE_MECHANISMS or not _kafka_python_supported():
        return None
    from kafka.errors import for_code
    from kafka.net.sasl import get_sasl_mechanism
    from kafka.protocol.parser import KafkaProtocol
    from kafka.protocol.sasl import SaslAuthenticateRequest, SaslHandshakeRequest

    from connectors.kafka_reader import _bootstrap

    first = _bootstrap(cfg).split(",")[0].strip()
    host, _, port_s = first.rpartition(":")
    host = host or first
    port = int(port_s) if port_s.isdigit() else 9092
    where = f"{host}:{port}"
    protocol_name = sasl["security_protocol"]
    explicit = _configured_security_protocol(cfg) in {"SASL_SSL", "SASL_PLAINTEXT"}
    deadline = time.monotonic() + min(float(timeout), 5.0)

    def inconclusive(exc: BaseException) -> KafkaSaslCheck:
        return KafkaSaslCheck(
            "inconclusive", f"Kafka SASL: check against {where} was inconclusive ({type(exc).__name__})."
        )

    try:
        sock = socket.create_connection((host, port), timeout=max(0.05, deadline - time.monotonic()))
    except OSError as exc:
        return inconclusive(exc)
    try:
        if protocol_name == "SASL_SSL":
            try:
                sock = sasl["ssl_context"].wrap_socket(sock, server_hostname=host)
            except ssl.SSLCertVerificationError as exc:
                return inconclusive(exc)
            except (ssl.SSLError, ConnectionError) as exc:
                lead = (
                    "SASL is configured without security_protocol, so SASL_SSL was used"
                    if not explicit
                    else "security_protocol=SASL_SSL is set"
                )
                return KafkaSaslCheck(
                    "tls_mismatch",
                    f"Kafka SASL: broker {where} did not complete a TLS handshake ({type(exc).__name__}): "
                    f"{lead}. If this listener is plaintext, set security_protocol=SASL_PLAINTEXT.",
                )
        parser = KafkaProtocol(client_id="dataflow-sasl-check")
        plaintext = protocol_name == "SASL_PLAINTEXT"
        handshake = SaslHandshakeRequest(mechanism=mechanism)
        handshake.API_VERSION = 1
        response = _sasl_round_trip(sock, parser, handshake, deadline, plaintext=plaintext)
        enabled = [str(m) for m in (response.mechanisms or [])]
        if response.error_code or mechanism not in enabled:
            return KafkaSaslCheck(
                "mechanism_unsupported",
                f"Kafka SASL: broker {where} does not enable mechanism {mechanism}; "
                f"enabled: {', '.join(enabled) or 'none'}.",
            )
        login = get_sasl_mechanism(mechanism)(host=host, **sasl)
        while not login.is_done():
            auth = SaslAuthenticateRequest(auth_bytes=login.auth_bytes())
            auth.API_VERSION = 1
            response = _sasl_round_trip(sock, parser, auth, deadline, plaintext=False)
            if response.error_code:
                return KafkaSaslCheck(
                    "auth_failed",
                    f"Kafka SASL: broker {where} rejected the {mechanism} login "
                    f"({for_code(response.error_code).__name__}): check the username and password.",
                )
            login.receive(response.auth_bytes)
        if not login.is_authenticated():
            return KafkaSaslCheck(
                "auth_failed",
                f"Kafka SASL: broker {where} did not accept the {mechanism} login: "
                "check the username and password.",
            )
        return KafkaSaslCheck("ok", f"Kafka SASL: {mechanism} login accepted by {where}.")
    except _TlsPeer:
        return KafkaSaslCheck(
            "tls_mismatch",
            f"Kafka SASL: broker {where} answered with TLS, but security_protocol is "
            f"{protocol_name}: set security_protocol=SASL_SSL.",
        )
    except Exception as exc:  # noqa: BLE001 - never classify on an unknown error
        return inconclusive(exc)
    finally:
        sock.close()


def _is_bootstrap_failure(exc: BaseException) -> bool:
    try:
        import kafka.errors as kerrors
    except ImportError:
        return False
    kinds = tuple(
        k for k in (getattr(kerrors, "KafkaTimeoutError", None), getattr(kerrors, "NoBrokersAvailable", None))
        if isinstance(k, type)
    )
    return bool(kinds) and isinstance(exc, kinds)


@contextmanager
def kafka_classified_bootstrap(cfg: dict[str, Any]):
    """Re-raise a bootstrap failure as KafkaSaslCheckError when the SASL check
    can say why; otherwise the original exception instance, unchanged. The
    check runs only after a failure, so the happy path pays nothing."""
    try:
        yield
    except Exception as exc:
        if not _is_bootstrap_failure(exc):
            raise
        check = kafka_sasl_check(cfg)
        if check is None or check.outcome in {"ok", "inconclusive"}:
            raise
        raise KafkaSaslCheckError(check) from exc


def kafka_dest_count(cfg: dict[str, Any], topic: str) -> int:
    from services.dest_precount import destination_row_count

    n = destination_row_count(
        "kafka",
        {**cfg, "type": "kafka", "format": "kafka"},
        schema="",
        table_name=kafka_topic(topic),
    )
    if n is None:
        raise ValueError(f"Kafka dest COUNT unmeasured for {topic}")
    return int(n)


def kafka_admin(cfg: dict[str, Any]):
    from kafka import KafkaAdminClient

    with kafka_classified_bootstrap(cfg):
        return KafkaAdminClient(**kafka_client_kwargs(cfg))


def kafka_byte_consumer(cfg: dict[str, Any]):
    from kafka import KafkaConsumer

    kwargs = kafka_client_kwargs(cfg)
    kwargs.update(
        {
            "enable_auto_commit": False,
            "auto_offset_reset": "earliest",
            "consumer_timeout_ms": 2000,
            "key_deserializer": None,
            "value_deserializer": None,
        }
    )
    with kafka_classified_bootstrap(cfg):
        return KafkaConsumer(**kwargs)


def kafka_byte_producer(cfg: dict[str, Any]):
    from kafka import KafkaProducer

    kwargs = kafka_client_kwargs(cfg)
    kwargs.update(
        {
            "acks": "all",
            "retries": 5,
            "key_serializer": None,
            "value_serializer": None,
            "linger_ms": 5,
        }
    )
    with kafka_classified_bootstrap(cfg):
        return KafkaProducer(**kwargs)


def kafka_dest_can_write(dest_cfg: dict[str, Any]) -> None:
    """Dest credentials must reach the cluster before dest mutation.

    Produce / create / delete use dest credentials so dest write ACL is
    the boundary. This probe lists topics — it does not produce a record.
    """
    admin = kafka_admin(dest_cfg)
    try:
        admin.list_topics()
    except Exception as exc:
        raise FastPathUnavailable(
            "dest credentials cannot administer Kafka cluster for identity COPY"
        ) from exc
    finally:
        _close_quiet(admin)


def kafka_topic_exists(cfg: dict[str, Any], topic: str) -> bool:
    name = kafka_topic(topic)
    consumer = kafka_byte_consumer(cfg)
    try:
        parts = consumer.partitions_for_topic(name)
        return bool(parts)
    finally:
        _close_quiet(consumer)


def kafka_partition_count(cfg: dict[str, Any], topic: str) -> int:
    name = kafka_topic(topic)
    consumer = kafka_byte_consumer(cfg)
    try:
        parts = consumer.partitions_for_topic(name)
        return len(parts) if parts else 0
    finally:
        _close_quiet(consumer)


def _wait_partitions(cfg: dict[str, Any], topic: str, expected: int) -> None:
    deadline = time.monotonic() + 15
    last = 0
    while time.monotonic() < deadline:
        last = kafka_partition_count(cfg, topic)
        if last == int(expected):
            return
        time.sleep(0.2)
    raise ValueError(
        f"Kafka topic {topic} metadata not ready (partitions={last}, expected={expected})"
    )


def kafka_snapshot_watermarks(
    cfg: dict[str, Any], topic: str
) -> tuple[int, list[Any], dict[Any, int], dict[Any, int]]:
    """beginning_offsets + end_offsets before consume. Does not decode records."""
    from kafka import TopicPartition

    name = kafka_topic(topic)
    consumer = kafka_byte_consumer(cfg)
    try:
        parts = consumer.partitions_for_topic(name)
        if not parts:
            raise FastPathUnavailable("Kafka source topic missing")
        tps = [TopicPartition(name, int(p)) for p in sorted(parts)]
        begin = consumer.beginning_offsets(tps)
        end = consumer.end_offsets(tps)
        source_count = int(sum(int(end[tp]) - int(begin[tp]) for tp in tps))
        begin_i = {tp: int(begin[tp]) for tp in tps}
        end_i = {tp: int(end[tp]) for tp in tps}
        return source_count, tps, begin_i, end_i
    finally:
        _close_quiet(consumer)


def kafka_create_topic(cfg: dict[str, Any], topic: str, *, num_partitions: int) -> None:
    from kafka.admin import NewTopic

    name = kafka_topic(topic)
    parts = max(1, int(num_partitions))
    admin = kafka_admin(cfg)
    try:
        deadline = time.monotonic() + 30
        last_exc: Exception | None = None
        while time.monotonic() < deadline:
            existing = set(admin.list_topics() or [])
            if name in existing:
                admin.wait_for_topics([name], timeout_ms=15000)
                _wait_partitions(cfg, name, parts)
                return
            try:
                admin.create_topics(
                    [NewTopic(name=name, num_partitions=parts, replication_factor=1)],
                    timeout_ms=15000,
                    validate_only=False,
                    raise_errors=True,
                    wait_for_metadata=True,
                )
                admin.wait_for_topics([name], timeout_ms=15000)
                _wait_partitions(cfg, name, parts)
                return
            except Exception as exc:
                last_exc = exc
                text = str(exc).lower()
                if "marked for deletion" in text or "already exists" in text:
                    time.sleep(0.25)
                    continue
                raise
        raise ValueError(f"Kafka create_topics failed for {name}: {last_exc}")
    finally:
        _close_quiet(admin)


def kafka_delete_topic(cfg: dict[str, Any], topic: str) -> None:
    name = kafka_topic(topic)
    admin = kafka_admin(cfg)
    try:
        existing = set(admin.list_topics() or [])
        if name not in existing:
            return
        admin.delete_topics([name], timeout_ms=15000, raise_errors=True)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if name not in set(admin.list_topics() or []):
                return
            time.sleep(0.2)
        raise ValueError(f"Kafka delete_topics did not drop {name}")
    finally:
        _close_quiet(admin)


def kafka_copy_bytes(
    *,
    source_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    src_topic: str,
    dest_topic: str,
    tps: list[Any],
    begin: dict[Any, int],
    end: dict[Any, int],
    pin_partition: bool,
) -> None:
    """Consume raw bytes from source; produce identical bytes with dest credentials.

    No consumer group (must not move the transfer cursor). No deserializer.
    Consume only up to the snapshot end watermark.
    """
    src = kafka_topic(src_topic)
    dest = kafka_topic(dest_topic)
    del src
    consumer = kafka_byte_consumer(source_cfg)
    producer = kafka_byte_producer(dest_cfg)
    try:
        consumer.assign(tps)
        for tp in tps:
            consumer.seek(tp, begin[tp])
        idle = 0
        while True:
            caught_up = True
            for tp in tps:
                if consumer.position(tp) < end[tp]:
                    caught_up = False
                    break
            if caught_up:
                break
            batch = consumer.poll(timeout_ms=1000, max_records=500)
            if not batch:
                idle += 1
                if idle > 30:
                    raise ValueError(
                        "Kafka consume stalled before snapshot end watermark"
                    )
                continue
            idle = 0
            for tp, records in batch.items():
                snap_end = end[tp]
                for rec in records:
                    if rec.offset >= snap_end:
                        continue
                    send_kw: dict[str, Any] = {
                        "topic": dest,
                        "key": rec.key,
                        "value": rec.value,
                        "headers": list(rec.headers) if rec.headers else None,
                        "timestamp_ms": rec.timestamp,
                    }
                    if pin_partition:
                        send_kw["partition"] = rec.partition
                    producer.send(**send_kw)
        producer.flush(timeout=60)
    finally:
        _close_quiet(producer)
        _close_quiet(consumer)


def skip_complete_kafka(
    *,
    source_count: int,
    dest_count: int,
    extra_snapshot: dict[str, Any] | None = None,
) -> FastPathResult:
    proof = f"dest_count:{dest_count}"
    snapshot = {
        "copy_workers": 1,
        "copy_split": "skip",
        "copy_partitions": 1,
        "partitions_skipped": 1,
        "partitions_loaded": 0,
        "shard_mode": "topic",
        **(extra_snapshot or {}),
    }
    return FastPathResult(
        rows_copied=source_count,
        source_rows=source_count,
        source_checksum=proof,
        target_rows=dest_count,
        target_checksum=proof,
        source_snapshot=snapshot,
        proof_scope="dest_count_equals_source_snapshot_count",
    )


def _close_quiet(client: Any) -> None:
    try:
        client.close()
    except Exception:
        return
