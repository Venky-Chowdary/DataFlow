"""D83 - a SASL_PLAINTEXT Kafka connector: Test, introspect and transfer dial one listener.

QA: a connector that declares ``security_protocol=SASL_PLAINTEXT`` passed
test_connector and list_connector_objects, then introspect_connector_schema,
plan_transfer and start_transfer failed: "SASL is configured without
security_protocol, so SASL_SSL was used". Test built its Kafka cfg with the
connector's ``extra``; the introspection tools flattened the saved connector
into host/port/credential keywords (plus ``connection_options``) and the
protocol declared under ``extra`` was dropped on the way.

Now Test and introspection build their cfg with one builder
(``kafka_connection_cfg``) and the flattening carries the Kafka listener keys,
so the three paths resolve the same client kwargs.
"""
from __future__ import annotations

import os
import uuid

import pytest

from test_d190_d194_d197_declared_string_types_e2e import transfer_client  # noqa: F401

SHAPES = {
    "extra": {"extra": {"security_protocol": "SASL_PLAINTEXT", "sasl_mechanism": "PLAIN"}},
    "form": {"schema": "SASL_PLAINTEXT", "database": "PLAIN"},
}


def _connector(shape: str, host: str = "kafka.internal.example", port: int = 9094) -> str:
    from services.connector_store import create_connector

    spec = {
        "name": f"d83-{shape}-{uuid.uuid4().hex[:6]}", "type": "kafka", "role": "source",
        "host": host, "port": port, "username": "svc", "password": "not-a-real-secret",
        "ssl": False, "workspace_id": "",
    }
    spec.update(SHAPES[shape])
    return str(create_connector(spec).id)


@pytest.mark.parametrize("shape", sorted(SHAPES))
def test_test_introspect_and_transfer_resolve_the_same_listener(transfer_client, monkeypatch, shape):
    pytest.importorskip("kafka")
    import connectors.kafka_reader as kafka_reader
    import connectors.kafka_writer as kafka_writer
    import services.copy_kafka_common as kafka_common
    from connectors.kafka_reader import _consumer_configs
    from src.ai.copilot.lifecycle_tools import test_connector
    from src.ai.copilot.schema_tools import (
        _endpoint_from_connector,
        _safe_connector,
        introspect_connector_schema,
    )
    from src.transfer.adapters import resolve_connector_config

    seen: dict[str, dict] = {}

    def _listener(cfg: dict) -> dict:
        kwargs = kafka_common.kafka_client_kwargs(cfg)
        return {k: kwargs.get(k) for k in ("security_protocol", "sasl_mechanism")}

    class _Producer:
        def close(self):
            return None

    def _test_producer(cfg, *args, **kwargs):
        seen["test"] = _listener(cfg)
        return _Producer()

    def _introspect_read(cfg, topic, **kwargs):
        seen["introspect"] = _listener(cfg)
        return {"id": "INTEGER"}, {}, None

    monkeypatch.setattr(kafka_common, "kafka_sasl_check", lambda cfg, timeout=5.0: None)
    monkeypatch.setattr(kafka_writer, "_producer", _test_producer)
    monkeypatch.setattr(kafka_reader, "infer_topic_schema", _introspect_read)

    cid = _connector(shape)
    tested = test_connector(connector_id=cid)
    assert tested.success and tested.output["ok"], tested.output
    topic = f"orders_{uuid.uuid4().hex[:6]}"
    info = introspect_connector_schema(connector_id=cid, table=topic)
    assert info.success, info.error

    conn, err = _safe_connector(cid, "", "start_transfer")
    assert err is None
    transfer_cfg = resolve_connector_config(_endpoint_from_connector(conn, table=topic))
    consumer_kwargs = _consumer_configs(transfer_cfg)
    seen["transfer"] = {
        k: consumer_kwargs.get(k) for k in ("security_protocol", "sasl_mechanism")
    }

    expected = {"security_protocol": "SASL_PLAINTEXT", "sasl_mechanism": "PLAIN"}
    assert seen == {"test": expected, "introspect": expected, "transfer": expected}


def test_flattened_connector_keeps_the_kafka_listener():
    """Every path that flattens a saved connector into keywords carries it."""
    from connectors.generic_sql import connection_options
    from services.copy_kafka_common import kafka_client_kwargs, kafka_connection_cfg

    saved = {
        "type": "kafka", "host": "b1", "port": 9094, "username": "svc", "password": "x",
        **SHAPES["extra"],
    }
    flat = {"host": "b1", "port": 9094, "username": "svc", "password": "x",
            **connection_options(saved)}
    built = kafka_connection_cfg(flat)
    assert kafka_client_kwargs(built)["security_protocol"] == "SASL_PLAINTEXT"
    # The builder keeps ``extra`` itself, so an unflattened cfg resolves the same.
    assert kafka_client_kwargs(kafka_connection_cfg(saved))["security_protocol"] == "SASL_PLAINTEXT"
    # A SQL connector's options are unchanged.
    assert connection_options({"type": "postgresql", "sslmode": "require"}) == {"sslmode": "require"}


_LIVE_BOOT = os.environ.get("DATAFLOW_LIVE_KAFKA_SASL_BOOTSTRAP", "")
_LIVE_PW = os.environ.get("DATAFLOW_LIVE_KAFKA_SASL_PASSWORD", "")


@pytest.mark.skipif(not (_LIVE_BOOT and _LIVE_PW), reason="no live SASL Kafka broker configured")
@pytest.mark.parametrize("shape", sorted(SHAPES))
def test_live_sasl_plaintext_connector_tests_introspects_and_plans(transfer_client, shape):
    from services.connector_store import create_connector
    from src.ai.copilot.lifecycle_tools import test_connector
    from src.ai.copilot.schema_tools import introspect_connector_schema, list_connector_objects
    from src.ai.copilot.transfer_tools import plan_transfer
    from tests.helpers.live_env import pg_creds, pg_up
    from tests.test_mx1_03_kafka_client_sasl_matrix import _live_cfg, _produce

    cfg = _live_cfg()
    topic = f"d83b_{shape}_{uuid.uuid4().hex[:8]}"
    _produce(cfg, topic, [{"id": i, "v": f"x{i}"} for i in range(1, 4)])
    spec = {
        "name": f"d83b-{uuid.uuid4().hex[:6]}", "type": "kafka", "role": "source",
        "host": cfg["host"], "port": cfg["port"], "username": cfg["username"],
        "password": cfg["password"], "ssl": False, "workspace_id": "", **SHAPES[shape],
    }
    cid = str(create_connector(spec).id)

    tested = test_connector(connector_id=cid)
    assert tested.success and tested.output["ok"], tested.output
    listed = list_connector_objects(connector_id=cid)
    assert listed.success and listed.output["connected"], listed.error
    info = introspect_connector_schema(connector_id=cid, table=topic)
    assert info.success, info.error
    assert [c["name"] for c in info.output["columns"]][:2] == ["id", "v"]
    if not pg_up():
        return
    c = pg_creds()
    dst = str(create_connector({
        "name": f"d83b-pg-{uuid.uuid4().hex[:6]}", "type": "postgresql", "role": "destination",
        "host": c["host"], "port": int(c["port"]), "database": c["database"],
        "username": c["username"], "password": c["password"], "schema": "public",
        "ssl": False, "workspace_id": "",
    }).id)
    plan = plan_transfer(
        source_connector_id=cid, source_table=topic, dest_connector_id=dst,
        dest_table=f"{topic}_d", sync_mode="full_refresh_overwrite",
    )
    assert plan.success, plan.error
