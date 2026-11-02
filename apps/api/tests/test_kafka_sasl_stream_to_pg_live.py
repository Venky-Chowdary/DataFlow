"""Live: Kafka (SASL) → PostgreSQL through execute_tracked, two runs.

Run 1 loads the topic, run 2 picks up only the new messages from the group's
committed offset (at-least-once; nothing re-read on a clean commit), Gate-8
passes on the destination, and the broker holds the committed offsets.
Skips unless DATAFLOW_LIVE_KAFKA_SASL_* is set and PostgreSQL is reachable.
"""
from __future__ import annotations

import os
import uuid
from unittest.mock import patch

import pytest

from tests.test_mx1_03_kafka_client_sasl_matrix import _live_cfg, _produce, live
from tests.test_property2_golden_path_never_blocked import _FakeMongo

MECHANISMS = [m for m in os.environ.get("DATAFLOW_LIVE_KAFKA_SASL_MECHANISMS", "PLAIN").split(",") if m]


class _Mongo(_FakeMongo):
    def update_job_fields(self, *a, **k):
        return None


def _pg():
    sa = pytest.importorskip("sqlalchemy")
    url = sa.engine.URL.create(
        "postgresql+psycopg2",
        username=os.environ.get("P2_PG_USER", "dataflow"),
        password=os.environ.get("P2_PG_PASSWORD", "dataflow"),
        host=os.environ.get("P2_PG_HOST", "127.0.0.1"),
        port=int(os.environ.get("P2_PG_PORT", "5432")),
        database=os.environ.get("P2_PG_DB", "dataflow"),
    )
    engine = sa.create_engine(url)
    try:
        with engine.connect():
            pass
    except Exception as exc:
        pytest.skip(f"PostgreSQL not reachable: {type(exc).__name__}")
    return sa, engine, url


@live
@pytest.mark.parametrize("mechanism", MECHANISMS)
def test_live_kafka_sasl_stream_to_pg_resumes_from_committed_offset(monkeypatch, mechanism):
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    sa, engine, url = _pg()
    from services.copy_kafka_common import kafka_admin
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    user = os.environ.get(f"DATAFLOW_LIVE_KAFKA_SASL_USER_{mechanism.replace('-', '_')}") or None
    pw = os.environ.get(f"DATAFLOW_LIVE_KAFKA_SASL_PASSWORD_{mechanism.replace('-', '_')}") or None
    kcfg = _live_cfg(sasl_mechanism=mechanism, **({"username": user, "password": pw} if user and pw else {}))
    topic = f"mx103_stream_{uuid.uuid4().hex[:10]}"
    group = f"g_{topic}"
    source = EndpointConfig(
        kind="database", format="kafka", host=kcfg["host"], port=kcfg["port"], table=topic,
        username=kcfg["username"], password=kcfg["password"],
        extra={"security_protocol": "SASL_PLAINTEXT", "sasl_mechanism": mechanism, "group_id": group},
    )
    dest = EndpointConfig(
        kind="database", format="postgresql", host=url.host, port=url.port, database=url.database,
        schema="public", table=topic, username=url.username, password=url.password,
    )

    def run():
        req = TransferRequest(source=source, destination=dest, sync_mode="incremental_append",
                              skip_preflight=True, validation_mode="strict")
        with patch("src.transfer.engine.get_mongodb_service", return_value=_Mongo()):
            return UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24])

    def pg_counts():
        with engine.connect() as c:
            return tuple(c.execute(sa.text(
                f'select count(*), count(distinct id), coalesce(sum(cast(amount as int)), 0) from "{topic}"'
            )).one())

    try:
        _produce(kcfg, topic, [{"id": i, "amount": i * 10} for i in range(1, 4)])
        first = run()
        assert first.success, first.error
        assert first.records_transferred == 3
        assert first.reconciliation.get("passed") is True, first.reconciliation
        assert pg_counts() == (3, 3, 60)

        _produce(kcfg, topic, [{"id": i, "amount": i * 10} for i in range(4, 6)])
        second = run()
        assert second.success, second.error
        assert second.records_transferred == 2, "run 2 must read only the new messages"
        assert second.reconciliation.get("passed") is True, second.reconciliation
        assert pg_counts() == (5, 5, 150)

        admin = kafka_admin(kcfg)
        try:
            offsets = admin.list_group_offsets({group: None})[group]
        finally:
            admin.close()
        assert sum(om.offset for tp, om in offsets.items() if tp.topic == topic) == 5
    finally:
        with engine.begin() as c:
            c.execute(sa.text(f'drop table if exists "{topic}"'))
        engine.dispose()
