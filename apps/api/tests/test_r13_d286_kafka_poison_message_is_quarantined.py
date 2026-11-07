"""D286 — a Kafka topic with one non-JSON message loads, and the poison is quarantined.

QA: rt1103_tamper (13 JSON messages + '{not json at all') -> PostgreSQL append,
lenient, under QUARANTINE_ROW and under the remedy the error prescribed. Every
job failed at Validate with 0 rows; quarantine samples said "Source column
missing: _kafka_value / _kafka_poison"; the advice looped.

Measured on 3e258e90f, two causes:

* the plan's type census and Validate sample kept the poison record, so the
  plan carried ``_kafka_value`` / ``_kafka_poison`` columns the transfer reader
  never emits (it routes the message to the rejected-rows channel);
* with those columns gone, the run crashed on the poison finding itself
  ("unhashable type: 'dict'"): its ``row`` is the broker record, and the
  quarantine counters put ``row`` into a set / ``int()``.
"""

from __future__ import annotations

import json
import os
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.job_quarantine import (  # noqa: E402
    rebase_batch_rejects,
    quarantined_row_count,
    rows_with_findings,
)
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from tests.test_property2_golden_path_never_blocked import _FakeMongo  # noqa: E402

_BOOTSTRAP = os.environ.get("DATAFLOW_TEST_KAFKA_BOOTSTRAP", "localhost:9092")


class _Mongo(_FakeMongo):
    def update_job_fields(self, job_id, fields):
        self.jobs.setdefault(job_id, {}).update(fields or {})
        return True


def test_record_valued_findings_count_and_renumber():
    poison = {"row": {"_kafka_value": "{not json", "_kafka_offset": "13"}, "reason": "kafka poison message"}
    cell = {"row": 2, "column": "amt", "reason": "bad"}
    assert rows_with_findings([poison, cell, dict(cell)]) == 2
    assert quarantined_row_count([poison, cell]) == 2
    renumbered = rebase_batch_rejects([poison, cell], batch_start=100)
    assert renumbered[0]["row"] == poison["row"] and renumbered[1]["row"] == 102


def test_topic_with_one_poison_message_loads_the_rest(monkeypatch):
    try:
        import psycopg2
        from kafka import KafkaProducer

        producer = KafkaProducer(bootstrap_servers=_BOOTSTRAP, request_timeout_ms=5000)
        pg = psycopg2.connect(host="localhost", port=5432, database="dataflow", user="dataflow",
                              password="dataflow", connect_timeout=3)
        pg.autocommit = True
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"engine unavailable: {exc}")
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: _Mongo())
    tag = uuid.uuid4().hex[:8]
    topic, dst = f"r13_d286_{tag}", f"r13_d286_d_{tag}"
    names = ["a", "", "  ", None, "東京"]
    for i in range(1, 14):
        producer.send(topic, json.dumps({"id": i, "name": names[i % 5], "amt": i + 0.5}).encode())
    producer.send(topic, b"{not json at all")
    producer.flush()
    producer.close()
    cfg = {"host": "localhost", "port": 9092, "bootstrap_servers": _BOOTSTRAP}
    try:
        from connectors.kafka_reader import infer_topic_schema

        schema, _native, _warning = infer_topic_schema(cfg, topic)
        assert "_kafka_value" not in schema and "_kafka_poison" not in schema
        assert {"id", "name", "amt"} <= set(schema)
        req = TransferRequest(
            source=EndpointConfig(kind="database", format="kafka", host="localhost", port=9092,
                                  table=topic, database=topic,
                                  extra={"bootstrap_servers": _BOOTSTRAP, "start_position": "earliest"}),
            destination=EndpointConfig(kind="database", format="postgresql", host="localhost", port=5432,
                                       database="dataflow", username="dataflow", password="dataflow",
                                       schema="public", table=dst),
            mappings=[{"source": k, "target": k} for k in sorted(schema)],
            sync_mode="full_refresh_append",
            skip_preflight=True,
            validation_mode="lenient",
        )
        job_id = uuid.uuid4().hex[:24]
        result = UniversalTransferEngine().execute_tracked(req, job_id)
        assert result.success is True, result.error
        assert result.records_transferred == 13
        with pg.cursor() as cur:
            cur.execute(f'SELECT id, name FROM public."{dst}" ORDER BY id::int')
            landed = cur.fetchall()
        assert len(landed) == 13
        by_id = {int(r[0]): r[1] for r in landed}
        assert by_id[1] == "" and by_id[2] == "  " and by_id[3] is None and by_id[4] == "東京" and by_id[5] == "a"
        from services.quarantine_dlq import quarantine_details_from_dlq

        poison = [d for d in quarantine_details_from_dlq(job_id) if "poison" in str(d.get("reason"))]
        assert len(poison) == 1
        assert poison[0]["row"]["_kafka_value"] == "{not json at all"
    finally:
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{dst}"')
            cur.execute(f'DROP TABLE IF EXISTS public."{dst}_df_quarantine"')
        pg.close()
        try:
            from kafka import KafkaAdminClient

            KafkaAdminClient(bootstrap_servers=_BOOTSTRAP).delete_topics([topic])
        except Exception:  # noqa: BLE001
            pass
