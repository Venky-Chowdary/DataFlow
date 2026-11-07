"""D259: ``source_timezone`` on naive Kafka timestamps into a new table.

The declaration makes ``ts`` an instant (``TIMESTAMPTZ`` + ``assume_timezone:UTC``),
but the create-new stamp kept the carrier read off the naive samples
(``TIMESTAMP``), so preflight blocked on a fidelity collapse and a signed run
refused every row as "timezone-aware value into NTZ destination".
"""

from __future__ import annotations

import contextlib
from datetime import datetime, timezone

import pytest
from kafka.errors import KafkaError

from tests._r4_temporal_harness import (  # noqa: F401 - fixtures
    connector,
    mysql_exec,
    pg_exec,
    run,
    tag,
    transfer_client,
    up,
)

_NAIVE = [
    (1, "2024-03-10T02:30:00.012345"),
    (2, "2024-11-03T01:30:00.500000"),
    (3, "2025-01-02T03:04:05.000001"),
]


def _kafka_topic() -> tuple[str, dict]:
    if not up("127.0.0.1", 9092):
        pytest.skip("Kafka 127.0.0.1:9092 not reachable")
    pytest.importorskip("kafka")
    from connectors.kafka_writer import _producer

    topic = tag("d259k")
    kafka = {"host": "127.0.0.1", "port": 9092}
    producer = _producer(kafka)
    try:
        for i, ts in _NAIVE:
            producer.send(topic, key=str(i).encode(), value={"id": i, "ts": ts})
        producer.flush(10)
    finally:
        producer.close()
    return topic, kafka


def _cleanup(topic: str, kafka: dict) -> None:
    from services.copy_kafka_common import kafka_admin

    admin = kafka_admin(kafka)
    try:
        with contextlib.suppress(KafkaError):
            admin.delete_topics([topic])
    finally:
        admin.close()


def _expected() -> dict[int, datetime]:
    return {
        i: datetime.fromisoformat(ts).replace(tzinfo=timezone.utc) for i, ts in _NAIVE
    }


def _as_utc(value) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


@pytest.mark.parametrize("dest", ["postgresql", "mysql"])
def test_d259_declared_utc_loads_every_naive_row_into_the_new_table(
    transfer_client, dest
):
    from services.connector_store import create_connector

    topic, kafka = _kafka_topic()
    source = str(create_connector({
        "name": tag("d259-kafka"), "type": "kafka", "role": "source",
        "host": "127.0.0.1", "port": 9092, "database": "", "ssl": False,
        "workspace_id": "", "extra": {"group_id": f"g_{topic}"},
    }).id)
    out = topic + "_out"
    try:
        job = run(
            transfer_client, source_connector_id=source, source_table=topic,
            dest_connector_id=connector(dest, "destination"), dest_table=out,
            sync_mode="full_refresh_overwrite", source_timezone="UTC",
        )
        assert int(job.get("rejected_rows") or 0) == 0, job.get("rejected_rows")
        if dest == "postgresql":
            got = pg_exec(f'SELECT "id", "ts" FROM public."{out}" ORDER BY "id"')
            kind = dict(pg_exec(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_name=%s", (out,),
            ))["ts"]
            assert kind == "timestamp with time zone", kind
        else:
            got = mysql_exec(f"SELECT `id`, `ts` FROM `{out}` ORDER BY `id`")
        assert {int(i): _as_utc(v) for i, v in got} == _expected()
    finally:
        if dest == "postgresql":
            pg_exec(f'DROP TABLE IF EXISTS public."{out}"')
        else:
            mysql_exec(f"DROP TABLE IF EXISTS `{out}`")
        _cleanup(topic, kafka)
