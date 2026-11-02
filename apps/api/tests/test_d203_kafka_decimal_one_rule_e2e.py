"""D203 - a Kafka number column gets one exact numeric type for the whole run.

QA on 557f556c: Kafka decimals still flip between number and string depending
on the value. Round 4 widened INTEGER/DOUBLE inside ``merge_kafka_logicals``,
but the value classifier never saw a JSON number as a decimal: the exact JSON
decoder keeps a number binary64 would round (``12345678901234567890.12345``)
as ``Decimal``, and ``_kafka_value_to_logical`` typed a ``Decimal`` TEXT. One
such message turned the whole column into text; without it the same column was
``double precision``. A column that mixed JSON numbers with numeric strings
("2.50") went TEXT as well.

The rule now, decided over every value the run reads:

* JSON integers: INTEGER, BIGINT when one exceeds int32, DECIMAL beyond int64;
* JSON fractions: DOUBLE while binary64 holds every one of them exactly (the
  stored double prints back the JSON number), else DECIMAL - one fraction
  binary64 would round, or an integer past 2**53 beside a fraction, widens the
  column to DECIMAL; it never turns it into text;
* JSON numbers mixed with strings that are plain decimal literals ("2.50"):
  DECIMAL, and the plan says so for that column;
* a string that is not a plain decimal literal ("0012", "") keeps the column
  text; a column of strings alone stays declared text (D190/D194).

Live broker on 127.0.0.1:9092, live PostgreSQL; MCP start_transfer -> confirm
-> job, twice into the table the first run created.
"""

from __future__ import annotations

import json
import socket
import time
import uuid
from decimal import Decimal

import pytest

from tests.helpers.live_env import pg_creds, pg_up

_BROKER = {"host": "127.0.0.1", "port": 9092}
_BIG = "12345678901234567890.12345"

# Raw JSON text so the decimal reaches the broker exactly as written. The
# decisive values sit at the head of the topic, ahead of 60 plain messages,
# so a tail sample of 50 messages never sees them: the decision has to be
# the run's, not the sample's.
_HEAD = [
    '{"id": 1, "amt": 1, "mixed": 1, "ints": 1, "huge": 1, "strs": "2.50", "loose": 1, "name": "a", "dbl": 1}',
    '{"id": 2, "amt": 1.5, "mixed": 1.5, "ints": 3000000000, "huge": 2, "strs": "0012", "loose": "0012", "name": "", "dbl": 0.30000000000000004}',
    '{"id": 3, "amt": 0.1, "mixed": "2.50", "ints": null, "huge": 123456789012345678901234, "strs": "", "loose": 2, "name": "  ", "dbl": 1e-7}',
    f'{{"id": 4, "amt": {_BIG}, "mixed": 0.1, "ints": -7, "huge": null, "strs": null, "loose": null, "name": "é中\U0001f600", "dbl": null}}',
    '{"id": 5, "amt": 1e-7, "mixed": null, "ints": 0, "huge": 0, "strs": "x", "loose": 3, "name": null, "dbl": 2.5}',
    '{"id": 6, "amt": null, "mixed": "-0.5", "ints": 2, "huge": 3, "strs": " ", "loose": 4, "name": "z", "dbl": -0.5}',
]
_TAIL = [
    json.dumps({"id": i, "amt": 2, "mixed": 3, "ints": 4, "huge": 5, "strs": "y",
                "loose": 6, "name": "f", "dbl": 3})
    for i in range(7, 67)
]

_EXPECTED_TYPES = {
    "amt": "numeric", "mixed": "numeric", "ints": "bigint", "huge": "numeric",
    "strs": "text", "loose": "text", "dbl": "double precision",
}
_EXPECTED_HEAD = [
    (1, "1", "1", "1", "1", "2.50", "1", "a", "1"),
    (2, "1.5", "1.5", "3000000000", "2", "0012", "0012", "", "0.30000000000000004"),
    (3, "0.1", "2.50", None, "123456789012345678901234", "", "2", "  ", "1e-07"),
    (4, _BIG, "0.1", "-7", None, None, None, "é中\U0001f600", None),
    (5, "0.0000001", None, "0", "0", "x", "3", None, "2.5"),
    (6, None, "-0.5", "2", "3", " ", "4", "z", "-0.5"),
]


def _broker_up() -> bool:
    try:
        with socket.create_connection((_BROKER["host"], _BROKER["port"]), timeout=1):
            return True
    except OSError:
        return False


pytestmark = [
    pytest.mark.skipif(not _broker_up(), reason="Kafka 127.0.0.1:9092 not reachable"),
    pytest.mark.skipif(not pg_up(), reason="PostgreSQL not reachable"),
]


def _pg():
    import psycopg2

    cfg = pg_creds()
    conn = psycopg2.connect(
        host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
        user=cfg["username"], password=cfg["password"],
    )
    conn.autocommit = True
    return conn


@pytest.fixture(params=["head_then_tail"])
def route(request):
    pytest.importorskip("kafka")
    from kafka import KafkaProducer

    from services.connector_store import create_connector, delete_connector
    from services.copy_kafka_common import kafka_admin

    sfx = uuid.uuid4().hex[:8]
    topic, table = f"d203_{sfx}", f"d203_{sfx}"
    producer = KafkaProducer(bootstrap_servers=f"{_BROKER['host']}:{_BROKER['port']}")
    try:
        for n, body in enumerate(_HEAD + (_TAIL if request.param == "head_then_tail" else [])):
            producer.send(topic, key=str(n).encode(), value=body.encode())
        producer.flush(10)
    finally:
        producer.close()
    cfg = pg_creds()
    src = create_connector({
        "name": f"d203-kafka-{sfx}", "type": "kafka", "role": "source",
        "host": _BROKER["host"], "port": _BROKER["port"],
    })
    dst = create_connector({
        "name": f"d203-pg-{sfx}", "type": "postgresql", "role": "destination",
        "host": cfg["host"], "port": cfg["port"], "database": cfg["database"],
        "username": cfg["username"], "password": cfg["password"], "schema": "public",
        "ssl": False,
    })
    try:
        yield {"topic": topic, "table": table, "src": src.name, "dst": dst.name,
               "tail": request.param == "head_then_tail"}
    finally:
        conn = _pg()
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
            cur.execute(f'DROP TABLE IF EXISTS public."{table}_df_quarantine"')
        conn.close()
        delete_connector(src.id)
        delete_connector(dst.id)
        admin = kafka_admin(_BROKER)
        try:
            admin.delete_topics([topic])
        except Exception:
            pass
        finally:
            admin.close()


def _transfer(route: dict) -> tuple[dict, dict]:
    from conftest import spend_pilot_ack
    from src.ai.copilot.tools import DataPilotTools

    tools = DataPilotTools()
    staged = tools.execute("start_transfer", {
        "source_connector_name": route["src"], "source_table": route["topic"],
        "dest_connector_name": route["dst"], "dest_table": route["table"],
        "sync_mode": "full_refresh_overwrite", "validation_mode": "strict",
    })
    assert staged.success, staged.error
    job_id = spend_pilot_ack(staged.output["ack_id"], "d203-test")["job_id"]
    deadline = time.time() + 180
    job: dict = {}
    while time.time() < deadline:
        job = dict(tools.execute("get_job", {"job_id": job_id}).output or {})
        if job.get("status") not in {None, "", "pending", "queued", "running", "starting"}:
            break
        time.sleep(1.0)
    return dict(staged.output or {}), job


def _column_types(table: str) -> dict[str, str]:
    conn = _pg()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = %s", (table,),
            )
            return dict(cur.fetchall())
    finally:
        conn.close()


def _rows(table: str) -> list[tuple]:
    conn = _pg()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'SELECT id, amt::text, mixed::text, ints::text, huge::text, strs, '
                f'loose::text, name, dbl::text FROM public."{table}" ORDER BY id'
            )
            return cur.fetchall()
    finally:
        conn.close()


@pytest.mark.parametrize("route", ["head_then_tail", "six_messages"], indirect=True)
def test_kafka_numbers_get_one_exact_type_over_two_runs(route):
    for n in (1, 2):
        staged, job = _transfer(route)
        assert job.get("status") == "completed", (n, job.get("error") or job.get("message"))
        types = _column_types(route["table"])
        assert {c: types.get(c) for c in _EXPECTED_TYPES} == _EXPECTED_TYPES, n
        rows = _rows(route["table"])
        assert len(rows) == (66 if route["tail"] else 6), n
        assert rows[:6] == _EXPECTED_HEAD, n
        assert set(rows[6:]) == {
            (i, "2", "3", "4", "5", "y", "6", "f", "3") for i in range(7, 67 if route["tail"] else 7)
        }
        # Exact: the stored value is the JSON number, not its binary64 neighbour.
        conn = _pg()
        with conn.cursor() as cur:
            cur.execute(f'SELECT amt FROM public."{route["table"]}" WHERE id = 4')
            assert cur.fetchone()[0] == Decimal(_BIG)
        conn.close()
        # The plan states the declared decision for the mixed column.
        notes = str((staged.get("plan") or {}).get("source_type_notes") or "")
        assert "mixed: JSON numbers and numeric strings in one column - typed DECIMAL" in notes
        assert "loose: JSON numbers and non-numeric strings in one column - typed TEXT" in notes


def test_one_value_does_not_flip_the_column_type():
    """Unit-level: the per-column decision is the same whatever value arrives."""
    from connectors.kafka_reader import _records_to_batch
    from services.value_serializer import json_loads_exact

    def types(*docs: str) -> dict[str, str]:
        recs = [json_loads_exact(d) for d in docs]
        return _records_to_batch(recs, None, None).meta["native_types"]

    assert types('{"a": 1.5}', '{"a": 0.1}', '{"a": 1}')["a"] == "DOUBLE"
    assert types('{"a": 1.5}', f'{{"a": {_BIG}}}')["a"] == "DECIMAL"
    assert types(f'{{"a": {_BIG}}}', '{"a": 1.5}')["a"] == "DECIMAL"
    assert types('{"a": 9007199254740993}', '{"a": 1.5}')["a"] == "DECIMAL"
    assert types('{"a": 1}', '{"a": 2}')["a"] == "INTEGER"
    assert types('{"a": 1}', '{"a": 3000000000}')["a"] == "BIGINT"
    assert types('{"a": 1}', '{"a": 123456789012345678901234}')["a"] == "DECIMAL"
    assert types('{"a": 1}', '{"a": "2.50"}', '{"a": null}')["a"] == "DECIMAL"
    assert types('{"a": "2.50"}', '{"a": 1}')["a"] == "DECIMAL"
    assert types('{"a": 1}', '{"a": "0012"}')["a"] == "TEXT"
    assert types('{"a": 1}', '{"a": ""}')["a"] == "TEXT"
    assert types('{"a": 1}', '{"a": " 2.5"}')["a"] == "TEXT"
    # Strings alone stay the declared string (D190).
    assert types('{"a": "2.50"}', '{"a": "1"}')["a"] not in {"DECIMAL", "INTEGER", "DOUBLE"}
    # Booleans never become numbers.
    assert types('{"a": true}', '{"a": 1}')["a"] == "TEXT"
