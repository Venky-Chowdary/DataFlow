"""QA D203 (round 10) - a Kafka number column keeps ONE rule over the whole run.

QA on 087ac4bf: Kafka decimals still flip between number and string across
messages or runs. Round 5 decided each field's type over the whole topic for
the plan (a census), but the run re-decided it per page: a source column the
plan typed text was re-typed from the first page's values whenever that page
held only numbers (``stream.py`` lets a page's native types replace a
"string" type). So a field that arrives as ``12.50``, ``"12.50"``, ``null``
and ``""`` was created DOUBLE when the first 500 messages were numbers, and
the page that carried ``""`` then failed or quarantined. With the same topic
the plan and the run disagreed, and two topics with the same values in a
different order got different destination types.

The rule (stated in the plan, one per column, from the declared or sampled
type, never from one message):

* JSON numbers mixed with numeric strings and null: DECIMAL, each string
  lands as the exact number it spells;
* any string that is not a plain decimal literal (here ``""``) keeps the
  column text, and every value lands as its JSON text, ``""`` stays ``""``
  and null stays NULL.

Live broker 127.0.0.1:9092, live PostgreSQL and MySQL; MCP start_transfer ->
confirm -> job; the deciding values sit after the first reader page; a second
run into the table the first one created.
"""
from __future__ import annotations

import json
import socket
import time
import uuid
from decimal import Decimal

import pytest

from tests.helpers.live_env import mysql_creds, mysql_up, pg_creds, pg_up

_BROKER = {"host": "127.0.0.1", "port": 9092}
_HEAD = 600  # more than one reader page (500) of numbers only


def _broker_up() -> bool:
    try:
        with socket.create_connection((_BROKER["host"], _BROKER["port"]), timeout=1):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(not _broker_up(), reason="Kafka 127.0.0.1:9092 not reachable")

# Raw JSON so 12.50 reaches the broker with its trailing zero.
_TAIL_VALUES = ['12.50', '"12.50"', 'null', '""', '"-0.5"', '7']


def _messages() -> list[str]:
    out = []
    for i in range(1, _HEAD + 1):
        out.append(f'{{"id": {i}, "price": 12.50, "cost": {i}.25, "label": "n{i}"}}')
    for j, value in enumerate(_TAIL_VALUES * 3, start=_HEAD + 1):
        cost = '"3.75"' if j % 2 else ("null" if j % 3 else "4.5")
        out.append(f'{{"id": {j}, "price": {value}, "cost": {cost}, "label": "t{j}"}}')
    return out


def _expected() -> dict[int, tuple]:
    """``id -> (price as text, cost as Decimal)``."""
    out: dict[int, tuple] = {}
    for body in _messages():
        doc = json.loads(body, parse_float=Decimal)
        price = doc["price"]
        if isinstance(price, Decimal):
            price = format(price, "f")
        elif isinstance(price, int):
            price = str(price)
        cost = doc["cost"]
        out[doc["id"]] = (price, None if cost is None else Decimal(str(cost)))
    return out


def _pg_conn():
    import psycopg2

    c = pg_creds()
    conn = psycopg2.connect(
        host=c["host"], port=c["port"], dbname=c["database"], user=c["username"], password=c["password"]
    )
    conn.autocommit = True
    return conn


def _mysql_conn():
    import pymysql

    c = mysql_creds()
    return pymysql.connect(
        host=c["host"], port=int(c["port"]), user=c["username"], password=c["password"],
        database=c["database"], autocommit=True, charset="utf8mb4",
    )


_DEST = {
    "postgresql": (pg_up, pg_creds, _pg_conn, '"{t}"'),
    "mysql": (mysql_up, mysql_creds, _mysql_conn, "`{t}`"),
}


@pytest.fixture
def route(request):
    engine = request.param
    if not _DEST[engine][0]():
        pytest.skip(f"{engine} not reachable")
    pytest.importorskip("kafka")
    from kafka import KafkaProducer

    from services.connector_store import create_connector, delete_connector
    from services.copy_kafka_common import kafka_admin

    sfx = uuid.uuid4().hex[:8]
    topic = table = f"r10d203_{sfx}"
    producer = KafkaProducer(bootstrap_servers=f"{_BROKER['host']}:{_BROKER['port']}")
    try:
        for n, body in enumerate(_messages()):
            producer.send(topic, key=str(n).encode(), value=body.encode())
        producer.flush(20)
    finally:
        producer.close()
    creds = _DEST[engine][1]()
    src = create_connector({
        "name": f"r10d203-kafka-{sfx}", "type": "kafka", "role": "source",
        "host": _BROKER["host"], "port": _BROKER["port"],
    })
    spec = {
        "name": f"r10d203-{engine}-{sfx}", "type": engine, "role": "destination",
        "host": creds["host"], "port": int(creds["port"]), "database": creds["database"],
        "username": creds["username"], "password": creds["password"], "ssl": False,
    }
    if engine == "postgresql":
        spec["schema"] = "public"
    dst = create_connector(spec)
    try:
        yield {"engine": engine, "topic": topic, "table": table, "src": src.name, "dst": dst.name}
    finally:
        conn = _DEST[engine][2]()
        try:
            with conn.cursor() as cur:
                for t in (table, f"{table}_df_quarantine"):
                    cur.execute(f"DROP TABLE IF EXISTS {_DEST[engine][3].format(t=t)}")
        finally:
            conn.close()
        delete_connector(src.id)
        delete_connector(dst.id)
        admin = kafka_admin(_BROKER)
        try:
            admin.delete_topics([topic])
        except Exception:  # noqa: BLE001 - best-effort cleanup
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
    job_id = spend_pilot_ack(staged.output["ack_id"], "r10-d203")["job_id"]
    deadline = time.time() + 240
    job: dict = {}
    while time.time() < deadline:
        job = dict(tools.execute("get_job", {"job_id": job_id}).output or {})
        if job.get("status") not in {None, "", "pending", "queued", "running", "starting"}:
            break
        time.sleep(1.0)
    return dict(staged.output or {}), job


def _types(route: dict) -> dict[str, str]:
    conn = _DEST[route["engine"]][2]()
    try:
        with conn.cursor() as cur:
            schema = "'public'" if route["engine"] == "postgresql" else "DATABASE()"
            cur.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                f"WHERE table_schema = {schema} AND table_name = %s", (route["table"],),
            )
            return {str(k): str(v).lower() for k, v in cur.fetchall()}
    finally:
        conn.close()


def _rows(route: dict) -> dict[int, tuple]:
    conn = _DEST[route["engine"]][2]()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT id, price, cost FROM {_DEST[route['engine']][3].format(t=route['table'])}")
            return {
                int(i): (p, None if c is None else Decimal(str(c)).normalize())
                for i, p, c in cur.fetchall()
            }
    finally:
        conn.close()


@pytest.mark.parametrize("route", ["postgresql", "mysql"], indirect=True)
def test_a_kafka_number_column_keeps_one_rule_across_pages_and_runs(route):
    expected = {
        k: (p, None if c is None else c.normalize()) for k, (p, c) in _expected().items()
    }
    for n in (1, 2):
        staged, job = _transfer(route)
        assert job.get("status") == "completed", (n, job.get("error") or job.get("message"))
        types = _types(route)
        # price carries "" -> one text column; cost (numbers + numeric
        # strings + null) -> one exact numeric column. Never re-decided per page.
        assert types.get("price") in {"text", "character varying", "varchar", "longtext", "mediumtext"}, (n, types)
        assert types.get("cost") in {"numeric", "decimal"}, (n, types)
        rows = _rows(route)
        assert len(rows) == len(expected), n
        assert rows == expected, n
        assert int(job.get("quarantine_row_count") or 0) == 0, n
        notes = str((staged.get("plan") or {}).get("source_type_notes") or "")
        assert "price: JSON numbers and non-numeric strings in one column - typed TEXT" in notes
        assert "cost: JSON numbers and numeric strings in one column - typed DECIMAL" in notes


def test_a_json_number_keeps_its_own_spelling_on_the_wire():
    """Unit-level: 12.50 and "12.50" are one spelling in a text column."""
    from connectors.kafka_debezium_bridge import kafka_value_to_row
    from connectors.kafka_reader import _records_to_batch

    docs = [b'{"p": 12.50}', b'{"p": "12.50"}', b'{"p": null}', b'{"p": ""}', b'{"p": 1e-7}']
    batch = _records_to_batch([kafka_value_to_row(d) for d in docs], None, None)
    cells = [row[batch.headers.index("p")] for row in batch.rows]
    from services.value_serializer import SQL_NULL_SENTINEL

    assert cells == ["12.50", "12.50", SQL_NULL_SENTINEL, "", "1e-7"]
    # Typing is unchanged: a binary64-exact fraction is still a double.
    assert _records_to_batch([kafka_value_to_row(b'{"p": 12.50}')], None, None).meta[
        "native_types"
    ]["p"] == "DOUBLE"
