"""D151 (QA round 10) - a Kafka source job's FIRST run loads the topic, typed.

QA on 087ac4bf: a Kafka-source job fails, then passes on retry; it "loaded 0
rows" or failed "has no columns or is empty", intermittently. Every earlier
test ran against the local broker one loopback hop away, where the consumer
group join and the first fetch both finish inside the first 800 ms poll.

The real path: a full refresh reads under the route's consumer group. The
group join can land at the very end of a poll, before the consumer has looked
up its start offsets or fetched a byte, and ``read_topic_batch`` took that
first empty assigned poll for "end of topic". On a broker a few hundred
milliseconds away (a managed cluster in another region) that is nearly every
fresh group's first page. The run's first page then had no headers ("no
columns or is empty"), and each continuation page re-joins the group, so a
later page could end the read early as well.

This test puts the live broker (127.0.0.1:9092) 150 ms away through a TCP
latency relay that every broker connection of the run is dialled through, and
runs each route ONCE on a fresh topic and a fresh route (so a fresh group):
MCP ``start_transfer`` -> ``/api/v1/copilot/confirm`` -> job, into live
PostgreSQL and live MySQL, then a second run into the table the first created.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import socket
import threading
import time
import uuid
from decimal import Decimal

import pytest

from test_d190_d194_d197_declared_string_types_e2e import _wait_job, transfer_client  # noqa: F401
from tests.helpers.live_env import mysql_creds, mysql_up, pg_creds, pg_up

_BROKER = {"host": "127.0.0.1", "port": 9092}
_ONE_WAY_LATENCY_S = 0.150
_N = 700  # two reader pages of 500


def _broker_up() -> bool:
    try:
        with socket.create_connection((_BROKER["host"], _BROKER["port"]), timeout=1):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(not _broker_up(), reason="Kafka 127.0.0.1:9092 not reachable")


def _message(i: int) -> dict:
    return {
        "id": i,
        "amount": round(i * 1.25, 2),
        "qty": i % 7,
        "ts": f"2024-03-01T10:{i // 60 % 60:02d}:{i % 60:02d}Z",
        "name": ["a", "", "  pad  ", None, "é中\U0001f600"][i % 5],
    }


class _LatencyRelay:
    """A TCP relay that delays every chunk in each direction (a distant broker)."""

    def __init__(self, target_port: int, delay_s: float) -> None:
        self.target_port, self.delay_s = target_port, delay_s
        self.loop = asyncio.new_event_loop()
        self.port = 0
        ready = threading.Event()
        threading.Thread(target=self._serve, args=(ready,), daemon=True).start()
        assert ready.wait(10)

    def _serve(self, ready: threading.Event) -> None:
        asyncio.set_event_loop(self.loop)

        async def pipe(reader, writer):
            queue: asyncio.Queue = asyncio.Queue()

            async def pump_in():
                while True:
                    data = await reader.read(65536)
                    await queue.put((time.monotonic() + self.delay_s, data))
                    if not data:
                        return

            async def pump_out():
                while True:
                    due, data = await queue.get()
                    wait = due - time.monotonic()
                    if wait > 0:
                        await asyncio.sleep(wait)
                    if not data:
                        writer.close()
                        return
                    writer.write(data)
                    await writer.drain()

            await asyncio.gather(pump_in(), pump_out(), return_exceptions=True)

        async def handle(client_r, client_w):
            try:
                broker_r, broker_w = await asyncio.open_connection("127.0.0.1", self.target_port)
            except OSError:
                client_w.close()
                return
            await asyncio.gather(
                pipe(client_r, broker_w), pipe(broker_r, client_w), return_exceptions=True
            )

        async def start():
            server = await asyncio.start_server(handle, "127.0.0.1", 0)
            self.port = server.sockets[0].getsockname()[1]
            ready.set()
            await server.serve_forever()

        self.loop.run_until_complete(start())

    def close(self) -> None:
        self.loop.call_soon_threadsafe(self.loop.stop)


@pytest.fixture
def distant_broker(monkeypatch):
    """Every kafka-python connection to the broker goes through the relay.

    The broker advertises 127.0.0.1:9092, so the relay must sit where the
    client dials each broker, not only at bootstrap.
    """
    pytest.importorskip("kafka")
    import kafka.net.manager as manager

    relay = _LatencyRelay(_BROKER["port"], _ONE_WAY_LATENCY_S)
    original = manager.create_connection

    async def via_relay(net, host, port, *args, **kwargs):
        if int(port) == _BROKER["port"]:
            host, port = "127.0.0.1", relay.port
        return await original(net, host, port, *args, **kwargs)

    monkeypatch.setattr(manager, "create_connection", via_relay)
    yield relay
    relay.close()


@pytest.fixture
def topic():
    from kafka import KafkaProducer
    from kafka.admin import NewTopic

    from services.copy_kafka_common import kafka_admin

    name = f"r10k_{uuid.uuid4().hex[:10]}"
    admin = kafka_admin(_BROKER)
    try:
        admin.create_topics([NewTopic(name, 1, 1)])
    finally:
        admin.close()
    producer = KafkaProducer(bootstrap_servers=f"{_BROKER['host']}:{_BROKER['port']}")
    try:
        for i in range(1, _N + 1):
            producer.send(name, key=str(i).encode(), value=json.dumps(_message(i)).encode())
        producer.flush(30)
    finally:
        producer.close()
    yield name
    admin = kafka_admin(_BROKER)
    try:
        admin.delete_topics([name])
    except Exception:  # noqa: BLE001 - best-effort cleanup
        pass
    finally:
        admin.close()


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


def _dest_connector(engine: str) -> str:
    from services.connector_store import create_connector

    creds = _DEST[engine][1]()
    spec = {
        "name": f"r10k-{engine}-{uuid.uuid4().hex[:6]}", "type": engine, "role": "destination",
        "host": creds["host"], "port": int(creds["port"]), "database": creds["database"],
        "username": creds["username"], "password": creds["password"], "ssl": False,
        "workspace_id": "",
    }
    if engine == "postgresql":
        spec["schema"] = "public"
    return str(create_connector(spec).id)


def _kafka_connector() -> str:
    from services.connector_store import create_connector

    return str(create_connector({
        "name": "r10k-src-" + uuid.uuid4().hex[:6], "type": "kafka", "role": "source",
        "host": _BROKER["host"], "port": _BROKER["port"], "database": "", "ssl": False,
        "workspace_id": "",
    }).id)


def _drop(engine: str, table: str) -> None:
    conn = _DEST[engine][2]()
    try:
        with conn.cursor() as cur:
            for t in (table, f"{table}_df_quarantine"):
                cur.execute(f"DROP TABLE IF EXISTS {_DEST[engine][3].format(t=t)}")
    finally:
        conn.close()


def _column_types(engine: str, table: str) -> dict[str, str]:
    conn = _DEST[engine][2]()
    try:
        with conn.cursor() as cur:
            if engine == "postgresql":
                cur.execute(
                    "SELECT column_name, data_type FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND table_name = %s", (table,),
                )
            else:
                cur.execute(
                    "SELECT column_name, data_type FROM information_schema.columns "
                    "WHERE table_schema = DATABASE() AND table_name = %s", (table,),
                )
            return {str(k): str(v).lower() for k, v in cur.fetchall()}
    finally:
        conn.close()


def _rows(engine: str, table: str) -> dict[int, tuple]:
    conn = _DEST[engine][2]()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT id, amount, qty, ts, name FROM {_DEST[engine][3].format(t=table)}")
            out: dict[int, tuple] = {}
            for rid, amount, qty, ts, name in cur.fetchall():
                if isinstance(ts, dt.datetime):
                    ts = (ts if ts.tzinfo else ts.replace(tzinfo=dt.timezone.utc)).astimezone(dt.timezone.utc)
                assert int(rid) not in out, ("duplicate id", rid)
                out[int(rid)] = (Decimal(str(amount)), int(qty), ts, name)
            return out
    finally:
        conn.close()


def _expected() -> dict[int, tuple]:
    out = {}
    for i in range(1, _N + 1):
        m = _message(i)
        ts = dt.datetime.fromisoformat(m["ts"].replace("Z", "+00:00"))
        out[i] = (Decimal(str(m["amount"])), m["qty"], ts, m["name"])
    return out


def _run(client, **kwargs) -> dict:
    from src.ai.copilot.transfer_tools import start_transfer

    staged = start_transfer(validation_mode="strict", **kwargs)
    assert staged.success, {
        "error": staged.error,
        "blockers": ((staged.output or {}).get("preflight") or {}).get("blockers"),
    }
    confirmed = client.post(
        "/api/v1/copilot/confirm", json={"ack_id": staged.output["ack_id"], "reason": "r10k"}
    )
    assert confirmed.status_code == 200, confirmed.text
    body = confirmed.json()
    assert body.get("ok") is True, body
    return _wait_job(str(body.get("job_id") or ""), timeout_s=600.0)


@pytest.mark.parametrize("engine", ["postgresql", "mysql"])
def test_first_run_of_a_fresh_route_loads_every_message_typed(
    distant_broker, transfer_client, topic, engine
):
    up = _DEST[engine][0]
    if not up():
        pytest.skip(f"{engine} not reachable")
    table = topic + "_d"
    _drop(engine, table)
    args = dict(
        source_connector_id=_kafka_connector(),
        source_table=topic,
        dest_connector_id=_dest_connector(engine),
        dest_table=table,
        sync_mode="full_refresh_overwrite",
    )
    try:
        for n in (1, 2):  # run 1 on a fresh group, then into the table run 1 made
            job = _run(transfer_client, **args)
            assert str(job.get("status") or "") == "completed", {
                "run": n, "status": job.get("status"), "error": job.get("error") or job.get("message"),
            }
            types = _column_types(engine, table)
            assert not all(t in {"text", "character varying", "varchar", "longtext"} for t in (
                types.get("id"), types.get("amount"), types.get("qty"),
            )), (n, types)
            assert types.get("id") in {"integer", "bigint", "int"}, (n, types)
            assert types.get("qty") in {"integer", "bigint", "int"}, (n, types)
            assert types.get("amount") in {"double precision", "numeric", "double", "decimal"}, (n, types)
            assert _rows(engine, table) == _expected(), n
    finally:
        _drop(engine, table)

