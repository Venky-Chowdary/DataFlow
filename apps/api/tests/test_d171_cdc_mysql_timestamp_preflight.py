"""D171 — MySQL ``TIMESTAMP`` blocked CDC preflight as "cannot fit the carrier".

``cdc_incremental`` MySQL -> PostgreSQL ``TIMESTAMPTZ`` was blocked by g3f
("N value(s) ... cannot fit the destination carrier"), while the same column and
the same destination through ``incremental_upsert`` passed.

Both routes judge a different population. ``incremental_upsert`` walks the
table through the Execute reader, which attaches UTC to a ``TIMESTAMP`` (its
digits are the stored UTC instant). A CDC run writes a changelog, so Validate
keeps the preview rows instead (``_iter_table_population_for_preflight``), and
the Pilot/MCP preview is a raw query that returns the same column as a naive
wall-clock string. A naive value cannot enter ``TIMESTAMPTZ``, so every
non-null value of a column Execute loads cleanly was refused (D147 and D155 are
the same shape for BSON dates and Oracle TSTZ).

Live MySQL and PostgreSQL; skipped when either is not reachable.
"""

from __future__ import annotations

import os
import socket
import uuid

import pytest

MY = dict(
    host=os.getenv("DATAFLOW_TEST_MYSQL_HOST") or "127.0.0.1",
    port=int(os.getenv("DATAFLOW_TEST_MYSQL_PORT") or 3306),
    database=os.getenv("DATAFLOW_TEST_MYSQL_DB") or "dataflow",
    username=os.getenv("DATAFLOW_TEST_MYSQL_USER") or "dataflow",
    password=os.getenv("DATAFLOW_TEST_MYSQL_PASSWORD") or "dataflow",
)
PG = dict(
    host=os.getenv("DATAFLOW_PG_HOST") or "127.0.0.1",
    port=int(os.getenv("PGPORT") or 5432),
    database=os.getenv("PGDATABASE") or "dataflow",
    username=os.getenv("PGUSER") or "dataflow",
    password=os.getenv("PGPASSWORD") or "dataflow",
)


def _up(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=1.5):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(
    not (_up(MY["port"]) and _up(PG["port"])), reason="MySQL and PostgreSQL not reachable"
)


def _connector(payload: dict) -> str:
    from services.connector_store import create_connector

    created = create_connector({"name": payload["type"] + uuid.uuid4().hex[:6], **payload})
    return getattr(created, "id", None) or created["id"]


@pytest.fixture()
def tables():
    import psycopg2
    import pymysql

    name = "d171_" + uuid.uuid4().hex[:8]
    my = pymysql.connect(
        host=MY["host"], port=MY["port"], user=MY["username"], password=MY["password"],
        database=MY["database"], autocommit=True,
    )
    pg = psycopg2.connect(
        host=PG["host"], port=PG["port"], user=PG["username"], password=PG["password"],
        dbname=PG["database"],
    )
    pg.autocommit = True
    with my.cursor() as cur:
        cur.execute(
            f"CREATE TABLE `{name}` (id INT PRIMARY KEY, label VARCHAR(20), "
            "wall DATETIME NULL, tstz TIMESTAMP NULL)"
        )
        for i in range(1, 26):
            cur.execute(
                f"INSERT INTO `{name}` VALUES (%s, %s, %s, %s)",
                (
                    i,
                    f"n{i}",
                    f"2024-05-0{1 + i % 9} 10:11:12",
                    None if i % 5 == 0 else f"2024-06-0{1 + i % 9} 01:02:03",
                ),
            )
    with pg.cursor() as cur:
        cur.execute(
            f'CREATE TABLE "{name}" (id INT PRIMARY KEY, label VARCHAR(20), '
            "wall TIMESTAMP, tstz TIMESTAMPTZ)"
        )
    try:
        yield name
    finally:
        with my.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{name}`")
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{name}"')
        my.close()
        pg.close()


@pytest.fixture()
def connectors():
    from services.connector_store import delete_connector

    src = _connector({"type": "mysql", **MY})
    dst = _connector({"type": "postgresql", **PG})
    yield src, dst
    delete_connector(src)
    delete_connector(dst)


def _g3f(planned) -> dict:
    preflight = (planned.output or {}).get("preflight") or {}
    return next(g for g in preflight["gates"] if g["id"] == "g3f_population_fit")


def test_cdc_plan_judges_a_mysql_timestamp_as_the_instant_execute_reads(tables, connectors):
    from src.ai.copilot import transfer_tools

    src, dst = connectors
    planned = transfer_tools.plan_transfer(
        source_connector_id=src,
        source_table=tables,
        dest_connector_id=dst,
        dest_table=tables,
        sync_mode="cdc_incremental",
    )
    assert planned.success, planned.error
    gate = _g3f(planned)
    assert "exceeds the destination carrier" not in gate["message"], gate["message"]
    assert gate["status"] != "block", gate["message"]
    preflight = planned.output["preflight"]
    assert not [b for b in preflight["blockers"] if b.get("id") == "g3f_population_fit"]


def test_preview_sample_hands_the_gates_the_rows_execute_reads(tables, connectors):
    from src.ai.copilot.transfer_tools import _sample_rows

    src, _dst = connectors
    rows = _sample_rows({"id": src, "type": "mysql"}, tables)
    assert rows
    from services.value_serializer import SQL_NULL_SENTINEL

    stamped = [r["tstz"] for r in rows if r.get("tstz") not in (None, "", SQL_NULL_SENTINEL)]
    assert stamped
    # A TIMESTAMP is an instant: the offset travels with it. A DATETIME is a
    # wall clock and stays zoneless for the declared source_timezone to read.
    assert all(str(v).endswith(("+00:00", "Z")) for v in stamped), stamped
    assert all("+" not in str(r["wall"]) and not str(r["wall"]).endswith("Z") for r in rows)
