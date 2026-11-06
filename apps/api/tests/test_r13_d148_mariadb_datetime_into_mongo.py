"""D148 — MariaDB DATETIME into a new MongoDB collection once the zone is declared.

QA: MariaDB ``ts DATETIME`` / ``updated_at DATETIME(6)`` -> new Mongo
collection was refused as "TIMESTAMP_NTZ -> DATE" fidelity collapse, and still
refused after following the remediation (``assume_timezone:UTC`` /
``source_timezone=UTC``). A BSON date is a UTC millisecond instant: with the
zone declared, whole-millisecond values land intact (93ceda63e grades the
target as an instant). Without a zone the run must still ask for one, since
writing a wall clock as UTC is an invented zone.

Live MariaDB 11 -> MongoDB 7, preflight ON, strict, run twice (overwrite),
NULL timestamps included.
"""

from __future__ import annotations

import os
import socket
import uuid
from datetime import datetime

import pytest

from tests.r13_mapping_harness import isolated  # noqa: F401 - fixture

pytestmark = pytest.mark.timeout(300)


def _up(port: int) -> bool:
    try:
        socket.create_connection(("127.0.0.1", port), timeout=1).close()
        return True
    except OSError:
        return False


def _mariadb():
    pymysql = pytest.importorskip("pymysql")
    pw = os.environ.get("DATAFLOW_TEST_MARIADB_PASSWORD", "")
    if not pw or not _up(3307):
        pytest.skip("MariaDB not configured (DATAFLOW_TEST_MARIADB_PASSWORD, :3307)")
    cfg = dict(host="127.0.0.1", port=3307,
               database=os.environ.get("DATAFLOW_TEST_MARIADB_DB", "dataflow"),
               username=os.environ.get("DATAFLOW_TEST_MARIADB_USER", "root"), password=pw)
    conn = pymysql.connect(host=cfg["host"], port=cfg["port"], user=cfg["username"],
                           password=pw, database=cfg["database"], autocommit=True)
    return cfg, conn


_ROWS = [
    (1, "2024-01-01 10:00:00", "2024-01-01 10:00:00.123000"),
    (2, "2024-06-30 23:59:59", "2024-06-30 23:59:59.999000"),
    (3, None, None),
]


def _run(fake, cfg, table, **kw):
    pymongo = pytest.importorskip("pymongo")
    if not _up(27017):
        pytest.skip("MongoDB not reachable")
    from src.transfer.models import EndpointConfig
    from tests import r13_mapping_harness as h

    return h.execute(
        fake,
        EndpointConfig(kind="database", format="mariadb", table=table, **cfg),
        EndpointConfig(kind="database", format="mongodb", host="127.0.0.1", port=27017,
                       database="dataflow_test", collection=table),
        sync_mode="full_refresh_overwrite", **kw,
    )


def test_zone_declared_datetimes_land_as_bson_instants_twice(isolated):  # noqa: F811
    import pymongo

    cfg, conn = _mariadb()
    table = f"d148_{uuid.uuid4().hex[:8]}"
    cur = conn.cursor()
    client = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=3000)
    try:
        cur.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, ts DATETIME NULL, updated_at DATETIME(6) NULL)")
        cur.executemany(f"INSERT INTO {table} VALUES (%s, %s, %s)", _ROWS)  # nosec B608
        refused = _run(isolated, cfg, table)
        assert not refused.success
        # The carrier is named for what it is, not a calendar DATE.
        assert "BSON instant" in (refused.error or ""), refused.error
        for _ in range(2):
            result = _run(isolated, cfg, table, source_timezone="UTC")
            assert result.success, result.error
        docs = list(client["dataflow_test"][table].find({}, {"_id": 0}).sort("id", 1))
        assert [(d["id"], d.get("ts"), d.get("updated_at")) for d in docs] == [
            (1, datetime(2024, 1, 1, 10, 0, 0), datetime(2024, 1, 1, 10, 0, 0, 123000)),
            (2, datetime(2024, 6, 30, 23, 59, 59), datetime(2024, 6, 30, 23, 59, 59, 999000)),
            (3, None, None),
        ]
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        conn.close()
        client["dataflow_test"][table].drop()
        client.close()
