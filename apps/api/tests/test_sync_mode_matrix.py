"""Every sync mode, run twice, against every destination we can reach.

A transfer returning ``success=True`` proves nothing about a sync mode. The mode
*is* its second-run behaviour, and the live matrix only ever ran
``full_refresh_overwrite`` once against a fresh table — so no test had ever run
a mode twice, and two modes were broken on every destination:

* ``full_refresh_overwrite`` failed from run two, because the destination it had
  just created reported "exists, no columns" and every target type stayed
  pending.
* ``incremental_append`` failed from run two, because reading no rows past the
  watermark — the steady state of every schedule — was discarded as an
  unmeasured source rather than a measured zero.

Row counts are the only thing that separates the modes, so they are what this
asserts. A destination that appends under ``upsert`` doubles the customer's
data; one that re-reads everything under ``incremental`` makes a schedule a
full refresh with extra steps.
"""

from __future__ import annotations

import sys
import uuid
from pathlib import Path
from typing import Any

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.adapters import write_destination_database  # noqa: E402
from src.transfer.models import EndpointConfig  # noqa: E402
from tests.sync_mode_probe import (  # noqa: E402
    COLUMNS,
    EXPECTED_AFTER_TWO_RUNS,
    MAPPINGS,
    RECORDS,
    SCHEMA,
    run_mode,
    stream_contract,
)

MODES = sorted(EXPECTED_AFTER_TWO_RUNS)


# ── destinations ─────────────────────────────────────────────────────────────


def _pg_endpoint(table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="postgresql",
        host="localhost",
        port=5432,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        schema="public",
        table=table,
    )


def _pg_conn():
    psycopg2 = pytest.importorskip("psycopg2")
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="dataflow",
            user="dataflow",
            password="dataflow",
        )
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL unavailable: {exc}")
    conn.autocommit = True
    return conn


def _postgresql(table: str, _tmp: Path):
    conn = _pg_conn()

    def count() -> int:
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{table}"')
            return int(cur.fetchone()[0])

    def drop() -> None:
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        finally:
            conn.close()

    return _pg_endpoint(table), count, drop


def _mysql(table: str, _tmp: Path):
    pymysql = pytest.importorskip("pymysql")
    try:
        conn = pymysql.connect(
            host="localhost",
            port=3306,
            user="dataflow",
            password="dataflow",
            database="dataflow",
            autocommit=True,
        )
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"MySQL unavailable: {exc}")
    endpoint = EndpointConfig(
        kind="database",
        format="mysql",
        host="localhost",
        port=3306,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        schema="dataflow",
        table=table,
    )

    def count() -> int:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{table}`")
            return int(cur.fetchone()[0])

    def drop() -> None:
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        finally:
            conn.close()

    return endpoint, count, drop


def _sqlite(table: str, tmp: Path):
    import sqlite3

    path = str(tmp / f"{table}.db")
    endpoint = EndpointConfig(
        kind="database", format="sqlite", database=path, table=table
    )

    def count() -> int:
        conn = sqlite3.connect(path)
        try:
            return int(conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])
        finally:
            conn.close()

    return endpoint, count, lambda: None


def _mongodb(table: str, _tmp: Path):
    pymongo = pytest.importorskip("pymongo")
    try:
        client = pymongo.MongoClient(
            "localhost", 27017, serverSelectionTimeoutMS=4000
        )
        client.admin.command("ping")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"MongoDB unavailable: {exc}")
    endpoint = EndpointConfig(
        kind="database",
        format="mongodb",
        host="localhost",
        port=27017,
        database="dataflow",
        table=table,
    )

    def count() -> int:
        return int(client["dataflow"][table].count_documents({}))

    def drop() -> None:
        try:
            client["dataflow"][table].drop()
        finally:
            client.close()

    return endpoint, count, drop


def _sqlserver(table: str, _tmp: Path):
    pymssql = pytest.importorskip("pymssql")
    try:
        conn = pymssql.connect(
            server="localhost",
            port=1433,
            user="sa",
            password="Dataflow!Pass1",
            database="master",
            login_timeout=3,
            timeout=5,
        )
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"SQL Server unavailable: {exc}")
    endpoint = EndpointConfig(
        kind="database",
        format="sqlserver",
        host="localhost",
        port=1433,
        database="master",
        username="sa",
        password="Dataflow!Pass1",
        schema="dbo",
        table=table,
    )

    def count() -> int:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM [dbo].[{table}]")
        return int(cur.fetchone()[0])

    def drop() -> None:
        try:
            cur = conn.cursor()
            cur.execute(f"DROP TABLE IF EXISTS [dbo].[{table}]")
            conn.commit()
        finally:
            conn.close()

    return endpoint, count, drop


def _redis(table: str, _tmp: Path):
    redis = pytest.importorskip("redis")
    try:
        client = redis.Redis(host="localhost", port=6379, socket_timeout=3)
        client.ping()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Redis unavailable: {exc}")
    endpoint = EndpointConfig(
        kind="database",
        format="redis",
        host="localhost",
        port=6379,
        database="0",
        table=table,
    )

    def count() -> int:
        return len(list(client.scan_iter(match=f"{table}*")))

    def drop() -> None:
        try:
            keys = list(client.scan_iter(match=f"{table}*"))
            if keys:
                client.delete(*keys)
        finally:
            client.close()

    return endpoint, count, drop


#: ``name -> builder``. A builder returns ``(endpoint, count_rows, drop)`` and
#: skips when its engine is not reachable, so the matrix reports honestly on a
#: machine that has only some of them up.
DESTINATIONS: dict[str, Any] = {
    "postgresql": _postgresql,
    "mysql": _mysql,
    "sqlite": _sqlite,
    "mongodb": _mongodb,
    "sqlserver": _sqlserver,
    "redis": _redis,
}

#: Destinations whose row identity *is* the key, so append cannot duplicate.
KEY_ADDRESSED = {"redis"}


@pytest.fixture
def pg_source():
    """A real PostgreSQL table holding the fixture rows."""
    conn = _pg_conn()
    table = f"sync_src_{uuid.uuid4().hex[:10]}"
    source = _pg_endpoint(table)
    write_destination_database(source, RECORDS, COLUMNS, SCHEMA, MAPPINGS)
    try:
        yield source, table, conn
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        conn.close()


@pytest.mark.parametrize("destination", sorted(DESTINATIONS))
@pytest.mark.parametrize("mode", MODES)
def test_sync_mode_second_run(destination: str, mode: str, pg_source, tmp_path):
    """Run one mode twice and hold the destination to the mode's own contract."""
    source, source_table, _conn = pg_source
    table = f"sync_{destination[:6]}_{uuid.uuid4().hex[:8]}"
    endpoint, count_rows, drop = DESTINATIONS[destination](table, tmp_path)
    try:
        outcome = run_mode(
            source,
            endpoint,
            mode,
            stream_name=source_table,
            count_rows=count_rows,
            key_addressed=destination in KEY_ADDRESSED,
        )
        assert outcome.ok, f"{destination}/{mode}: {outcome.error}"
        assert outcome.destination_rows == outcome.expected, (
            f"{destination}/{mode}: destination holds {outcome.destination_rows} "
            f"row(s) after two runs, expected {outcome.expected}"
        )
    finally:
        drop()


def test_incremental_moves_only_new_rows(pg_source, tmp_path):
    """The watermark must advance: a later row moves, earlier ones do not.

    Row counts alone cannot tell "read nothing" from "re-read and deduplicated",
    so this inserts a row between runs and checks that exactly it arrives.
    """
    import uuid as _uuid

    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import TransferRequest

    source, source_table, conn = pg_source
    table = f"sync_incr_{_uuid.uuid4().hex[:8]}"
    endpoint, count_rows, drop = DESTINATIONS["postgresql"](table, tmp_path)

    def _run() -> int:
        result = UniversalTransferEngine().execute_tracked(
            TransferRequest(
                source=source,
                destination=endpoint,
                sync_mode="incremental_append",
                skip_preflight=False,
                validation_mode="balanced",
                stream_contracts=[
                    stream_contract(source_table, "incremental_append")
                ],
                mappings=list(MAPPINGS),
            ),
            _uuid.uuid4().hex[:24],
        )
        assert result.success is True, result.error
        return int(result.records_transferred)

    try:
        assert _run() == len(RECORDS)
        # Steady state: no new rows. This is the normal case for a schedule and
        # must not fail — it used to, on every destination.
        assert _run() == 0
        with conn.cursor() as cur:
            cur.execute(f'INSERT INTO "{source_table}" (id, amount) VALUES (3, 3000.75)')
        # Exactly the new row, never a re-read of the whole source.
        assert _run() == 1
        assert count_rows() == len(RECORDS) + 1
        assert _run() == 0
    finally:
        drop()


def test_redis_keyspace_probe_survives_a_populated_instance():
    """The probe must finish a pass over a real keyspace, not a near-empty one.

    It used to scan 64 calls of 8 keys — about 512 — and report "unknown" past
    that, so every Redis destination fail-closed on any instance holding more
    keys than a fresh test container. It passed only because CI's Redis was
    nearly empty.
    """
    from connectors.redis_writer import _redis_prefix_key_count_hint

    class _FakeRedis:
        """A keyspace with many keys and none under the probed prefix."""

        def __init__(self, total: int) -> None:
            self.total = total
            self.calls = 0

        def scan(self, cursor: int = 0, match: str = "", count: int = 10):
            self.calls += 1
            nxt = int(cursor) + int(count)
            if nxt >= self.total:
                return 0, []  # cursor wrapped: the prefix is genuinely absent
            return nxt, []

    fake = _FakeRedis(total=250_000)
    assert _redis_prefix_key_count_hint(fake, "orders") == 0, (
        f"probe gave up after {fake.calls} call(s) on a 250k-key instance"
    )

    # A prefix that does exist is answered on the first match, not after a pass.
    class _HasPrefix(_FakeRedis):
        def scan(self, cursor: int = 0, match: str = "", count: int = 10):
            self.calls += 1
            return 0, ["orders:1"]

    assert _redis_prefix_key_count_hint(_HasPrefix(total=10), "orders") > 0

    # Exhausting the budget still reports unknown rather than guessing empty:
    # reading "no keys" off an incomplete scan would bind Map VARCHAR over live
    # typed documents.
    class _NeverWraps(_FakeRedis):
        def scan(self, cursor: int = 0, match: str = "", count: int = 10):
            self.calls += 1
            return int(cursor) + 1, []

    assert _redis_prefix_key_count_hint(_NeverWraps(total=1), "orders") == -1


def test_every_mode_has_a_declared_second_run_expectation():
    """A mode nobody declared an expectation for is a mode nobody proved."""
    from services.sync_cursor import CANONICAL_SYNC_MODES

    # cdc, scd2, mirror and reverse_etl have their own dedicated suites and are
    # not two-run row-count shaped; the operator-facing Advanced options are.
    operator_modes = {
        "full_refresh_overwrite",
        "full_refresh_append",
        "incremental_append",
        "upsert",
    }
    assert operator_modes <= CANONICAL_SYNC_MODES
    assert set(EXPECTED_AFTER_TWO_RUNS) == operator_modes
