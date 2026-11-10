"""D344 — a column added to a CDC source is propagated or stops the stream.

PostgreSQL (``wal_level=logical``) → MySQL CDC. After the snapshot and one
round of changes the source gains ``tier INT DEFAULT 1``; later events set it
(UPDATE id 5 tier 9, DELETE id 6, INSERT id 9 tier 4, UPDATE id 9 tier 5).

Before the fix every event was applied through the column list the capture
started with: the ``tier`` values were dropped on the floor, the destination
never gained the column, and the job reported "CDC applied N change(s)".

* ``schema_policy=propagate_columns``: the destination gains a nullable
  ``tier`` and the events carry 9 / 5.
* any other policy: the apply stops before the first event that carries the
  column, the error names it, and the destination is left as it was.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest

pytestmark = pytest.mark.timeout(300)

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")


# ── unit: the decision itself (no live engines) ──────────────────────────


def test_only_columns_the_capture_did_not_start_with_are_new():
    from src.transfer.cdc_added_columns import added_source_columns

    imaged = ["id", "Name", "tier", "_df_lsn", "_df_before", "__op", "__deleted", "TIER"]
    assert added_source_columns(imaged, ["id", "name"]) == ["tier"]


def test_a_new_column_is_refused_by_name_unless_the_policy_propagates():
    from src.transfer.cdc_added_columns import CdcAddedColumnRefused, adopt_added_columns

    base = dict(
        mappings=[{"source": "id", "target": "id"}],
        headers=["id"],
        column_types={"id": "integer"},
        table="public.t",
        dest_table="t_dst",
    )
    with pytest.raises(CdcAddedColumnRefused) as refused:
        adopt_added_columns(["tier"], propagate=False, **base)
    assert refused.value.columns == ["tier"]
    assert "tier" in str(refused.value) and "propagate_columns" in str(refused.value)

    maps, headers, types = adopt_added_columns(
        ["tier"], propagate=True, source_types=lambda cols: {"tier": "integer"}, **base
    )
    assert headers == ["id", "tier"] and types["tier"] == "integer"
    added = maps[-1]
    assert (added["source"], added["target"], added["propagate_reason"]) == ("tier", "tier", "add_column")


def test_the_running_job_and_the_repair_finding_name_the_added_column():
    from services.batch_progress import batch_write_message
    from services.cdc_drift_repair import cdc_drift_operator_message

    msg = batch_write_message(3, 3, 13, is_cdc=True, checkpoint={"cdc_added_columns": ["tier"]})
    assert msg.startswith("CDC applied 13 change(s)") and "tier" in msg
    finding = cdc_drift_operator_message(
        {"dest_table": "t_dst", "pk_fields": ["id"], "keys": [{}] * 4, "added_columns": ["tier"]}
    )
    assert "repair_cdc_drift" in finding and "tier" in finding


def _pg_cfg() -> dict:
    from tests.helpers.live_env import pg_creds

    c = pg_creds()
    return {
        "host": str(c["host"]), "port": int(c["port"]), "database": str(c["database"]),
        "username": str(c["username"]), "password": str(c["password"]),
    }


def _my_cfg() -> dict:
    from tests.helpers.live_env import mysql_creds

    c = mysql_creds()
    return {
        "host": str(c["host"]), "port": int(c["port"]), "database": str(c["database"]),
        "username": str(c["username"]), "password": str(c["password"]),
    }


def _pg_sql(sql: str, params=None, fetch: bool = False):
    import psycopg2

    c = _pg_cfg()
    conn = psycopg2.connect(
        host=c["host"], port=c["port"], dbname=c["database"],
        user=c["username"], password=c["password"], connect_timeout=3,
    )
    conn.autocommit = True
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        return [tuple(r) for r in cur.fetchall()] if fetch else None
    finally:
        conn.close()


def _my_sql(sql: str, fetch: bool = False):
    import pymysql

    c = _my_cfg()
    conn = pymysql.connect(
        host=c["host"], port=c["port"], user=c["username"], password=c["password"],
        database=c["database"], autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return [tuple(r) for r in cur.fetchall()] if fetch else None
    finally:
        conn.close()


def _live() -> bool:
    from tests.helpers.live_env import mysql_up, pg_up

    if not (pg_up() and mysql_up()):
        return False
    try:
        return _pg_sql("SHOW wal_level", fetch=True)[0][0] == "logical"
    except Exception:  # noqa: BLE001
        return False


needs_live = pytest.mark.skipif(
    not _live(), reason="needs PostgreSQL with wal_level=logical and MySQL"
)


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    import services.sync_cursor as sc
    import src.transfer.engine as engine_mod
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    class _Store(_FakeMongo):
        def update_job_fields(self, job_id, fields, **_kw):
            self.jobs.setdefault(job_id, {}).update(fields)
            return True

    fake = _Store()
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    monkeypatch.setenv("DATAFLOW_CDC_MAPPING_REVIEW_STORE", str(tmp_path / "reviews.jsonl"))
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _request(table: str, sync: str, policy: str):
    from src.transfer.models import EndpointConfig, TransferRequest

    return TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql", schema="public",
                              table=table, **_pg_cfg()),
        destination=EndpointConfig(kind="database", format="mysql",
                                   table=f"{table}_dst", **_my_cfg()),
        sync_mode=sync,
        stream_contracts=[
            {"name": table, "selected": True, "primary_key": ["id"], "sync_mode": sync,
             "cursor_semantics": "cdc_position"}
        ],
        schema_policy=policy,
        skip_preflight=True,
        validation_mode="strict",
    )


def _run(fake, table: str, sync: str, policy: str):
    from src.transfer.engine import UniversalTransferEngine

    job_id = "d344" + uuid.uuid4().hex[:12]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(_request(table, sync, policy), job_id)


def _cleanup(table: str) -> None:
    for name, active in _pg_sql("SELECT slot_name, active FROM pg_replication_slots", fetch=True):
        if table in name and not active:
            _pg_sql("SELECT pg_drop_replication_slot(%s)", (name,))
    for (pub,) in _pg_sql(
        "SELECT pubname FROM pg_publication WHERE pubname LIKE %s", (f"%{table}%",), fetch=True
    ):
        _pg_sql(f'DROP PUBLICATION IF EXISTS "{pub}"')
    _pg_sql(f'DROP TABLE IF EXISTS "{table}"')
    _my_sql(f"DROP TABLE IF EXISTS `{table}_dst`")


def _dest_columns(table: str) -> list[str]:
    return [
        r[0]
        for r in _my_sql(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = DATABASE() "
            f"AND table_name = '{table}_dst' ORDER BY ordinal_position",
            fetch=True,
        )
    ]


def _start(fake, table: str, policy: str) -> None:
    _pg_sql(f'CREATE TABLE "{table}" (id INT PRIMARY KEY, name TEXT, status TEXT)')
    _pg_sql(f'ALTER TABLE "{table}" REPLICA IDENTITY FULL')
    for i in range(1, 7):
        _pg_sql(f'INSERT INTO "{table}" VALUES (%s, %s, %s)', (i, f"n{i}", "new"))
    first = _run(fake, table, "cdc", policy)
    assert first.success, first.error
    _pg_sql(f"UPDATE \"{table}\" SET status = 'paid' WHERE id = 2")
    second = _run(fake, table, "cdc_incremental", policy)
    assert second.success, second.error


def _alter_mid_capture(monkeypatch, table: str, default: str = " DEFAULT 1") -> None:
    """Run the DDL + changes once the run has fixed its column list.

    The capture introspects the source before it reads the log; the ALTER
    lands between that and the first poll, as on a long-running capture job.
    """
    from connectors.postgresql_change_stream import PostgreSqlChangeStreamCdc

    original = PostgreSqlChangeStreamCdc.poll
    fired = {"done": False}

    def poll(self):
        if not fired["done"]:
            fired["done"] = True
            _add_column_and_change(table, default)
        yield from original(self)

    monkeypatch.setattr(PostgreSqlChangeStreamCdc, "poll", poll)


def _add_column_and_change(table: str, default: str = " DEFAULT 1") -> None:
    _pg_sql(f'ALTER TABLE "{table}" ADD COLUMN tier INT{default}')
    _pg_sql(f"UPDATE \"{table}\" SET status = 'pending', tier = 9 WHERE id = 5")
    _pg_sql(f'DELETE FROM "{table}" WHERE id = 6')
    _pg_sql(f"INSERT INTO \"{table}\" (id, name, status, tier) VALUES (9, 'n9', 'new', 4)")
    _pg_sql(f"UPDATE \"{table}\" SET name = 'renamed', tier = 5 WHERE id = 9")


def _dest_rows(table: str) -> dict[int, tuple]:
    return {
        r[0]: r[1:]
        for r in _my_sql(f"SELECT id, name, status, tier FROM `{table}_dst` ORDER BY id", fetch=True)
    }


@needs_live
def test_propagate_columns_adds_the_column_and_applies_its_values(isolated, monkeypatch):
    table = "d344p_" + uuid.uuid4().hex[:8]
    try:
        _start(isolated, table, "propagate_columns")
        _alter_mid_capture(monkeypatch, table, default="")

        third = _run(isolated, table, "cdc_incremental", "propagate_columns")

        assert third.success, third.error
        assert "tier" in _dest_columns(table)
        rows = _dest_rows(table)
        assert 6 not in rows
        assert rows[5] == ("n5", "pending", 9)
        assert rows[9] == ("renamed", "new", 5)
        assert rows[1] == ("n1", "new", None)
        tier_type = _my_sql(
            "SELECT data_type FROM information_schema.columns WHERE table_schema = DATABASE() "
            f"AND table_name = '{table}_dst' AND column_name = 'tier'",
            fetch=True,
        )[0][0]
        assert str(tier_type).lower() in {"int", "bigint"}, tier_type
        summary = third.destination_summary or {}
        assert (summary.get("schema_propagation") or {}).get("added_columns") == ["tier"]
    finally:
        _cleanup(table)


@needs_live
def test_propagate_with_source_default_applies_events_and_names_the_backfill(
    isolated, monkeypatch
):
    """The QA repro: ``ADD COLUMN tier INT DEFAULT 1``.

    PostgreSQL gives the existing rows ``tier = 1`` with no change event, so
    the CDC image check finds them different at the destination. The events
    themselves land with their tier values; the run then stops on the image
    check, which names the added column and the repair tool, instead of
    reporting success over rows it never received.
    """
    table = "d344d_" + uuid.uuid4().hex[:8]
    try:
        _start(isolated, table, "propagate_columns")
        _alter_mid_capture(monkeypatch, table)

        third = _run(isolated, table, "cdc_incremental", "propagate_columns")

        assert "tier" in _dest_columns(table)
        rows = _dest_rows(table)
        assert 6 not in rows
        assert rows[5] == ("n5", "pending", 9)
        assert rows[9] == ("renamed", "new", 5)
        if not third.success:
            assert "repair_cdc_drift" in str(third.error)
            assert "tier" in str(third.error)
        else:
            # An at-least-once route upserts the source image itself.
            assert rows[1] == ("n1", "new", 1)
    finally:
        _cleanup(table)


@needs_live
def test_manual_review_stops_the_stream_and_names_the_column(isolated, monkeypatch):
    table = "d344m_" + uuid.uuid4().hex[:8]
    try:
        _start(isolated, table, "manual_review")
        before = _my_sql(f"SELECT id, status FROM `{table}_dst` ORDER BY id", fetch=True)
        _alter_mid_capture(monkeypatch, table, default="")

        third = _run(isolated, table, "cdc_incremental", "manual_review")

        assert not third.success
        assert "tier" in str(third.error) and "propagate_columns" in str(third.error)
        assert "tier" not in _dest_columns(table)
        # Nothing after the column appeared was applied without it.
        assert _my_sql(f"SELECT id, status FROM `{table}_dst` ORDER BY id", fetch=True) == before

        # The held changes were not consumed: once the policy propagates, the
        # next run applies every one of them, tier included.
        fourth = _run(isolated, table, "cdc_incremental", "propagate_columns")

        assert fourth.success, fourth.error
        rows = _dest_rows(table)
        assert 6 not in rows
        assert rows[5] == ("n5", "pending", 9)
        assert rows[9] == ("renamed", "new", 5)
    finally:
        _cleanup(table)


# ── MySQL binlog source → PostgreSQL ──────────────────────────────────────


def _binlog_ready() -> bool:
    from tests.helpers.live_env import mysql_up, pg_up

    try:
        import pymysqlreplication  # noqa: F401
    except ImportError:
        return False
    if not (mysql_up() and pg_up()):
        return False
    try:
        rows = _my_sql("SHOW VARIABLES LIKE 'binlog_format'", fetch=True)
        return bool(rows) and str(rows[0][1]).upper() == "ROW"
    except Exception:  # noqa: BLE001
        return False


needs_binlog = pytest.mark.skipif(
    not _binlog_ready(), reason="needs MySQL with ROW binlog and PostgreSQL"
)


def _binlog_request(table: str, sync: str, policy: str):
    from src.transfer.models import EndpointConfig, TransferRequest

    return TransferRequest(
        source=EndpointConfig(kind="database", format="mysql", table=table, **_my_cfg()),
        destination=EndpointConfig(kind="database", format="postgresql", schema="public",
                                   table=f"{table}_dst", **_pg_cfg()),
        sync_mode=sync,
        stream_contracts=[
            {"name": table, "selected": True, "primary_key": ["id"], "sync_mode": sync,
             "cursor_semantics": "cdc_position"}
        ],
        schema_policy=policy,
        skip_preflight=True,
        validation_mode="strict",
    )


def _binlog_run(fake, table: str, sync: str, policy: str):
    from src.transfer.engine import UniversalTransferEngine

    job_id = "d344b" + uuid.uuid4().hex[:12]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(_binlog_request(table, sync, policy), job_id)


@needs_binlog
@pytest.mark.parametrize("policy", ["propagate_columns", "manual_review"])
def test_mysql_binlog_added_column_is_propagated_or_stops(isolated, monkeypatch, policy):
    from connectors.mysql_change_stream import MySqlChangeStreamCdc

    table = "d344b_" + uuid.uuid4().hex[:8]
    try:
        _my_sql(f"CREATE TABLE `{table}` (id INT PRIMARY KEY, name VARCHAR(40), status VARCHAR(20))")
        for i in range(1, 7):
            _my_sql(f"INSERT INTO `{table}` VALUES ({i}, 'n{i}', 'new')")
        first = _binlog_run(isolated, table, "cdc", policy)
        assert first.success, first.error
        _my_sql(f"UPDATE `{table}` SET status = 'paid' WHERE id = 2")
        second = _binlog_run(isolated, table, "cdc_incremental", policy)
        assert second.success, second.error

        original = MySqlChangeStreamCdc.poll
        fired = {"done": False}

        def poll(self):
            if not fired["done"]:
                fired["done"] = True
                # The reader has cached the three-column layout by now.
                self._ordered_columns(self.table)
                _my_sql(f"ALTER TABLE `{table}` ADD COLUMN tier INT")
                _my_sql(f"UPDATE `{table}` SET status = 'pending', tier = 9 WHERE id = 5")
                _my_sql(f"INSERT INTO `{table}` (id, name, status, tier) VALUES (9, 'n9', 'new', 4)")
            yield from original(self)

        monkeypatch.setattr(MySqlChangeStreamCdc, "poll", poll)
        third = _binlog_run(isolated, table, "cdc_incremental", policy)

        dest_cols = [
            r[0]
            for r in _pg_sql(
                "SELECT column_name FROM information_schema.columns WHERE table_name = %s "
                "ORDER BY ordinal_position",
                (f"{table}_dst",),
                fetch=True,
            )
        ]
        assert not any(c.upper().startswith("UNKNOWN_COL") for c in dest_cols), dest_cols
        if policy == "propagate_columns":
            assert third.success, third.error
            assert "tier" in dest_cols
            rows = {
                r[0]: r[1:]
                for r in _pg_sql(f'SELECT id, status, tier FROM "{table}_dst"', fetch=True)
            }
            assert rows[5] == ("pending", 9) and rows[9] == ("new", 4)
        else:
            assert not third.success
            assert "tier" in str(third.error), third.error
            assert "tier" not in dest_cols
            assert 9 not in {r[0] for r in _pg_sql(f'SELECT id FROM "{table}_dst"', fetch=True)}
    finally:
        _my_sql(f"DROP TABLE IF EXISTS `{table}`")
        _pg_sql(f'DROP TABLE IF EXISTS "{table}_dst"')


# ── MongoDB change stream → PostgreSQL: a field no snapshot document had ──

_MONGO = {
    "host": "localhost",
    "port": 27017,
    "database": "dataflow_cdc",
    "connection_string": "mongodb://localhost:27017/dataflow_cdc?directConnection=true",
    "ssl": False,
}


def _change_streams_ready() -> bool:
    from tests.helpers.live_env import pg_up

    try:
        from connectors.mongodb_change_stream import MongodbChangeStreamCdc

        return pg_up() and MongodbChangeStreamCdc(
            _MONGO, collection="probe", primary_key="_id"
        ).is_available()
    except Exception:  # noqa: BLE001
        return False


def _mongo_run(fake, coll: str, sync: str, policy: str):
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    request = TransferRequest(
        source=EndpointConfig(kind="database", format="mongodb", table=coll, **_MONGO),
        destination=EndpointConfig(kind="database", format="postgresql", schema="public",
                                   table=f"{coll}_dst", **_pg_cfg()),
        sync_mode=sync,
        stream_contracts=[
            {"name": coll, "selected": True, "sync_mode": sync, "primary_key": "_id"}
        ],
        schema_policy=policy,
        skip_preflight=True,
        validation_mode="balanced",
    )
    job_id = "d344g" + uuid.uuid4().hex[:12]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


@pytest.mark.skipif(not _change_streams_ready(), reason="needs a MongoDB replica set and PostgreSQL")
@pytest.mark.parametrize("policy", ["propagate_columns", "manual_review"])
def test_mongo_new_field_is_propagated_or_stops(isolated, monkeypatch, policy):
    from connectors.mongodb_change_stream import MongodbChangeStreamCdc

    name = "d344g_" + uuid.uuid4().hex[:8]
    coll = MongodbChangeStreamCdc(_MONGO, collection=name, primary_key="_id").coll
    coll.insert_many([{"_id": i, "name": f"n{i}"} for i in range(1, 5)])
    try:
        first = _mongo_run(isolated, name, "cdc", policy)
        assert first.success, first.error

        original = MongodbChangeStreamCdc.poll
        fired = {"done": False}

        def poll(self):
            if not fired["done"]:
                fired["done"] = True
                coll.update_one({"_id": 2}, {"$set": {"tier": 9}})
                coll.insert_one({"_id": 5, "name": "n5", "tier": 4})
            yield from original(self)

        monkeypatch.setattr(MongodbChangeStreamCdc, "poll", poll)
        second = _mongo_run(isolated, name, "cdc_incremental", policy)

        dest_cols = [
            r[0]
            for r in _pg_sql(
                "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
                (f"{name}_dst",),
                fetch=True,
            )
        ]
        if policy == "propagate_columns":
            assert second.success, second.error
            assert "tier" in dest_cols
            tier_type = _pg_sql(
                "SELECT data_type FROM information_schema.columns "
                "WHERE table_name = %s AND column_name = 'tier'",
                (f"{name}_dst",),
                fetch=True,
            )[0][0]
            # Typed from the values the change carried (Mongo has no catalog).
            assert tier_type in {"bigint", "integer", "smallint", "numeric"}, tier_type
            tiers = dict(_pg_sql(f'SELECT "_id", tier FROM "{name}_dst"', fetch=True))
            assert {int(k): (None if v is None else int(v)) for k, v in tiers.items()} == {
                1: None, 2: 9, 3: None, 4: None, 5: 4,
            }
        else:
            assert not second.success
            assert "tier" in str(second.error), second.error
            assert "tier" not in dest_cols
    finally:
        coll.drop()
        _pg_sql(f'DROP TABLE IF EXISTS "{name}_dst"')
