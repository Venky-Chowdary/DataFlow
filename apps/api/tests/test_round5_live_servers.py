"""Round-5 defects on the Docker servers: MySQL 8, PostgreSQL 16, SQL Server 2022.

Oracle is not part of this matrix. Each test skips when its server is down.
"""

from __future__ import annotations

import os
import uuid

import pytest

pytestmark = pytest.mark.timeout(90)


def _pg():
    import psycopg2
    from conftest import pg_test_cfg

    cfg = pg_test_cfg()
    try:
        conn = psycopg2.connect(
            host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
            user=cfg["username"], password=cfg["password"], connect_timeout=2,
        )
    except Exception as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    conn.autocommit = False
    return conn, cfg


def _mysql():
    import pymysql

    try:
        conn = pymysql.connect(
            host="127.0.0.1", port=3306, user="dataflow", password="dataflow",
            database="dataflow", connect_timeout=2, autocommit=False,
            charset="utf8mb4",
        )
    except Exception as exc:
        pytest.skip(f"MySQL not reachable: {exc}")
    return conn


def _mssql():
    import pymssql

    password = os.environ.get("DATAFLOW_TEST_MSSQL_PASSWORD") or os.environ.get(
        "DATAFLOW_SQLSERVER_PASSWORD"
    ) or "DataFlow_CDC_2022!"
    try:
        conn = pymssql.connect(
            server="127.0.0.1", port="1433", user="sa", password=password,
            database="dataflow", login_timeout=3, timeout=15,
        )
    except Exception as exc:
        pytest.skip(f"SQL Server not reachable: {exc}")
    return conn, password


def test_mysql_not_empty_drop_unlocks_and_the_pool_can_use_another_table(monkeypatch):
    import sqlalchemy as sa

    from services.batch_undo import note_dropped_empty_create
    from services.error_handling import TransferCancelled

    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8")
    conn = _mysql()
    filled = f"d378f_{uuid.uuid4().hex[:8]}"
    other = f"d378o_{uuid.uuid4().hex[:8]}"
    cfg = {
        "type": "mysql", "host": "127.0.0.1", "port": 3306,
        "database": "dataflow", "username": "dataflow", "password": "dataflow",
    }
    try:
        cur = conn.cursor()
        cur.execute(f"CREATE TABLE `{filled}` (id INT)")
        cur.execute(f"CREATE TABLE `{other}` (id INT)")
        cur.execute(f"INSERT INTO `{filled}` VALUES (1)")
        conn.commit()
        kept = TransferCancelled("cancelled", rows_written=0)
        note_dropped_empty_create(kept, cfg, filled, None)
        assert kept.dropped_created_table == ""
        # The write lock must be gone. A leaked LOCK TABLES makes this INSERT
        # wait until lock_wait_timeout (1205) and the pooled connection raise
        # 1100 on every other table.
        other_session = _mysql()
        try:
            ocur = other_session.cursor()
            ocur.execute("SET SESSION lock_wait_timeout = 3")
            ocur.execute(f"INSERT INTO `{filled}` VALUES (2)")
            other_session.commit()
            ocur.execute(f"SELECT COUNT(*) FROM `{filled}`")
            assert ocur.fetchone()[0] == 2
        finally:
            other_session.close()
        from connectors.generic_sql import _engine

        with _engine(cfg).connect() as pooled:
            count = pooled.execute(sa.text(f"SELECT COUNT(*) FROM `{other}`")).scalar()
        assert int(count or 0) == 0
    finally:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS `{filled}`")
        cur.execute(f"DROP TABLE IF EXISTS `{other}`")
        conn.commit()
        conn.close()


def test_postgres_empty_drop_does_not_wait_forever_on_a_lock(monkeypatch):
    import time

    from services.batch_undo import note_dropped_empty_create
    from services.error_handling import TransferCancelled

    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8")
    conn, cfg = _pg()
    name = f"d378l_{uuid.uuid4().hex[:8]}"
    blocker = None
    try:
        cur = conn.cursor()
        cur.execute(f'CREATE TABLE "{name}" (id int)')
        conn.commit()
        blocker, _cfg = _pg()
        bcur = blocker.cursor()
        bcur.execute(f'LOCK TABLE "{name}" IN ACCESS EXCLUSIVE MODE')
        started = time.monotonic()
        outcome = TransferCancelled("cancelled", rows_written=0)
        note_dropped_empty_create(outcome, {"type": "postgresql", **cfg}, name, None)
        elapsed = time.monotonic() - started
        # lock_timeout is 5s. An instant return is a connect failure, not the wait.
        assert 4 <= elapsed < 20
        assert outcome.dropped_created_table == ""
        cur.execute(
            "SELECT 1 FROM pg_tables WHERE tablename = %s", (name,),
        )
        assert cur.fetchone() is not None
    finally:
        if blocker is not None:
            blocker.rollback()
            blocker.close()
        cur = conn.cursor()
        cur.execute(f'DROP TABLE IF EXISTS "{name}"')
        conn.commit()
        conn.close()


def test_sqlserver_empty_drop_does_not_wait_forever_on_a_lock(monkeypatch):
    import time

    from services.batch_undo import note_dropped_empty_create
    from services.error_handling import TransferCancelled

    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8")
    conn, password = _mssql()
    name = f"d378l_{uuid.uuid4().hex[:8]}"
    blocker = None
    try:
        cur = conn.cursor()
        cur.execute(f"CREATE TABLE dbo.[{name}] (id INT)")
        conn.commit()
        blocker, _password = _mssql()
        bcur = blocker.cursor()
        bcur.execute(f"BEGIN TRAN SELECT TOP 1 id FROM dbo.[{name}] WITH (TABLOCKX)")
        started = time.monotonic()
        outcome = TransferCancelled("cancelled", rows_written=0)
        note_dropped_empty_create(
            outcome,
            {
                "type": "sqlserver", "host": "127.0.0.1", "port": 1433,
                "database": "dataflow", "username": "sa", "password": password,
                "trust_server_certificate": True,
            },
            name,
            "dbo",
        )
        elapsed = time.monotonic() - started
        # LOCK_TIMEOUT is 5s. An instant return is a connect failure, not the wait.
        assert 4 <= elapsed < 20
        assert outcome.dropped_created_table == ""
    finally:
        if blocker is not None:
            blocker.rollback()
            blocker.close()
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS dbo.[{name}]")
        conn.commit()
        conn.close()


def _mysql_length(name: str, column: str) -> int:
    import pymysql

    conn = pymysql.connect(
        host="127.0.0.1", port=3306, user="dataflow", password="dataflow",
        database="dataflow", autocommit=True,
    )
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND COLUMN_NAME = %s",
            (name, column),
        )
        row = cur.fetchone()
        return int(row[0])
    finally:
        conn.close()


def test_mysql_quarantine_of_every_check_violation_restores_the_widen(monkeypatch):
    from connectors.column_widen_restore import note_restore
    from connectors.mysql_writer import write_mapped_rows

    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8")
    restores: list[str] = []

    def _spy(sql: str) -> None:
        restores.append(sql)
        note_restore(sql)

    monkeypatch.setattr("connectors.column_widen_restore.note_restore", _spy)
    conn = _mysql()
    name = f"d368q_{uuid.uuid4().hex[:8]}"
    try:
        cur = conn.cursor()
        cur.execute(
            f"CREATE TABLE `{name}` ("
            "id INT PRIMARY KEY, "
            "name VARCHAR(40), "
            "CONSTRAINT name_short CHECK (CHAR_LENGTH(name) <= 10))"
        )
        conn.commit()
        result = write_mapped_rows(
            host="127.0.0.1", port=3306, database="dataflow",
            username="dataflow", password="dataflow", schema="",
            connection_string="", ssl=False, table_name=name,
            headers=["id", "name"],
            data_rows=[["1", "x" * 50], ["2", "y" * 50]],
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "name", "target": "name"},
            ],
            column_types={"id": "INT", "name": "VARCHAR(80)"},
            create_table=False,
            error_policy="quarantine",
            backfill_new_fields=True,
        )
        assert result.rows_written == 0
        assert result.ok
        assert restores, "the widen never ran, so a varchar(40) column does not prove a restore"
        assert _mysql_length(name, "name") == 40
    finally:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS `{name}`")
        conn.commit()
        conn.close()


def test_postgres_quarantine_of_every_check_violation_restores_the_widen(monkeypatch):
    from connectors.column_widen_restore import note_restore
    from connectors.postgresql_writer import write_mapped_rows

    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8")
    restores: list[str] = []

    def _spy(sql: str) -> None:
        restores.append(sql)
        note_restore(sql)

    monkeypatch.setattr("connectors.column_widen_restore.note_restore", _spy)
    conn, cfg = _pg()
    name = f"d368q_{uuid.uuid4().hex[:8]}"
    try:
        cur = conn.cursor()
        cur.execute(
            f'CREATE TABLE "{name}" ('
            "id int PRIMARY KEY, "
            "name varchar(40), "
            "CONSTRAINT name_short CHECK (char_length(name) <= 10))"
        )
        conn.commit()
        result = write_mapped_rows(
            host=cfg["host"], port=cfg["port"], database=cfg["database"],
            username=cfg["username"], password=cfg["password"], schema="public",
            connection_string="", ssl=False, table_name=name,
            headers=["id", "name"],
            data_rows=[["1", "x" * 50], ["2", "y" * 50]],
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "name", "target": "name"},
            ],
            column_types={"id": "INT", "name": "VARCHAR(80)"},
            create_table=False,
            error_policy="quarantine",
            backfill_new_fields=True,
        )
        assert result.rows_written == 0
        assert result.ok
        assert restores, "the widen never ran, so a varchar(40) column does not prove a restore"
        cur.execute(
            "SELECT character_maximum_length FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = %s AND column_name = 'name'",
            (name,),
        )
        assert cur.fetchone()[0] == 40
    finally:
        conn.rollback()
        cur = conn.cursor()
        cur.execute(f'DROP TABLE IF EXISTS "{name}"')
        conn.commit()
        conn.close()


def test_mysql_utf8mb4_0900_ai_ci_matches_a_real_unique_index():
    from services.collation_equality import group_key_values, read_column_collation

    conn = _mysql()
    name = f"d391_{uuid.uuid4().hex[:8]}"
    try:
        cur = conn.cursor()
        cur.execute(
            f"CREATE TABLE `{name}` ("
            "k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci, "
            "UNIQUE KEY u (k))"
        )
        conn.commit()
        cur.execute(f"INSERT INTO `{name}` (k) VALUES (%s)", ("a",))
        cur.execute(f"INSERT INTO `{name}` (k) VALUES (%s)", ("a ",))
        conn.commit()
        with pytest.raises(Exception):
            cur.execute(f"INSERT INTO `{name}` (k) VALUES (%s)", ("é",))
            cur.execute(f"INSERT INTO `{name}` (k) VALUES (%s)", ("e",))
            conn.commit()
        conn.rollback()
        cur.execute(f"DELETE FROM `{name}`")
        cur.execute(f"INSERT INTO `{name}` (k) VALUES (%s)", ("e",))
        conn.commit()
        with pytest.raises(Exception):
            cur.execute(f"INSERT INTO `{name}` (k) VALUES (%s)", ("é",))
            conn.commit()
        conn.rollback()
        cur.execute(f"DELETE FROM `{name}`")
        cur.execute(f"INSERT INTO `{name}` (k) VALUES (%s)", ("i",))
        conn.commit()
        with pytest.raises(Exception):
            cur.execute(f"INSERT INTO `{name}` (k) VALUES (%s)", ("İ",))
            conn.commit()
        conn.rollback()

        from connectors.generic_sql import _engine

        cfg = {
            "type": "mysql", "host": "127.0.0.1", "port": 3306,
            "database": "dataflow", "username": "dataflow", "password": "dataflow",
        }
        with _engine(cfg).connect() as pooled:
            fact = read_column_collation(
                pooled, "mysql", schema="dataflow", table=name, column="k"
            )
            assert fact is not None
            assert fact.pad.upper() == "NO PAD"
            groups = group_key_values(pooled, "mysql", fact, ["a", "a ", "e", "é", "İ", "i"])
            pooled.commit()
        assert groups["a"] != groups["a "]
        assert groups["e"] == groups["é"]
        assert groups["İ"] == groups["i"]
    finally:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS `{name}`")
        conn.commit()
        conn.close()


def test_sqlserver_pads_trailing_spaces_and_not_tabs(monkeypatch):
    from services.collation_equality import group_key_values, read_column_collation

    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8")
    conn, password = _mssql()
    name = f"d391_{uuid.uuid4().hex[:8]}"
    try:
        cur = conn.cursor()
        cur.execute(
            f"CREATE TABLE dbo.[{name}] (k NVARCHAR(20) COLLATE Latin1_General_CI_AS NOT NULL)"
        )
        cur.execute(f"CREATE UNIQUE INDEX ux ON dbo.[{name}] (k)")
        conn.commit()
        cur.execute(f"INSERT INTO dbo.[{name}] (k) VALUES (%s)", ("a",))
        conn.commit()
        with pytest.raises(Exception):
            cur.execute(f"INSERT INTO dbo.[{name}] (k) VALUES (%s)", ("a ",))
            conn.commit()
        conn.rollback()
        cur.execute(f"INSERT INTO dbo.[{name}] (k) VALUES (%s)", ("a\t",))
        conn.commit()
        cur.execute(f"SELECT COUNT(*) FROM dbo.[{name}]")
        assert cur.fetchone()[0] == 2

        from connectors.generic_sql import _engine

        cfg = {
            "type": "sqlserver", "host": "127.0.0.1", "port": 1433,
            "database": "dataflow", "username": "sa", "password": password,
            "trust_server_certificate": True,
        }
        with _engine(cfg).connect() as pooled:
            fact = read_column_collation(
                pooled, "sqlserver", schema="dbo", table=name, column="k"
            )
            assert fact is not None
            assert fact.pad == "PAD SPACE"
            groups = group_key_values(pooled, "sqlserver", fact, ["a", "a ", "a\t"])
            pooled.commit()
        assert groups["a"] == groups["a "]
        assert groups["a"] != groups["a\t"]
    finally:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS dbo.[{name}]")
        conn.commit()
        conn.close()


def test_a_changed_postgres_schema_version_skips_reuse(monkeypatch):
    from src.transfer.models import EndpointConfig

    from services.staged_preflight import (
        bind_staged_preflight,
        destination_schema_version,
        remember_staged_preflight,
        staged_preflight_for_bound,
    )

    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8")
    conn, cfg = _pg()
    name = f"d308s_{uuid.uuid4().hex[:8]}"
    try:
        cur = conn.cursor()
        cur.execute(f'CREATE TABLE "{name}" (id int)')
        conn.commit()
        dest = EndpointConfig(
            kind="database", format="postgresql", table=name, schema="public",
            host=cfg["host"], port=cfg["port"], database=cfg["database"],
            username=cfg["username"], password=cfg["password"],
        )
        first = destination_schema_version(dest)
        assert first not in {"", "unread"}
        remember_staged_preflight(
            "pf_live", "fp", {"passed": True, "gates": []}, schema_version=first
        )
        with bind_staged_preflight("pf_live", "fp", first):
            assert staged_preflight_for_bound()["reused_staged_preflight"] is True
        cur.execute(f'ALTER TABLE "{name}" ADD COLUMN extra int')
        conn.commit()
        second = destination_schema_version(dest)
        assert second != first
        with bind_staged_preflight("pf_live", "fp", second):
            assert staged_preflight_for_bound() is None
    finally:
        conn.rollback()
        cur = conn.cursor()
        cur.execute(f'DROP TABLE IF EXISTS "{name}"')
        conn.commit()
        conn.close()


def test_a_dead_dns_name_from_mysql_is_dns_not_found():
    import pymysql

    from services.error_taxonomy import classify

    try:
        pymysql.connect(
            host="no-such-host.dataflow.invalid", port=3306, user="dataflow",
            password="dataflow", database="dataflow", connect_timeout=3,
        )
    except Exception as exc:
        classified = classify(exc)
    else:
        pytest.fail("a nonexistent host accepted a MySQL login")
    assert classified is not None
    assert classified.code == "network.dns_not_found"
