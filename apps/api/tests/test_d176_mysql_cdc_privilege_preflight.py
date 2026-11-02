"""D176: MySQL CDC preflight refuses a user without REPLICATION SLAVE/CLIENT.

``is_available`` used to swallow the privilege failure ("poll will raise with
detail"), so Validate and the run's own preflight passed, the snapshot
committed, and the job then died on error 1227. The privilege check now runs
before any write and names both grants.
"""

from __future__ import annotations

import os
import socket
import uuid

import pytest

pytest.importorskip("pymysqlreplication", reason="requires the MySQL replication dependency")

from connectors.mysql_change_stream import MySqlChangeStreamCdc
from services.cdc_capability import CAUSE_PRIVILEGE
from services.cdc_log_capture_probe import build_log_capture_gate, probe_log_capture


class _Cursor:
    def __init__(self, grants: list[str] | Exception) -> None:
        self._grants = grants
        self._rows: list[tuple] = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql: str, *args) -> None:
        text = " ".join(sql.split()).upper()
        if "LIKE 'LOG_BIN'" in text:
            self._rows = [("log_bin", "ON")]
        elif "LIKE 'BINLOG_FORMAT'" in text:
            self._rows = [("binlog_format", "ROW")]
        elif "LIKE 'BINLOG_ROW_IMAGE'" in text:
            self._rows = [("binlog_row_image", "FULL")]
        elif text.startswith("SHOW GRANTS"):
            if isinstance(self._grants, Exception):
                raise self._grants
            self._rows = [(g,) for g in self._grants]
        else:
            raise AssertionError(f"unexpected SQL {sql!r}")

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class _Conn:
    def __init__(self, grants: list[str] | Exception) -> None:
        self._grants = grants

    def cursor(self):
        return _Cursor(self._grants)

    def close(self) -> None:
        pass


def _cdc(grants: list[str] | Exception) -> MySqlChangeStreamCdc:
    cfg = {"host": "h", "port": 3306, "database": "d", "username": "u", "password": "x"}
    cdc = MySqlChangeStreamCdc(cfg, table="t", primary_key="id")
    cdc._conn = lambda: _Conn(grants)  # type: ignore[method-assign]
    return cdc


def _available(cdc: MySqlChangeStreamCdc) -> bool:
    # The reader constructor is irrelevant here: no server is attached.
    from unittest.mock import patch

    with patch("pymysqlreplication.BinLogStreamReader"):
        return cdc.is_available()


@pytest.mark.parametrize(
    "grants,missing",
    [
        (["GRANT SELECT ON `d`.* TO `u`@`%`"], ["REPLICATION SLAVE", "REPLICATION CLIENT"]),
        (
            ["GRANT SELECT, REPLICATION CLIENT ON *.* TO `u`@`%`"],
            ["REPLICATION SLAVE"],
        ),
        (
            ["GRANT SELECT, REPLICATION SLAVE ON *.* TO `u`@`%`"],
            ["REPLICATION CLIENT"],
        ),
        # A schema-level grant of the same name is not a global privilege.
        (["GRANT REPLICATION SLAVE ON `d`.* TO `u`@`%`"], ["REPLICATION SLAVE"]),
    ],
)
def test_missing_replication_grant_is_refused_with_both_names(grants, missing) -> None:
    cdc = _cdc(grants)
    assert _available(cdc) is False
    reason = cdc.unavailable_reason
    assert reason is not None and reason.cause == CAUSE_PRIVILEGE
    assert "REPLICATION SLAVE and REPLICATION CLIENT" in reason.detail
    for name in missing:
        assert name in reason.detail
    assert reason.fail_closed is True


@pytest.mark.parametrize(
    "grants",
    [
        ["GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO `u`@`%`"],
        ["GRANT ALL PRIVILEGES ON *.* TO `u`@`%` WITH GRANT OPTION"],
        ["GRANT REPLICATION SLAVE, SUPER ON *.* TO `u`@`%`"],
        ["GRANT BINLOG MONITOR, REPLICATION SLAVE ON *.* TO `u`@`%`"],
        [
            "GRANT USAGE ON *.* TO `u`@`%`",
            "GRANT REPLICATION SLAVE ON *.* TO `u`@`%`",
            "GRANT REPLICATION CLIENT ON *.* TO `u`@`%`",
        ],
    ],
)
def test_sufficient_grants_pass(grants) -> None:
    cdc = _cdc(grants)
    assert _available(cdc) is True
    assert cdc.unavailable_reason is None


def test_unreadable_grants_do_not_block() -> None:
    """If SHOW GRANTS itself fails the check is inconclusive; poll still raises."""
    assert _available(_cdc(RuntimeError("SHOW GRANTS denied"))) is True


def test_validate_gate_blocks_on_the_privilege_refusal(monkeypatch) -> None:
    monkeypatch.setattr(
        "connectors.mysql_change_stream.MySqlChangeStreamCdc._conn",
        lambda self: _Conn(["GRANT SELECT ON `d`.* TO `u`@`%`"]),
    )
    from unittest.mock import patch

    with patch("pymysqlreplication.BinLogStreamReader"):
        probe = probe_log_capture(
            "mysql",
            {"host": "h", "port": 3306, "database": "d", "username": "u", "password": "x"},
            table="t",
            primary_key="id",
        )
    assert probe.available is False and probe.cause == CAUSE_PRIVILEGE
    gate = build_log_capture_gate(
        probe,
        cursor_field="id",
        cursor_semantics="",
        primary_key_columns=["id"],
        pass_status="pass",
        block_status="block",
    )
    assert gate is not None and gate["status"] == "block"
    assert "REPLICATION SLAVE and REPLICATION CLIENT" in gate["message"]


# --- live MySQL: a real user without replication grants -----------------------

_NOREPL_USER = os.environ.get("DATAFLOW_TEST_MYSQL_NOREPL_USER", "")
_NOREPL_PASSWORD = os.environ.get("DATAFLOW_TEST_MYSQL_NOREPL_PASSWORD", "")
_NOREPL_DB = os.environ.get("DATAFLOW_TEST_MYSQL_NOREPL_DB", "")


def _mysql_up() -> bool:
    host = os.environ.get("DATAFLOW_TEST_MYSQL_HOST", "localhost")
    port = int(os.environ.get("DATAFLOW_TEST_MYSQL_PORT", "3306"))
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


@pytest.mark.skipif(
    not (_NOREPL_USER and _NOREPL_PASSWORD and _NOREPL_DB and _mysql_up()),
    reason="set DATAFLOW_TEST_MYSQL_NOREPL_USER/_PASSWORD/_DB to a user with SELECT "
    "on one schema and no REPLICATION grants",
)
def test_live_user_without_replication_grants_is_refused() -> None:
    cfg = {
        "host": os.environ.get("DATAFLOW_TEST_MYSQL_HOST", "localhost"),
        "port": int(os.environ.get("DATAFLOW_TEST_MYSQL_PORT", "3306")),
        "database": _NOREPL_DB,
        "username": _NOREPL_USER,
        "password": _NOREPL_PASSWORD,
        "ssl": False,
        "job_id": f"d176-{uuid.uuid4().hex[:8]}",
    }
    cdc = MySqlChangeStreamCdc(cfg, table="anything", primary_key="id")
    assert cdc.is_available() is False
    assert cdc.unavailable_reason is not None
    assert cdc.unavailable_reason.cause == CAUSE_PRIVILEGE
    assert "REPLICATION SLAVE and REPLICATION CLIENT" in cdc.unavailable_reason.detail


@pytest.mark.skipif(
    not (_NOREPL_USER and _NOREPL_PASSWORD and _NOREPL_DB and _mysql_up()),
    reason="needs the no-replication MySQL user (see above)",
)
def test_live_cdc_run_refuses_before_any_destination_write(tmp_path, monkeypatch) -> None:
    import sqlite3

    import pymysql

    import src.transfer.engine as engine_mod
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    host = os.environ.get("DATAFLOW_TEST_MYSQL_HOST", "localhost")
    port = int(os.environ.get("DATAFLOW_TEST_MYSQL_PORT", "3306"))
    table = "d176_" + uuid.uuid4().hex[:8]
    src = pymysql.connect(
        host=host, port=port, user=_NOREPL_USER, password=_NOREPL_PASSWORD, database=_NOREPL_DB
    )
    try:
        with src.cursor() as cur:
            cur.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, v VARCHAR(20))")
            cur.execute(f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b')")
        src.commit()

        fake = _FakeMongo()
        monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
        import services.sync_cursor as sc

        monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
        monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
        dst = tmp_path / "dst.sqlite"
        request = TransferRequest(
            source=EndpointConfig(
                kind="database", format="mysql", host=host, port=port,
                database=_NOREPL_DB, username=_NOREPL_USER, password=_NOREPL_PASSWORD,
                table=table,
            ),
            destination=EndpointConfig(
                kind="database", format="sqlite", database=str(dst), table=table
            ),
            sync_mode="cdc_incremental",
            stream_contracts=[
                {"name": table, "primary_key": ["id"], "sync_mode": "cdc_incremental",
                 "selected": True}
            ],
            skip_preflight=True,
            validation_mode="strict",
        )
        job_id = "d176" + uuid.uuid4().hex[:16]
        fake.update_job_status(job_id, "pending", transfer_request={})
        result = UniversalTransferEngine().execute_tracked(request, job_id)

        assert not result.success
        assert "REPLICATION SLAVE and REPLICATION CLIENT" in str(result.error)
        if dst.exists():
            conn = sqlite3.connect(str(dst))
            try:
                tables = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            finally:
                conn.close()
            assert tables == [], f"destination was written before the refusal: {tables}"
    finally:
        with src.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
        src.commit()
        src.close()
