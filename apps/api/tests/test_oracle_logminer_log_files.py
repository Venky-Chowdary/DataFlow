"""Oracle LogMiner redo selection and session lifecycle regressions."""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass

import pytest

import connectors.oracle_logminer as oracle_logminer
from connectors.oracle_logminer import (
    CdcScnGapError,
    OracleLogMinerCdc,
    encode_logminer_token,
    register_logminer_logs,
    start_logminer_session,
)


class _FakeOracleError(RuntimeError):
    def __init__(self, code: int) -> None:
        self.code = code
        super().__init__(f"ORA-{code:05d}: simulated LogMiner failure")


@dataclass(frozen=True)
class _RedoPath:
    thread: int
    sequence: int
    path: str
    dest_id: int = 0
    status: str = "A"


class _FakeLogMinerCursor:
    def __init__(
        self,
        *,
        online: list[_RedoPath] | None = None,
        archived: list[_RedoPath] | None = None,
        fail_start: int = 0,
        fail_mine: int = 0,
        fail_add: int = 0,
        fail_final_end: bool = False,
        unreadable: bool = False,
    ) -> None:
        self.online = online or []
        self.archived = archived or []
        self.path_keys = {
            item.path: (item.thread, item.sequence)
            for item in [*self.online, *self.archived]
        }
        self.statement = ""
        self.executed_statements: list[str] = []
        self.listing = ""
        self.session_sequences: set[tuple[int, int]] = set()
        self.added_paths: list[str] = []
        self.end_calls = 0
        self.start_calls = 0
        self.fail_start = fail_start
        self.fail_mine = fail_mine
        self.fail_add = fail_add
        self.fail_final_end = fail_final_end
        self.unreadable = unreadable

    def execute(self, statement: str, params: dict | None = None) -> None:
        self.statement = statement
        self.executed_statements.append(statement)
        normalized = statement.upper()
        if "END_LOGMNR" in normalized:
            self.end_calls += 1
            if self.fail_final_end and self.start_calls:
                self.fail_final_end = False
                raise _FakeOracleError(1307)
            self.session_sequences.clear()
        elif "V$LOGFILE" in normalized:
            if self.unreadable:
                raise RuntimeError("catalog unavailable")
            self.listing = "online"
        elif "V$ARCHIVED_LOG" in normalized:
            if self.unreadable:
                raise RuntimeError("catalog unavailable")
            self.listing = "archived"
        elif "ADD_LOGFILE" in normalized:
            path = str((params or {})["fname"])
            if self.fail_add:
                self.fail_add -= 1
                raise _FakeOracleError(1289)
            key = self.path_keys[path]
            self.added_paths.append(path)
            if "DBMS_LOGMNR.NEW" in normalized:
                self.session_sequences.clear()
            if key in self.session_sequences:
                raise _FakeOracleError(1289)
            self.session_sequences.add(key)
        elif "START_LOGMNR" in normalized:
            self.start_calls += 1
            if self.fail_start:
                self.fail_start -= 1
                raise _FakeOracleError(16331)

    def fetchall(self) -> list[tuple]:
        normalized = self.statement.upper()
        if "V$LOGMNR_CONTENTS" in normalized:
            if self.fail_mine:
                self.fail_mine -= 1
                raise _FakeOracleError(600)
            return [(1,)]
        if self.listing == "online":
            if "THREAD#" in normalized:
                return [
                    (item.thread, item.sequence, item.path)
                    for item in self.online
                ]
            return [(item.path,) for item in self.online]
        if self.listing == "archived":
            rows = self.archived
            if "STATUS = 'A'" in normalized:
                rows = [item for item in rows if item.status == "A"]
            if "THREAD#" in normalized:
                return [
                    (
                        item.thread,
                        item.sequence,
                        item.dest_id,
                        item.status,
                        item.path,
                    )
                    for item in rows
                ]
            return [(item.path,) for item in rows]
        return []

    def fetchone(self) -> tuple[int]:
        return (20,)


def _logminer_session(cur: _FakeLogMinerCursor, **kwargs):
    session = getattr(oracle_logminer, "_logminer_session", None)
    assert callable(session), "_logminer_session context manager is missing"
    return session(cur, **kwargs)


def test_online_and_archived_copies_register_only_the_archived_file() -> None:
    online = _RedoPath(1, 106, "/redo/redo02.log")
    archived = _RedoPath(1, 106, "/archive/arch1_106.dbf", dest_id=1)
    cur = _FakeLogMinerCursor(online=[online], archived=[archived])

    count = register_logminer_logs(cur, start_scn=100, end_scn=200)

    assert count == 1
    assert cur.added_paths == [archived.path]
    assert (1, 106) in cur.session_sequences


def test_multiplexed_online_members_register_the_first_member_name() -> None:
    online = [
        _RedoPath(1, 106, "/redo/redo02b.log"),
        _RedoPath(1, 106, "/redo/redo02a.log"),
    ]
    cur = _FakeLogMinerCursor(online=online)

    count = register_logminer_logs(cur, start_scn=100, end_scn=200)

    assert count == 1
    assert cur.added_paths == ["/redo/redo02a.log"]


def test_archive_destinations_register_the_lowest_available_dest_id() -> None:
    archived = [
        _RedoPath(1, 106, "/archive/dest2/arch.dbf", dest_id=2),
        _RedoPath(1, 106, "/archive/dest1/arch.dbf", dest_id=1),
        _RedoPath(1, 106, "/archive/unavailable/arch.dbf", dest_id=0, status="D"),
    ]
    cur = _FakeLogMinerCursor(archived=archived)

    count = register_logminer_logs(cur, start_scn=100, end_scn=200)

    assert count == 1
    assert cur.added_paths == ["/archive/dest1/arch.dbf"]
    archive_sql = next(
        statement
        for statement in _executed_statements(cur)
        if "V$ARCHIVED_LOG" in statement.upper()
    )
    assert "RESETLOGS_CHANGE#" in archive_sql
    assert "STATUS = 'A'" in archive_sql
    assert "STANDBY_DEST = 'NO'" in archive_sql


def _executed_statements(cur: _FakeLogMinerCursor) -> list[str]:
    return list(getattr(cur, "executed_statements", []))


def test_current_unarchived_online_log_is_registered() -> None:
    current = _RedoPath(1, 107, "/redo/current.log")
    cur = _FakeLogMinerCursor(online=[current])

    count = register_logminer_logs(cur, start_scn=190, end_scn=250)

    assert count == 1
    assert cur.added_paths == [current.path]


def test_registered_file_order_is_thread_then_sequence() -> None:
    online = [
        _RedoPath(2, 1, "/redo/thread2-seq1.log"),
        _RedoPath(1, 5, "/redo/thread1-seq5.log"),
        _RedoPath(1, 2, "/redo/thread1-seq2.log"),
    ]
    cur = _FakeLogMinerCursor(online=online)

    count = register_logminer_logs(cur, start_scn=100, end_scn=200)

    assert count == 3
    assert cur.added_paths == [
        "/redo/thread1-seq2.log",
        "/redo/thread1-seq5.log",
        "/redo/thread2-seq1.log",
    ]


def test_no_files_keeps_the_typed_redo_gap_error() -> None:
    cur = _FakeLogMinerCursor()

    with pytest.raises(CdcScnGapError, match="no redo"):
        start_logminer_session(cur, start_scn=100, end_scn=200)


def test_unreadable_log_catalogs_still_raise() -> None:
    cur = _FakeLogMinerCursor(unreadable=True)

    with pytest.raises(RuntimeError, match="could not read"):
        register_logminer_logs(cur, start_scn=100, end_scn=200)


def test_failed_start_ends_session_and_retry_succeeds(
    caplog: pytest.LogCaptureFixture,
) -> None:
    cur = _FakeLogMinerCursor(
        online=[_RedoPath(1, 106, "/redo/redo02.log")],
        fail_start=1,
    )
    kwargs = {
        "start_scn": 100,
        "end_scn": 200,
        "committed_only": True,
        "reader_identity": "job=test slot=test-slot table=APP.T",
        "step_context": "unit",
    }

    with caplog.at_level("WARNING"):
        with pytest.raises(_FakeOracleError):
            with _logminer_session(cur, **kwargs):
                pass

    assert cur.end_calls == 2
    assert not cur.session_sequences
    assert "step=start_logminer" in caplog.text
    assert "reader=job=test slot=test-slot table=APP.T" in caplog.text
    assert "SCN=[100,200]" in caplog.text

    with _logminer_session(cur, **kwargs):
        pass

    assert cur.start_calls == 2
    assert cur.added_paths == ["/redo/redo02.log", "/redo/redo02.log"]


def test_mid_mine_failure_ends_session_and_retry_succeeds(
    caplog: pytest.LogCaptureFixture,
) -> None:
    cur = _FakeLogMinerCursor(
        online=[_RedoPath(1, 106, "/redo/redo02.log")],
        fail_mine=1,
    )
    kwargs = {
        "start_scn": 100,
        "end_scn": 200,
        "reader_identity": "job=test slot=test-slot table=APP.T",
        "step_context": "unit",
    }

    with caplog.at_level("WARNING"):
        with pytest.raises(_FakeOracleError):
            with _logminer_session(cur, **kwargs):
                cur.execute("SELECT * FROM V$LOGMNR_CONTENTS")
                cur.fetchall()

    assert cur.end_calls == 2
    assert not cur.session_sequences
    assert "step=mine" in caplog.text
    assert "reader=job=test slot=test-slot table=APP.T" in caplog.text
    assert "SCN=[100,200]" in caplog.text

    with _logminer_session(cur, **kwargs):
        cur.execute("SELECT * FROM V$LOGMNR_CONTENTS")
        assert cur.fetchall() == [(1,)]

    assert cur.end_calls == 4


def test_add_failure_is_logged_and_cleanup_runs(caplog: pytest.LogCaptureFixture) -> None:
    cur = _FakeLogMinerCursor(
        online=[_RedoPath(1, 106, "/redo/redo02.log")],
        fail_add=1,
    )

    with caplog.at_level("WARNING"):
        with pytest.raises(_FakeOracleError):
            with _logminer_session(
                cur,
                start_scn=100,
                end_scn=200,
                reader_identity="job=test slot=test-slot table=APP.T",
                step_context="unit",
            ):
                pass

    assert cur.end_calls == 2
    assert "step=add_logfile" in caplog.text
    assert "reader=job=test slot=test-slot table=APP.T" in caplog.text
    assert "SCN=[100,200]" in caplog.text


def test_end_failure_is_logged_without_masking_mining_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    cur = _FakeLogMinerCursor(
        online=[_RedoPath(1, 106, "/redo/redo02.log")],
        fail_final_end=True,
    )

    with caplog.at_level("WARNING"):
        with pytest.raises(_FakeOracleError) as raised:
            with _logminer_session(
                cur,
                start_scn=100,
                end_scn=200,
                reader_identity="job=test slot=test-slot table=APP.T",
                step_context="unit",
            ):
                raise _FakeOracleError(600)

    assert raised.value.code == 600
    assert "step=end_logminer" in caplog.text
    assert "reader=job=test slot=test-slot table=APP.T" in caplog.text
    assert "SCN=[100,200]" in caplog.text


def test_visibility_retry_failure_ends_both_sessions_then_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reader = OracleLogMinerCdc(
        {"host": "localhost", "database": "ORCL", "username": "APP"},
        table="T",
        primary_key="ID",
        schema="APP",
        resume_token=encode_logminer_token(10, table="T", phase="streaming"),
    )
    cur = _FakeLogMinerCursor(online=[_RedoPath(1, 106, "/redo/redo02.log")])
    reader._empty_window_retry_sec = lambda: 0.01
    monkeypatch.setattr(oracle_logminer.time, "sleep", lambda _delay: None)

    real_start = oracle_logminer.start_logminer_session
    start_calls = 0

    def fail_second_start(cur, **kwargs) -> None:
        nonlocal start_calls
        start_calls += 1
        real_start(cur, **kwargs)
        if start_calls == 2:
            raise _FakeOracleError(16331)

    query_calls = 0

    def query_rows(*_args, **_kwargs):
        nonlocal query_calls
        query_calls += 1
        return [] if query_calls == 1 else [(11, "0xA", 1, "INSERT")]

    monkeypatch.setattr(oracle_logminer, "start_logminer_session", fail_second_start)
    monkeypatch.setattr(reader, "_query_logminer_rows", query_rows)
    with pytest.raises(_FakeOracleError):
        reader._fetch_logminer_rows_visible(
            cur,
            start_scn=10,
            end_scn=15,
            table_predicate="TABLE_NAME = :tbl",
            limit=10,
        )

    assert cur.end_calls == 4
    assert not cur.session_sequences

    rows, end_scn = reader._fetch_logminer_rows_visible(
        cur,
        start_scn=10,
        end_scn=15,
        table_predicate="TABLE_NAME = :tbl",
        limit=10,
    )

    assert rows == [(11, "0xA", 1, "INSERT")]
    assert end_scn == 15
    assert cur.end_calls == 6


def _live_oracle_cfg() -> dict:
    os.environ.setdefault("DATAFLOW_ORACLE_ENABLE", "1")
    os.environ.setdefault("DATAFLOW_ORACLE_SERVICE", "FREEPDB1")
    os.environ.setdefault("DATAFLOW_ORACLE_CDB_SERVICE", "FREE")
    from test_cdc_oracle_logminer_transfer_e2e import (
        _oracle_cfg,
        _oracle_logminer_ready,
    )

    if not _oracle_logminer_ready():
        pytest.skip("df-oracle is unreachable or its LogMiner user is not ready")
    return _oracle_cfg()


def _source_connection(cfg: dict):
    from connectors.generic_sql import get_connection

    return get_connection(
        host=cfg["host"],
        port=cfg["port"],
        database=cfg["database"],
        username=cfg["username"],
        password=cfg["password"],
        connection_string="",
        ssl=False,
        db_type="oracle",
    )


def _new_live_reader(cfg: dict, table: str, start_scn: int) -> OracleLogMinerCdc:
    return OracleLogMinerCdc(
        cfg,
        table=table,
        primary_key="ID",
        schema=cfg["schema"],
        resume_token=encode_logminer_token(
            start_scn,
            table=table,
            phase="streaming",
        ),
    )


def _read_live_source(cfg: dict, table: str) -> list[tuple[str, str]]:
    table_ref = f'"{cfg["schema"]}"."{table}"'
    with _source_connection(cfg) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT ID, VALUE_TEXT FROM {table_ref} ORDER BY ID")
            return [
                (str(row[0]), str(row[1]))
                for row in (cur.fetchall() or [])
            ]


def _drop_live_table(cfg: dict, table: str) -> None:
    table_ref = f'"{cfg["schema"]}"."{table}"'
    try:
        with _source_connection(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE {table_ref} PURGE")
            conn.commit()
    except Exception:
        pass


def _sqlplus_rows(sql: str) -> list[str]:
    from test_oracle_redo_inventory import _oracle_admin_sql

    output = _oracle_admin_sql(
        "SET PAGESIZE 0 FEEDBACK OFF HEADING OFF VERIFY OFF\n" + sql
    )
    return [line.strip() for line in output.splitlines() if line.strip()]


def _sqlplus_marker_value(rows: list[str], marker: str) -> str:
    prefix = f"{marker}="
    line = next((row for row in rows if row.startswith(prefix)), None)
    assert line is not None, rows
    return line[len(prefix) :]


@pytest.mark.parametrize("iteration", range(5))
def test_live_mid_mine_failure_ends_session_and_retry_has_no_loss_or_duplicates(
    iteration: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _live_oracle_cfg()
    monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", "0")
    table = "M5_RETRY_" + uuid.uuid4().hex[:10].upper()
    table_ref = f'"{cfg["schema"]}"."{table}"'
    reader = None
    try:
        with _source_connection(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"CREATE TABLE {table_ref} "
                    "(ID NUMBER PRIMARY KEY, VALUE_TEXT VARCHAR2(100))"
                )
                cur.execute(
                    f"ALTER TABLE {table_ref} "
                    "ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS"
                )
                conn.commit()
                cur.execute("SELECT current_scn FROM V$DATABASE")
                start_scn = int(cur.fetchone()[0])
                cur.execute(
                    f"INSERT INTO {table_ref} (ID, VALUE_TEXT) VALUES (1, 'first')"
                )
                cur.execute(
                    f"INSERT INTO {table_ref} (ID, VALUE_TEXT) VALUES (2, 'second')"
                )
            conn.commit()

        reader = _new_live_reader(cfg, table, start_scn)
        original_query = reader._query_logminer_rows
        failed_once = False

        def fail_once(cur, **kwargs):
            nonlocal failed_once
            if not failed_once:
                failed_once = True
                raise RuntimeError("injected mid-mine failure")
            return original_query(cur, **kwargs)

        monkeypatch.setattr(reader, "_query_logminer_rows", fail_once)
        monkeypatch.setattr(
            "services.cdc_incremental_runner.interleave_incremental_snapshot",
            lambda *_args, **_kwargs: iter(()),
        )
        with pytest.raises(oracle_logminer.OracleLogMinerReadError):
            list(reader._poll_once())
        batches = list(reader._poll_once())
        emitted = [
            (str(row["ID"]), str(row["VALUE_TEXT"]))
            for batch in batches
            for row in batch.inserts or []
        ]
        source = _read_live_source(cfg, table)

        assert failed_once
        assert sorted(emitted) == sorted(source)
        assert len(emitted) == len(set(emitted))
        assert len(emitted) == 2
    finally:
        if reader is not None:
            reader.close()
        _drop_live_table(cfg, table)


@pytest.mark.parametrize("iteration", range(5))
def test_live_archived_online_sequence_registers_without_ora_01289(
    iteration: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _live_oracle_cfg()
    monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", "0")

    table = "M5_ARCHIVE_" + uuid.uuid4().hex[:10].upper()
    table_ref = f'"{cfg["schema"]}"."{table}"'
    reader = None
    try:
        with _source_connection(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT LOG_MODE FROM V$DATABASE")
                if str(cur.fetchone()[0]).upper() != "ARCHIVELOG":
                    pytest.skip("df-oracle is not in ARCHIVELOG mode")
                cur.execute(
                    f"CREATE TABLE {table_ref} "
                    "(ID NUMBER PRIMARY KEY, VALUE_TEXT VARCHAR2(100))"
                )
                cur.execute(
                    f"ALTER TABLE {table_ref} "
                    "ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS"
                )
                conn.commit()
                cur.execute("SELECT current_scn FROM V$DATABASE")
                start_scn = int(cur.fetchone()[0])
                cur.execute(
                    f"INSERT INTO {table_ref} (ID, VALUE_TEXT) "
                    "VALUES (1, 'archived-online')"
                )
            conn.commit()
            with conn.cursor() as cur:
                cur.execute("SELECT current_scn FROM V$DATABASE")
                end_scn = int(cur.fetchone()[0])
        assert end_scn >= start_scn

        target_rows = _sqlplus_rows(
            "SELECT 'M5_TARGET=' || THREAD# || ',' || SEQUENCE# FROM V$LOG "
            f"WHERE FIRST_CHANGE# <= {start_scn} AND NEXT_CHANGE# > {end_scn};"
        )
        target = _sqlplus_marker_value(target_rows, "M5_TARGET")
        thread, sequence = map(int, target.split(","))

        from test_oracle_redo_inventory import _oracle_admin_sql

        _oracle_admin_sql("ALTER SYSTEM ARCHIVE LOG CURRENT;")

        online_rows = _sqlplus_rows(
            "SELECT 'M5_ONLINE=' || COUNT(*) FROM V$LOG "
            f"WHERE THREAD# = {thread} AND SEQUENCE# = {sequence} "
            "AND ARCHIVED = 'YES' "
            f"AND FIRST_CHANGE# <= {start_scn} AND NEXT_CHANGE# > {end_scn};"
        )
        archive_rows = _sqlplus_rows(
            "SELECT 'M5_ARCHIVED=' || COUNT(*) FROM V$ARCHIVED_LOG "
            f"WHERE THREAD# = {thread} AND SEQUENCE# = {sequence} "
            "AND DELETED = 'NO' AND STANDBY_DEST = 'NO' AND STATUS = 'A' "
            "AND RESETLOGS_CHANGE# = "
            "(SELECT RESETLOGS_CHANGE# FROM V$DATABASE) "
            f"AND FIRST_CHANGE# <= {start_scn} AND NEXT_CHANGE# > {end_scn};"
        )
        assert int(_sqlplus_marker_value(online_rows, "M5_ONLINE")) > 0
        assert int(_sqlplus_marker_value(archive_rows, "M5_ARCHIVED")) > 0

        reader = _new_live_reader(cfg, table, start_scn)
        monkeypatch.setattr(
            "services.cdc_incremental_runner.interleave_incremental_snapshot",
            lambda *_args, **_kwargs: iter(()),
        )
        batches = list(reader._poll_once())
        emitted = [
            (str(row["ID"]), str(row["VALUE_TEXT"]))
            for batch in batches
            for row in batch.inserts or []
        ]
        source = _read_live_source(cfg, table)

        assert sorted(emitted) == sorted(source) == [("1", "archived-online")]
        assert len(emitted) == len(set(emitted)) == 1
    finally:
        if reader is not None:
            reader.close()
        _drop_live_table(cfg, table)
