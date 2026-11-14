"""CDC identity matrix: a real PostgreSQL logical-replication source -> every
live SQL destination, single and composite keys, read back with each
destination's own driver.

Each cell runs: snapshot, unchanged rerun (0 rows), INSERT + UPDATE + DELETE
rerun, then a primary-key UPDATE (Debezium: delete old key + create new key).
CDC is at-least-once upsert; the population after each run must equal the
source exactly. A keyless CDC stream is refused with zero writes.
"""

from __future__ import annotations

import os
import tempfile
import time
import uuid
from pathlib import Path

import pytest

import src.transfer.engine as engine_mod
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest
from tests.route_cert_readback import read_back
from tests.test_execute_tracked_universal_matrix import _build_db_endpoint, _endpoint_reachable
from tests.test_property2_golden_path_never_blocked import _FakeMongo

ENGINES = ["sqlite", "postgresql", "mysql", "sqlserver", "oracle"]
COLS = ["id", "region", "name", "updated_at"]
PG = {"host": "127.0.0.1", "port": 5432, "database": "dataflow",
      "username": "dataflow", "password": "dataflow"}
MYSQL = {"host": "127.0.0.1", "port": 3306, "database": "dataflow",
         "username": "dataflow", "password": "dataflow"}
MSSQL = {"host": "127.0.0.1", "port": 1433, "database": "dataflow", "username": "sa",
         "password": os.environ.get("DATAFLOW_TEST_MSSQL_PASSWORD", ""),
         "connection_string": "", "ssl": False}
_ORA = _build_db_endpoint("oracle", Path(tempfile.mkdtemp()), "src", "cdc")
ORACLE = {"host": _ORA.host, "port": _ORA.port, "database": _ORA.database,
          "username": _ORA.username, "password": _ORA.password,
          "connection_string": "", "ssl": False}
SOURCES = {"postgresql": PG, "mysql": MYSQL, "sqlserver": MSSQL, "oracle": ORACLE}
KEYS = [("single", "id", "PRIMARY KEY (id)"), ("composite", "region,id", "PRIMARY KEY (region, id)")]


def _pg(statements: list[str]):
    import psycopg2

    conn = psycopg2.connect(host=PG["host"], port=PG["port"], dbname=PG["database"],
                            user=PG["username"], password=PG["password"], connect_timeout=3)
    conn.autocommit = True
    try:
        cur = conn.cursor()
        for sql in statements:
            cur.execute(sql)
        if cur.description:
            return cur.fetchall()
        return None
    finally:
        conn.close()


def _mysql(statements: list[str]):
    import pymysql

    conn = pymysql.connect(host=MYSQL["host"], port=MYSQL["port"], user=MYSQL["username"],
                           password=MYSQL["password"], database=MYSQL["database"],
                           autocommit=True, connect_timeout=3)
    try:
        with conn.cursor() as cur:
            for sql in statements:
                cur.execute(sql)
            return cur.fetchall() if cur.description else None
    finally:
        conn.close()


def _mssql(statements: list[str]):
    import pymssql

    conn = pymssql.connect(server=MSSQL["host"], port=MSSQL["port"], user=MSSQL["username"],
                           password=MSSQL["password"], database=MSSQL["database"],
                           autocommit=True, login_timeout=3)
    try:
        cur = conn.cursor()
        for sql in statements:
            cur.execute(sql)
        return cur.fetchall() if cur.description else None
    finally:
        conn.close()


def _mssql_agent() -> bool:
    try:
        rows = _mssql(["SELECT status_desc FROM sys.dm_server_services "
                       "WHERE servicename LIKE 'SQL Server Agent%'"])
        return bool(rows) and rows[0][0] == "Running"
    except Exception:  # noqa: BLE001 — skip reason below
        return False


def _mssql_enable_cdc(table: str) -> None:
    _mssql(["IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = DB_NAME() "
            "AND is_cdc_enabled = 1) EXEC sys.sp_cdc_enable_db",
            f"EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'{table}', "
            "@role_name = NULL, @supports_net_changes = 0"])


def _mssql_change_count(table: str) -> int:
    """Rows in the capture instance; -1 until SQL Agent has a valid LSN range (error 313)."""
    try:
        return int(_mssql([f"SELECT COUNT(*) FROM cdc.fn_cdc_get_all_changes_dbo_{table}("  # nosec B608
                           f"sys.fn_cdc_get_min_lsn('dbo_{table}'), sys.fn_cdc_get_max_lsn(), "
                           "N'all update old')"])[0][0])
    except Exception:  # noqa: BLE001 — LSN range not ready yet
        return -1


def _mssql_capture(table: str, key: str, before: int) -> None:
    """SQL Agent captures asynchronously and may harvest a multi-statement window
    across scans: wait until the change table grew past ``before`` and then held
    steady for longer than the Agent's 5 s polling interval."""
    from connectors.sqlserver_cdc_native import SqlServerNativeCdc

    deadline = time.monotonic() + 60
    last, steady_since = -1, time.monotonic()
    while time.monotonic() < deadline:
        SqlServerNativeCdc(dict(MSSQL), table=table, primary_key=key, schema="dbo").force_cdc_scan()
        count = _mssql_change_count(table)
        if count != last:
            last, steady_since = count, time.monotonic()
        elif count > before and time.monotonic() - steady_since >= 6:
            return
        time.sleep(0.5)
    pytest.fail(f"SQL Server CDC did not capture the DML on {table} within 60 s")


def _mysql_binlog_row() -> bool:
    try:
        import pymysqlreplication  # noqa: F401

        row = _mysql(["SELECT @@log_bin, @@binlog_format, @@binlog_row_image"])[0]
        return (int(row[0]), row[1], row[2]) == (1, "ROW", "FULL")
    except Exception:  # noqa: BLE001 — skip reason below
        return False


def _oracle(statements: list[str]):
    import oracledb

    conn = oracledb.connect(user=ORACLE["username"], password=ORACLE["password"],
                            dsn=f"{ORACLE['host']}:{ORACLE['port']}/{ORACLE['database']}")
    conn.autocommit = True
    try:
        cur = conn.cursor()
        for sql in statements:
            cur.execute(sql)
        return cur.fetchall() if cur.description else None
    finally:
        conn.close()


def _oracle_logminer() -> bool:
    try:
        from connectors.oracle_logminer import OracleLogMinerCdc

        return OracleLogMinerCdc({**ORACLE, **ORACLE_EXTRA}, table="probe", primary_key="id").is_available()
    except Exception:  # noqa: BLE001 — skip reason below
        return False


ORACLE_EXTRA = {"cdb_service": "FREE", "logminer_username": "C##DATAFLOW"}


def _src(source: str, statements: list[str]):
    return {"postgresql": _pg, "mysql": _mysql, "sqlserver": _mssql,
            "oracle": _oracle}[source](statements)


def _create(source: str, table: str, pk_ddl: str) -> list[str]:
    if source != "oracle":
        return [f"CREATE TABLE {table} (id BIGINT NOT NULL, region VARCHAR(8) NOT NULL, "
                f"name VARCHAR(32), updated_at BIGINT, {pk_ddl})",
                f"INSERT INTO {table} VALUES (1,'eu','a',1),(2,'eu','b',1)"]
    # LogMiner reconstructs key-changing UPDATEs only with full supplemental logging.
    return [f"CREATE TABLE {table} (id NUMBER(19) NOT NULL, region VARCHAR2(8) NOT NULL, "
            f"name VARCHAR2(32), updated_at NUMBER(19), {pk_ddl})",
            f"ALTER TABLE {table} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
            f"INSERT INTO {table} VALUES (1,'eu','a',1)",
            f"INSERT INTO {table} VALUES (2,'eu','b',1)"]


def _drop(source: str, table: str) -> str:
    return f"DROP TABLE {table} PURGE" if source == "oracle" else f"DROP TABLE IF EXISTS {table}"


def _pg_logical() -> bool:
    try:
        return _pg(["SHOW wal_level"])[0][0] == "logical"
    except Exception:  # noqa: BLE001 — skip reason below
        return False


SOURCE_READY = {
    "postgresql": (_pg_logical(), "PostgreSQL with wal_level=logical not reachable"),
    "mysql": (_mysql_binlog_row(), "MySQL with ROW/FULL binlog + pymysqlreplication not reachable"),
    "sqlserver": (_mssql_agent(), "SQL Server with a running SQL Agent not reachable"),
    "oracle": (_oracle_logminer(), "Oracle with ARCHIVELOG, supplemental logging and a "
               "C##DATAFLOW LogMiner user not reachable"),
}
pytestmark = pytest.mark.skipif(not SOURCE_READY["postgresql"][0], reason=SOURCE_READY["postgresql"][1])


def _run(fake: _FakeMongo, table: str, dest: EndpointConfig, key: str, source: str = "postgresql",
         validation_mode: str = "strict"):
    schema = {"postgresql": {"schema": "public"},
              "sqlserver": {"schema": "dbo",
                            "extra": {"trust_server_certificate": True, "encrypt": "yes"}},
              "oracle": {"schema": ORACLE["username"].upper(), "extra": ORACLE_EXTRA},
              }.get(source, {})
    request = TransferRequest(
        source=EndpointConfig(kind="database", format=source, table=table, **schema,
                              **SOURCES[source]),
        destination=dest,
        sync_mode="cdc",
        validation_mode=validation_mode,
        mappings=[{"source": c, "target": c, "confidence": 1.0, "approved": True,
                   "user_override": True} for c in COLS],
        stream_contracts=[{"name": table, "selected": True, "sync_mode": "cdc", "primary_key": key}],
        ddl_changes_acknowledged=True,
    )
    job_id = uuid.uuid4().hex[:24]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _norm(value) -> str:
    text = "" if value is None else str(value)
    return text[:-2] if text.endswith(".0") else text


def _population(dest: EndpointConfig) -> list[tuple]:
    return sorted(tuple(_norm(r[c]) for c in COLS) for r in read_back(dest, COLS))


def _source(table: str, source: str = "postgresql") -> list[tuple]:
    rows = _src(source, [f"SELECT id, region, name, updated_at FROM {table}"])  # nosec B608
    return sorted(tuple(_norm(v) for v in r) for r in rows)


def _drop_slots(table: str) -> None:
    """Drop this test's slots, active ones included.

    ``WHERE NOT active`` skipped a slot still held by the run's replication
    connection at teardown, and the inactive slot then pinned WAL (1.76 GB,
    ``df_dataflow_cdcord_*``) until the disk filled. The holder is ended
    first; a failure is reported, not swallowed. ``tests/pg_slot_guard``
    drops anything left at session end, SIGTERM included.
    """
    like = f"%{table[-8:]}%"
    try:
        _pg([f"SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots "  # nosec B608
             f"WHERE active AND slot_name LIKE '{like}'",
             "SELECT pg_sleep(0.2)",
             f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "  # nosec B608
             f"WHERE NOT active AND slot_name LIKE '{like}'"])
    except Exception as exc:  # noqa: BLE001 — cleanup must not mask the test result
        import warnings

        warnings.warn(f"CDC slot cleanup for {table} failed: {exc}", stacklevel=2)


@pytest.fixture
def fake(monkeypatch):
    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    return fake


@pytest.mark.parametrize("source", list(SOURCES))
@pytest.mark.parametrize("engine", ENGINES)
@pytest.mark.parametrize(("shape", "key", "pk_ddl"), KEYS, ids=[k[0] for k in KEYS])
def test_cdc_identity_lifecycle(source, engine, shape, key, pk_ddl, fake, tmp_path: Path) -> None:
    ready, reason = SOURCE_READY[source]
    if not ready:
        pytest.skip(reason)
    dest = _build_db_endpoint(engine, tmp_path, "dst", uuid.uuid4().hex[:8])
    if engine != "sqlite" and not _endpoint_reachable(dest):
        pytest.skip(f"{engine} not reachable on its configured host/port")
    table = "cdcid_" + uuid.uuid4().hex[:8]
    _src(source, _create(source, table, pk_ddl))
    try:
        if source == "sqlserver":
            _mssql_enable_cdc(table)
        steps = [
            ("snapshot", [], 2),
            ("unchanged", [], 0),
            ("insert_update_delete", [
                f"UPDATE {table} SET name='B', updated_at=2 WHERE id=2",
                f"INSERT INTO {table} VALUES (3,'us','c',2)",
                f"DELETE FROM {table} WHERE id=1"], None),
            ("key_mutation", [f"UPDATE {table} SET id=30 WHERE id=3"], None),
        ]
        for label, dml, written in steps:
            if dml:
                before = _mssql_change_count(table) if source == "sqlserver" else 0
                _src(source, dml)
                if source == "sqlserver":
                    _mssql_capture(table, key, max(before, 0))
            result = _run(fake, table, dest, key, source)
            assert result.success, f"{label}: {result.error}"
            if written is not None:
                assert int(result.records_transferred or 0) == written, label
            assert (result.reconciliation or {}).get("passed") is not False, (label, result.reconciliation)
            assert _population(dest) == _source(table, source), label
    finally:
        _src(source, [_drop(source, table)])
        if source == "postgresql":
            _drop_slots(table)


def test_cdc_keyless_stream_is_refused_with_zero_writes(fake, tmp_path: Path) -> None:
    dest = _build_db_endpoint("sqlite", tmp_path, "dst", uuid.uuid4().hex[:8])
    table = "cdcid_" + uuid.uuid4().hex[:8]
    _pg([f"CREATE TABLE {table} (id BIGINT, region VARCHAR(8), name VARCHAR(32), updated_at BIGINT)",
         f"INSERT INTO {table} VALUES (1,'eu','a',1)"])
    try:
        result = _run(fake, table, dest, "")
        assert not result.success
        assert int(result.records_transferred or 0) == 0
        assert "key" in (result.error or "").lower(), result.error
        try:
            landed = read_back(dest, COLS)
        except Exception:  # noqa: BLE001 — the refused run never created the table
            landed = []
        assert not landed
    finally:
        _pg([f"DROP TABLE IF EXISTS {table}"])
        _drop_slots(table)


@pytest.mark.parametrize("source", list(SOURCES))
@pytest.mark.parametrize("engine", ENGINES)
def test_cdc_cross_transaction_order_per_key(source, engine, fake, tmp_path: Path) -> None:
    """Separate transactions on one key in one window: the last committed image wins.

    Applying a window as inserts → updates → deletes lists would resurrect the
    UPDATE after a DELETE+INSERT, or delete the re-inserted row.
    """
    ready, reason = SOURCE_READY[source]
    if not ready:
        pytest.skip(reason)
    dest = _build_db_endpoint(engine, tmp_path, "dst", uuid.uuid4().hex[:8])
    if engine != "sqlite" and not _endpoint_reachable(dest):
        pytest.skip(f"{engine} not reachable on its configured host/port")
    table = "cdcord_" + uuid.uuid4().hex[:8]
    _src(source, _create(source, table, "PRIMARY KEY (id)"))
    try:
        if source == "sqlserver":
            _mssql_enable_cdc(table)
        result = _run(fake, table, dest, "id", source)
        assert result.success, result.error
        before = _mssql_change_count(table) if source == "sqlserver" else 0
        for stmt in [
            f"UPDATE {table} SET name='X', updated_at=2 WHERE id=2",
            f"DELETE FROM {table} WHERE id=2",
            f"INSERT INTO {table} VALUES (2,'eu','Y',3)",
            f"INSERT INTO {table} VALUES (4,'eu','t',3)",
            f"DELETE FROM {table} WHERE id=4",
            f"UPDATE {table} SET name='Z', updated_at=4 WHERE id=1",
            # A stored key deleted, re-inserted and deleted again leaves it gone.
            f"DELETE FROM {table} WHERE id=1",
            f"INSERT INTO {table} VALUES (1,'eu','W',5)",
            f"DELETE FROM {table} WHERE id=1",
        ]:
            _src(source, [stmt])
        if source == "sqlserver":
            _mssql_capture(table, "id", max(before, 0))
        result = _run(fake, table, dest, "id", source)
        assert _population(dest) == _source(table, source)
        assert result.success, result.error
        assert (result.reconciliation or {}).get("passed") is not False, result.reconciliation
    finally:
        _src(source, [_drop(source, table)])
        if source == "postgresql":
            _drop_slots(table)


_WIDEN = {
    "postgresql": "ALTER TABLE {t} ALTER COLUMN name TYPE VARCHAR(100)",
    "mysql": "ALTER TABLE {t} MODIFY name VARCHAR(100)",
    "sqlserver": "ALTER TABLE {t} ALTER COLUMN name NVARCHAR(100)",
    "oracle": "ALTER TABLE {t} MODIFY (name VARCHAR2(100 CHAR))",
}


@pytest.mark.parametrize("engine", list(_WIDEN))
def test_cdc_refused_drift_window_is_held_not_lost(engine, fake, tmp_path: Path) -> None:
    """A change window the destination cannot hold losslessly (source column
    widened past the destination width) is refused with zero writes; once the
    operator widens the destination, the next run delivers the held window."""
    from tests.route_cert_readback import execute_sql

    table = "cdcw_" + uuid.uuid4().hex[:8]
    dest = _build_db_endpoint(engine, tmp_path, "dst", table)
    if not _endpoint_reachable(dest):
        pytest.skip(f"{engine} destination not reachable")
    try:
        _pg(_create("postgresql", table, "PRIMARY KEY (id)"))
        snap = _run(fake, table, dest, "id")
        assert snap.success, snap.error
        held = _population(dest)
        _pg([f"ALTER TABLE {table} ALTER COLUMN name TYPE VARCHAR(100)",
             f"INSERT INTO {table} VALUES (3,'eu','{'x' * 60}',2)",
             f"INSERT INTO {table} VALUES (4,'eu','ok',2)"])
        refused = _run(fake, table, dest, "id")
        assert not refused.success
        assert "fidelity" in (refused.error or "").lower(), refused.error
        assert _population(dest) == held, "a refused window must write nothing"
        execute_sql(dest, [_WIDEN[engine].format(t=dest.table)])
        resumed = _run(fake, table, dest, "id")
        assert resumed.success, resumed.error
        assert _population(dest) == _source(table), "the held window was lost"
    finally:
        _pg([f"DROP TABLE IF EXISTS {table}"])
        _drop_slots(table)


_CHECK = {
    "postgresql": ("ALTER TABLE {t} ADD CONSTRAINT {c} CHECK (name <> 'bad')",
                   "ALTER TABLE {t} DROP CONSTRAINT {c}"),
    "mysql": ("ALTER TABLE {t} ADD CONSTRAINT {c} CHECK (name <> 'bad')",
              "ALTER TABLE {t} DROP CHECK {c}"),
    "sqlserver": ("ALTER TABLE {t} ADD CONSTRAINT {c} CHECK (name <> 'bad')",
                  "ALTER TABLE {t} DROP CONSTRAINT {c}"),
    "oracle": ("ALTER TABLE {t} ADD CONSTRAINT {c} CHECK (name <> 'bad')",
               "ALTER TABLE {t} DROP CONSTRAINT {c}"),
}


@pytest.mark.parametrize("engine", list(_CHECK))
def test_cdc_check_violation_window_is_held_not_lost(engine, fake, tmp_path: Path) -> None:
    """A change the destination's CHECK rejects holds the whole window: zero
    writes, the stream does not advance past it on rerun, and once the operator
    fixes the constraint the held window — rejected and innocent rows — lands.
    Matches a Debezium JDBC sink's ``errors.tolerance=none`` default: a CDC
    image is never parked out of order for a later stale replay."""
    from tests.route_cert_readback import execute_sql

    table = "cdcc_" + uuid.uuid4().hex[:8]
    dest = _build_db_endpoint(engine, tmp_path, "dst", table)
    if not _endpoint_reachable(dest):
        pytest.skip(f"{engine} destination not reachable")
    add, drop = (s.format(t=dest.table, c=f"ck_{table}") for s in _CHECK[engine])
    try:
        _pg(_create("postgresql", table, "PRIMARY KEY (id)"))
        snap = _run(fake, table, dest, "id")
        assert snap.success, snap.error
        held = _population(dest)
        execute_sql(dest, [add])
        _pg([f"INSERT INTO {table} VALUES (3,'eu','bad',2)",
             f"INSERT INTO {table} VALUES (4,'eu','ok',2)"])
        for attempt in ("refused", "rerun"):
            refused = _run(fake, table, dest, "id")
            assert not refused.success, attempt
            assert "check constraint" in (refused.error or "").lower(), refused.error
            assert _population(dest) == held, f"{attempt}: a refused window must write nothing"
        execute_sql(dest, [drop])
        resumed = _run(fake, table, dest, "id")
        assert resumed.success, resumed.error
        assert (resumed.reconciliation or {}).get("passed") is not False, resumed.reconciliation
        assert _population(dest) == _source(table), "the held window was lost"
    finally:
        _pg([f"DROP TABLE IF EXISTS {table}"])
        _drop_slots(table)


def test_cdc_unprobeable_check_rejected_at_write_holds_window(fake, tmp_path: Path) -> None:
    """A CHECK preflight cannot evaluate (MySQL ``REGEXP``) is caught by the
    writer: the rejected change never lands and the window is not advanced, so
    after the fix it is delivered. Rows of the same window may already have
    landed — at-least-once upsert, re-applied idempotently on delivery."""
    from tests.route_cert_readback import execute_sql

    table = "cdcr_" + uuid.uuid4().hex[:8]
    dest = _build_db_endpoint("mysql", tmp_path, "dst", table)
    if not _endpoint_reachable(dest):
        pytest.skip("mysql destination not reachable")
    try:
        _pg(_create("postgresql", table, "PRIMARY KEY (id)"))
        assert _run(fake, table, dest, "id").success
        execute_sql(dest, [f"ALTER TABLE {dest.table} ADD CONSTRAINT ck_{table} "
                           "CHECK (name NOT REGEXP '^bad$')"])
        _pg([f"INSERT INTO {table} VALUES (3,'eu','bad',2)",
             f"INSERT INTO {table} VALUES (4,'eu','ok',2)"])
        source = set(_source(table))
        for attempt in ("refused", "rerun"):
            refused = _run(fake, table, dest, "id")
            assert not refused.success, attempt
            assert "check constraint" in (refused.error or "").lower(), refused.error
            landed = set(_population(dest))
            assert landed <= source, f"{attempt}: a row absent from source landed"
            assert ("3", "eu", "bad", "2") not in landed, attempt
        execute_sql(dest, [f"ALTER TABLE {dest.table} DROP CHECK ck_{table}"])
        resumed = _run(fake, table, dest, "id")
        assert resumed.success, resumed.error
        assert (resumed.reconciliation or {}).get("passed") is not False, resumed.reconciliation
        assert _population(dest) == _source(table), "the held window was lost"
    finally:
        _pg([f"DROP TABLE IF EXISTS {table}"])
        _drop_slots(table)


@pytest.mark.parametrize("engine", list(_CHECK))
def test_cdc_rejected_image_superseded_in_same_txn_lands(engine, fake, tmp_path: Path) -> None:
    """One transaction inserts a row the destination CHECK rejects, then fixes
    it. Only the latest image per key is applied (Debezium JDBC sink reduction
    buffer), so the window lands, the CHECK holds, nothing is quarantined."""
    from tests.route_cert_readback import execute_sql

    table = "cdcs_" + uuid.uuid4().hex[:8]
    dest = _build_db_endpoint(engine, tmp_path, "dst", table)
    if not _endpoint_reachable(dest):
        pytest.skip(f"{engine} destination not reachable")
    add, _drop_ck = (s.format(t=dest.table, c=f"ck_{table}") for s in _CHECK[engine])
    try:
        _pg(_create("postgresql", table, "PRIMARY KEY (id)"))
        assert _run(fake, table, dest, "id").success
        execute_sql(dest, [add])
        _pg([f"BEGIN; INSERT INTO {table} VALUES (3,'eu','bad',2); "
             f"UPDATE {table} SET name='ok', updated_at=3 WHERE id=3; COMMIT"])
        landed = _run(fake, table, dest, "id")
        assert landed.success, landed.error
        assert (landed.reconciliation or {}).get("passed") is not False, landed.reconciliation
        assert not (landed.destination_summary or {}).get("rejected_details")
        assert _population(dest) == _source(table)
    finally:
        _pg([f"DROP TABLE IF EXISTS {table}"])
        _drop_slots(table)


@pytest.mark.parametrize("engine", list(_CHECK))
def test_cdc_quarantined_image_superseded_by_later_change_closes(engine, fake, tmp_path: Path) -> None:
    """Balanced CDC quarantines an image the destination CHECK rejects; a later
    transaction fixes that key and lands. The stale image must leave the open
    quarantine (job summary and DLQ) so Gate-8 grades the key on its newer row
    and no replay can regress it to the rejected value."""
    from services.quarantine_dlq import quarantine_details_from_dlq
    from tests.route_cert_readback import execute_sql

    table = "cdcq_" + uuid.uuid4().hex[:8]
    dest = _build_db_endpoint(engine, tmp_path, "dst", table)
    if not _endpoint_reachable(dest):
        pytest.skip(f"{engine} destination not reachable")
    add, _drop_ck = (s.format(t=dest.table, c=f"ck_{table}") for s in _CHECK[engine])
    try:
        _pg(_create("postgresql", table, "PRIMARY KEY (id)"))
        assert _run(fake, table, dest, "id", validation_mode="balanced").success
        execute_sql(dest, [add])
        _pg([f"INSERT INTO {table} VALUES (3,'eu','bad',2)"])
        _pg([f"UPDATE {table} SET name='ok', updated_at=3 WHERE id=3"])
        landed = _run(fake, table, dest, "id", validation_mode="balanced")
        summary = landed.destination_summary or {}
        assert landed.success, landed.error
        assert (landed.reconciliation or {}).get("passed") is not False, landed.reconciliation
        assert not summary.get("rejected_details"), summary.get("rejected_details")
        assert int(summary.get("rejected_rows") or 0) == 0
        assert quarantine_details_from_dlq(str(landed.job_id), include_skip_audit=False) == []
        assert _population(dest) == _source(table)
    finally:
        _pg([f"DROP TABLE IF EXISTS {table}"])
        _drop_slots(table)
