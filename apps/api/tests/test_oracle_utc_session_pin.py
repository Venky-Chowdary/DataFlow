"""Accuracy review finding 8: Oracle instants are written on a confirmed UTC session.

python-oracledb sends a datetime's clock digits without its offset, so every
Oracle connection DataFlow opens runs ``ALTER SESSION SET TIME_ZONE = '+00:00'``
and reads ``SESSIONTIMEZONE`` back, and the writer binds UTC digits. An offset
or instant into an Oracle zone column is graded instant-preserving only when
this run's destination session read back UTC; otherwise it needs a Risk
Contract and the reason says why. These checks use fakes (no Oracle here); the
live proof is tests/test_oracle_utc_session_pin_live.py.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
import sqlalchemy as sa

from connectors.oracle_session import (
    INFO_KEY,
    PIN_STATEMENT,
    install_utc_session_pin,
    oracle_instant_bind,
    pin_utc_session,
    session_zone_is_utc,
)
from services.oracle_session_scope import (
    bind_oracle_utc_pin,
    oracle_destination_scope,
    oracle_pin_bound,
    oracle_utc_pin,
    same_oracle_instance,
)
from services.source_engine_scope import bind_source_engine
from services.type_coercion import is_lossy_coercion


class _Cursor:
    def __init__(self, conn):
        self.conn = conn
        self._row = None

    def execute(self, sql, *_a):
        self.conn.statements.append(sql)
        if sql == PIN_STATEMENT:
            if self.conn.refuse_pin:
                raise RuntimeError("ORA-01031: insufficient privileges")
            self.conn.zone = self.conn.after_pin
        elif "SESSIONTIMEZONE" in sql:
            if self.conn.refuse_read:
                raise RuntimeError("ORA-00942")
            self._row = (self.conn.zone,)

    def fetchone(self):
        return self._row

    def close(self):
        pass


class _Conn:
    def __init__(self, zone="America/St_Johns", after_pin="+00:00", refuse_pin=False, refuse_read=False):
        self.zone, self.after_pin = zone, after_pin
        self.refuse_pin, self.refuse_read = refuse_pin, refuse_read
        self.statements: list[str] = []

    def cursor(self):
        return _Cursor(self)


@pytest.mark.parametrize("reading,utc", [
    ("+00:00", True), ("UTC", True), ("Etc/UTC", True), (" +00:00 ", True),
    ("-00:00", True), ("+05:30", False), ("Europe/London", False), ("America/St_Johns", False), ("", False),
])
def test_only_a_utc_reading_counts(reading, utc):
    assert session_zone_is_utc(reading) is utc


def test_pin_runs_alter_session_then_reads_the_zone_back():
    conn = _Conn()
    assert pin_utc_session(conn) == (True, "session time zone read back as +00:00")
    assert conn.statements[0] == PIN_STATEMENT and "SESSIONTIMEZONE" in conn.statements[1]


@pytest.mark.parametrize("conn,why", [
    (_Conn(refuse_pin=True), "refused"),
    (_Conn(after_pin="America/St_Johns"), "not UTC"),
    (_Conn(refuse_read=True), "could not be read back"),
])
def test_an_unconfirmed_pin_says_why_and_never_raises(conn, why):
    pinned, reason = pin_utc_session(conn)
    assert pinned is False and why in reason


def _sqlite_engine_with_pin():
    # SQLite refuses ALTER SESSION: the pool behaves as an Oracle pool whose pin failed.
    engine = sa.create_engine("sqlite://")
    install_utc_session_pin(engine)
    return engine


def test_failed_pin_is_recorded_and_refuses_a_run_graded_on_a_utc_session():
    engine = _sqlite_engine_with_pin()
    with engine.connect() as conn:  # unchecked run: the Risk Contract was required, write proceeds
        pinned, reason = conn.connection._connection_record.info[INFO_KEY]
        assert pinned is False and "refused" in reason
    engine.dispose()
    engine = _sqlite_engine_with_pin()
    with bind_oracle_utc_pin(True, "session time zone read back as +00:00"):
        with pytest.raises(RuntimeError, match="pin lost"):
            engine.connect()


def test_writer_binds_utc_digits_and_refuses_a_naive_value():
    ist = timezone(timedelta(hours=5, minutes=30))
    assert oracle_instant_bind(datetime(2024, 1, 2, 3, 4, 5, 123000, tzinfo=ist)) == datetime(
        2024, 1, 1, 21, 34, 5, 123000)
    with pytest.raises(ValueError, match="naive"):
        oracle_instant_bind(datetime(2024, 1, 2, 3, 4, 5))


@pytest.mark.parametrize("target", ["TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP(6) WITH LOCAL TIME ZONE"])
def test_generic_writer_sends_utc_digits_to_oracle_zone_columns(target):
    from sqlalchemy.dialects import oracle

    from connectors.generic_sql import _to_sa_value

    sa_type = oracle.TIMESTAMP(local_timezone="LOCAL" in target, timezone="LOCAL" not in target)
    got = _to_sa_value("2024-03-10 01:59:59.999999-05:00", target, sa_type=sa_type, db_type="oracle")
    assert got == datetime(2024, 3, 10, 6, 59, 59, 999999) and got.tzinfo is None


@pytest.mark.parametrize("engine,src", [
    ("sqlserver", "DATETIMEOFFSET(6)"), ("postgresql", "TIMESTAMPTZ(6)"), ("csv", "TIMESTAMPTZ"),
    ("oracle", "TIMESTAMP(6) WITH TIME ZONE"),
])
def test_grade_follows_the_session_read_back(engine, src):
    tgt = "TIMESTAMP(6) WITH TIME ZONE"
    with bind_source_engine(engine):
        assert is_lossy_coercion(src, tgt, dest_db="oracle"), "unchecked session: Risk Contract"
        with bind_oracle_utc_pin(False, "session time zone read back as America/St_Johns, not UTC"):
            assert is_lossy_coercion(src, tgt, dest_db="oracle")
        with bind_oracle_utc_pin(True, "session time zone read back as +00:00"):
            assert not is_lossy_coercion(src, tgt, dest_db="oracle")


def test_unconfirmed_reason_reaches_the_grade():
    from services.source_engine_semantics import oracle_zone_bind_shifts_instant

    with bind_oracle_utc_pin(False, "ALTER SESSION SET TIME_ZONE was refused (DatabaseError)"):
        why = oracle_zone_bind_shifts_instant("TIMESTAMPTZ", "TIMESTAMP WITH LOCAL TIME ZONE", dest_db="oracle")
    assert "was refused" in why and "may move" in why


def test_same_instance_oracle_copy_is_exempt_cross_host_is_not():
    src = tgt = "TIMESTAMP(6) WITH TIME ZONE"
    with bind_source_engine("oracle"):
        with bind_oracle_utc_pin(False, "not checked", same_instance=True):
            assert not is_lossy_coercion(src, tgt, dest_db="oracle")
        with bind_oracle_utc_pin(False, "not checked", same_instance=False):
            assert is_lossy_coercion(src, tgt, dest_db="oracle")
    a = {"host": "db1", "port": 1521, "service_name": "FREEPDB1"}
    assert same_oracle_instance("oracle", a, "oracle", dict(a))
    assert not same_oracle_instance("oracle", a, "oracle", {**a, "host": "db2"})
    assert not same_oracle_instance("postgresql", a, "oracle", dict(a))


def test_destination_scope_probes_once_and_keeps_an_outer_binding(monkeypatch):
    import services.oracle_session_scope as scope

    calls = []
    monkeypatch.setattr(scope, "probe_oracle_utc_pin", lambda cfg: calls.append(cfg) or (True, "read back +00:00"))
    with oracle_destination_scope("postgresql", {"host": "x"}):
        assert not oracle_pin_bound()
    with oracle_destination_scope("oracle", None):
        assert oracle_utc_pin()[0] is False
    with oracle_destination_scope("oracle", {"host": "x"}):
        assert oracle_utc_pin() == (True, "read back +00:00")
        with oracle_destination_scope("oracle", {"host": "y"}):
            assert oracle_utc_pin() == (True, "read back +00:00")
    assert len(calls) == 1 and not oracle_pin_bound()


def test_probe_reads_the_zone_on_the_destination_engine(monkeypatch):
    import connectors.generic_sql as gs
    from services.oracle_session_scope import probe_oracle_utc_pin

    class _Ctx:
        def __init__(self, conn):
            self.connection = conn

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    class _Engine:
        def __init__(self, zone):
            self.zone = zone

        def connect(self):
            return _Ctx(_Conn(zone=self.zone))

    monkeypatch.setattr(gs, "_engine", lambda cfg: _Engine("+00:00"))
    assert probe_oracle_utc_pin({"type": "oracle"})[0] is True
    monkeypatch.setattr(gs, "_engine", lambda cfg: _Engine("Asia/Kolkata"))
    assert probe_oracle_utc_pin({"type": "oracle"}) == (False, "session time zone read back as Asia/Kolkata, not UTC")

    def _down(cfg):
        raise OSError("refused")

    monkeypatch.setattr(gs, "_engine", _down)
    assert "could not be opened" in probe_oracle_utc_pin({"type": "oracle"})[1]


def test_execute_binds_the_pin_for_an_oracle_destination(monkeypatch):
    import services.oracle_session_scope as scope
    import src.transfer.adapters as adapters
    from src.transfer.engine import _oracle_session_scope
    from src.transfer.models import EndpointConfig, TransferRequest

    monkeypatch.setattr(adapters, "resolve_connector_config", lambda ep, workspace_id=None: {"host": ep.host})
    monkeypatch.setattr(scope, "probe_oracle_utc_pin", lambda cfg: (True, f"read back +00:00 on {cfg['host']}"))
    req = TransferRequest(source=EndpointConfig(kind="file", format="csv"),
                          destination=EndpointConfig(kind="database", format="oracle", host="ora", table="T"))
    with _oracle_session_scope(req):
        assert oracle_utc_pin() == (True, "read back +00:00 on ora")
    req_pg = TransferRequest(source=EndpointConfig(kind="file", format="csv"),
                             destination=EndpointConfig(kind="database", format="postgresql", host="pg", table="t"))
    with _oracle_session_scope(req_pg):
        assert not oracle_pin_bound()


def test_validate_outside_a_run_probes_the_destination(monkeypatch):
    import services.oracle_session_scope as scope
    from services.preflight_service import _with_oracle_session_pin

    monkeypatch.setattr(scope, "probe_oracle_utc_pin", lambda cfg: (False, "session time zone read back as -03:30, not UTC"))
    seen = _with_oracle_session_pin(lambda **kw: oracle_utc_pin())
    assert seen(destination_db_type="oracle", destination_config={"host": "o"}, destination_connected=True) == (
        False, "session time zone read back as -03:30, not UTC")
    assert seen(destination_db_type="oracle", destination_config={"host": "o"})[1].endswith("not checked")
