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
    PRIOR_KEY,
    install_utc_session_pin,
    oracle_instant_bind,
    pin_utc_session,
    session_zone_is_utc,
)
from services.oracle_session_scope import (
    assert_writer_session,
    bind_oracle_utc_pin,
    mark_server_side_oracle_copy,
    note_oracle_rows_written,
    oracle_destination_scope,
    oracle_pin_bound,
    oracle_same_instance,
    oracle_utc_pin,
    server_side_copy_scope,
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


def test_failed_pin_is_recorded_on_the_pool_record_with_the_opening_zone():
    # SQLite refuses ALTER SESSION: the pool behaves as an Oracle pool whose pin failed.
    engine = sa.create_engine("sqlite://")
    install_utc_session_pin(engine)
    with engine.connect() as conn:  # no checkout refusal: source checkouts are never blocked
        pinned, reason = conn.info[INFO_KEY]
        assert pinned is False and "refused" in reason and conn.info[PRIOR_KEY] == ""
    engine.dispose()


class _SAConn:
    """A SQLAlchemy-like connection around the fake DB-API one."""

    def __init__(self, zone):
        self.connection = _Conn(zone=zone)


def test_writer_refuses_a_batch_on_a_non_utc_session_and_counts_what_was_written():
    with bind_oracle_utc_pin(True, "session time zone read back as +00:00"):
        assert_writer_session(_SAConn("+00:00"))  # fine
        with pytest.raises(RuntimeError, match="No batch of this run had been written"):
            assert_writer_session(_SAConn("Asia/Kolkata"))
        note_oracle_rows_written(1500)
        with pytest.raises(RuntimeError, match="had committed 1500 row"):
            assert_writer_session(_SAConn("Asia/Kolkata"))
    with bind_oracle_utc_pin(False, "not checked"):  # graded with a Risk Contract: writes as accepted
        assert_writer_session(_SAConn("Asia/Kolkata"))


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


def test_server_side_copy_counts_only_once_committed():
    src = tgt = "TIMESTAMP(6) WITH TIME ZONE"
    a = {"host": "db1", "port": 1521, "service_name": "FREEPDB1"}
    with bind_source_engine("oracle"), bind_oracle_utc_pin(False, "not checked"), server_side_copy_scope():
        # Same configs, but nothing committed yet (the copy may still decline): gated.
        assert is_lossy_coercion(src, tgt, dest_db="oracle")
        assert mark_server_side_oracle_copy(a, dict(a)) is True
        assert oracle_same_instance() and not is_lossy_coercion(src, tgt, dest_db="oracle")
    assert not oracle_same_instance()


@pytest.mark.parametrize("src_cfg,dest_cfg", [
    ({"host": "db1", "port": 1521, "sid": "PROD"}, {"host": "db1", "port": 1521, "sid": "TEST"}),  # SID only
    ({"host": "db1", "port": 1521, "service_name": "A"}, {"host": "db1", "port": 1521, "service_name": "B"}),
    ({"host": "db1", "port": 1521, "service_name": "A"}, {"host": "db2", "port": 1521, "service_name": "A"}),
])
def test_not_one_instance_never_marks_a_server_side_copy(src_cfg, dest_cfg):
    with server_side_copy_scope():
        assert mark_server_side_oracle_copy(src_cfg, dest_cfg) is False
        assert not oracle_same_instance()


@pytest.mark.parametrize("switch,ddls", [
    ("0", ["TIMESTAMP(6) WITH TIME ZONE"]),        # INSERT ... SELECT switched off
    ("1", []),                                     # column list / DDL mismatch
])
def test_a_declined_server_side_copy_leaves_the_route_gated(monkeypatch, switch, ddls):
    """Same instance, but the INSERT ... SELECT declines to the row writer: no exemption.

    (A value-changing transform never reaches the copy: the route declines it
    before, so nothing is marked either.)
    """
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_oracle_oracle import copy_oracle_to_oracle

    same = {"host": "db1", "port": 1521, "service_name": "FREEPDB1", "username": "u", "password": "p"}
    monkeypatch.setenv("DATAFLOW_ORACLE_ORACLE_INSERT_SELECT", switch)
    with bind_source_engine("oracle"), bind_oracle_utc_pin(False, "not checked"), server_side_copy_scope():
        with pytest.raises(FastPathUnavailable):
            copy_oracle_to_oracle(source_cfg=same, source_table="S", dest_cfg=dict(same), dest_table="D",
                                  pairs=[("AT", "AT")], oracle_ddls=ddls, replace_destination=True)
        assert not oracle_same_instance()
        assert is_lossy_coercion("TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP(6) WITH TIME ZONE", dest_db="oracle")


def test_row_path_says_the_offset_label_is_not_stored():
    from services.mapping_proof import _mapping_risks
    from services.offset_label import stores_originating_offset
    from services.timezone_policy import resolve_timezone_policy

    mapping = {"source": "at", "target": "at", "source_type": "DATETIMEOFFSET(6)",
               "target_type": "TIMESTAMP(6) WITH TIME ZONE"}
    with bind_oracle_utc_pin(True, "session time zone read back as +00:00"), server_side_copy_scope():
        policy = resolve_timezone_policy("DATETIMEOFFSET(6)", "TIMESTAMP(6) WITH TIME ZONE", dest_db="oracle")
        assert policy.offset_label_preserved is False
        risks = _mapping_risks(mapping, dest_mode="existing", destination_db_type="oracle")
        assert any("offset label not stored" in r["message"] for r in risks), risks
        assert stores_originating_offset("oracle", "TIMESTAMP WITH TIME ZONE")  # as a source it holds them
        assert not stores_originating_offset("oracle", "TIMESTAMP WITH TIME ZONE", as_destination=True)
        mark_server_side_oracle_copy({"host": "h", "service_name": "S"}, {"host": "h", "service_name": "S"})
        assert resolve_timezone_policy(
            "DATETIMEOFFSET(6)", "TIMESTAMP(6) WITH TIME ZONE", dest_db="oracle").offset_label_preserved


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


def _fake_sessions(monkeypatch, *, confirmed=True, prior="+00:00"):
    import services.oracle_session_scope as scope
    import src.transfer.adapters as adapters

    monkeypatch.setattr(adapters, "resolve_connector_config", lambda ep, workspace_id=None: {"host": ep.host})
    monkeypatch.setattr(scope, "probe_oracle_session", lambda cfg: (
        confirmed, f"session time zone read back as {'+00:00' if confirmed else prior} on {cfg['host']}", prior))


def _req(src_fmt="csv", dest_fmt="oracle", **kw):
    from src.transfer.models import EndpointConfig, TransferRequest

    src_kind = "file" if src_fmt == "csv" else "database"
    return TransferRequest(source=EndpointConfig(kind=src_kind, format=src_fmt, host="src", table="S",
                                                 extra=kw.pop("src_extra", {})),
                           destination=EndpointConfig(kind="database", format=dest_fmt, host="ora", table="T"), **kw)


def test_execute_binds_the_pin_for_an_oracle_destination(monkeypatch):
    from services.oracle_session_scope import oracle_run_scope

    _fake_sessions(monkeypatch)
    with oracle_run_scope(_req()):
        assert oracle_utc_pin() == (True, "session time zone read back as +00:00 on ora")
    with oracle_run_scope(_req(dest_fmt="postgresql")):
        assert not oracle_pin_bound()


@pytest.mark.parametrize("kw,why", [
    ({"sync_mode": "cdc"}, "a CDC route"),
    ({"sync_mode": "mirror"}, "a mirror route"),
    ({"src_extra": {"source_query": "SELECT * FROM T WHERE AT > CURRENT_DATE"}}, "a custom source query"),
    ({"source_filter": {"column": "AT", "op": ">", "value": "2024-01-01"}}, "a source filter"),
])
def test_non_utc_default_needs_one_acknowledgement_per_route(monkeypatch, tmp_path, kw, why):
    import services.oracle_session_scope as scope
    from services.oracle_session_scope import oracle_run_scope

    monkeypatch.setattr(scope, "_ack_path", lambda: tmp_path / "acks.jsonl")
    _fake_sessions(monkeypatch, prior="America/St_Johns")
    req = _req(src_fmt="oracle", **kw)
    with pytest.raises(ValueError, match=f"defaulted to America/St_Johns.*CURRENT_DATE.*{why}"):
        with oracle_run_scope(req):
            pass
    # An actor/reason given for another acknowledgement never stands in for this one.
    req.session_zone_acknowledged = True
    req.acknowledgment_actor, req.acknowledgment_reason = "ops", "compliance reviewed for this table"
    with pytest.raises(ValueError, match="session_zone_ack_actor"):
        with oracle_run_scope(req):
            pass
    req.session_zone_ack_actor, req.session_zone_ack_reason = "ops", "UTC is intended for this route"
    with oracle_run_scope(req):
        pass
    req.session_zone_acknowledged = False  # one-time: the route stays acknowledged
    with oracle_run_scope(req):
        pass


def test_utc_default_or_plain_copy_needs_no_acknowledgement(monkeypatch, tmp_path):
    import services.oracle_session_scope as scope
    from services.oracle_session_scope import oracle_run_scope

    monkeypatch.setattr(scope, "_ack_path", lambda: tmp_path / "acks.jsonl")
    _fake_sessions(monkeypatch, prior="+00:00")
    with oracle_run_scope(_req(src_fmt="oracle", sync_mode="cdc")):
        pass
    _fake_sessions(monkeypatch, prior="Asia/Kolkata")
    with oracle_run_scope(_req(src_fmt="oracle")):  # full refresh, no custom SQL
        pass


def test_validate_outside_a_run_probes_and_warns(monkeypatch):
    import services.oracle_session_scope as scope
    from services.oracle_session_scope import with_oracle_session_pin

    monkeypatch.setattr(scope, "probe_oracle_session", lambda cfg: (False, "session time zone read back as -03:30, not UTC", "-03:30"))
    monkeypatch.setattr(scope, "_jobs_in_window", lambda store=None: [])
    seen = with_oracle_session_pin(lambda **kw: {"pin": oracle_utc_pin(), "warnings": []})
    out = seen(destination_db_type="oracle", destination_config={"host": "o"}, destination_connected=True,
               sync_mode="cdc", destination_table="T")
    assert out["pin"] == (False, "session time zone read back as -03:30, not UTC")
    assert any("defaulted to -03:30" in w for w in out["warnings"])
    assert seen(destination_db_type="oracle", destination_config={"host": "o"})["pin"][1].endswith("not checked")


def _job(stamp=None, mappings=None, table="T", status="completed", connector="ora-prod", schema="APP"):
    job = {"status": status, "transfer_request": {
        "destination": {"format": "oracle", "table": table, "connector_id": connector, "schema": schema},
        "mappings": mappings if mappings is not None else [
            {"source": "at", "target": "AT", "source_type": "TIMESTAMPTZ(6)",
             "target_type": "TIMESTAMP(6) WITH TIME ZONE"}],
    }}
    if stamp is not None:
        job["oracle_session"] = stamp
    return job


TARGET = {"connector_id": "ora-prod", "schema": "APP", "table": "T"}


def test_upgrade_advisory_names_unpinned_instant_writes_only():
    from services.oracle_session_scope import UPGRADE_ADVISORY, upgrade_advisory

    def adv(*jobs, target=TARGET):
        return upgrade_advisory("oracle", target, lambda: list(jobs))

    assert adv(_job()) == UPGRADE_ADVISORY
    assert adv(_job(mappings=[])) == UPGRADE_ADVISORY  # unrecorded mapping
    assert adv(_job(stamp={"utc_pinned": False, "reason": "read back Asia/Kolkata"})) == UPGRADE_ADVISORY
    assert adv(_job(stamp={"utc_pinned": True})) == ""
    assert adv(_job(table="OTHER")) == ""
    assert adv(_job(connector="ora-test")) == ""  # same table name, another database
    assert adv(_job(schema="OTHER")) == ""
    assert adv(_job(status="failed")) == ""
    assert adv(_job(mappings=[
        {"source": "n", "target": "N", "source_type": "INTEGER", "target_type": "NUMBER(10)"}])) == ""
    assert upgrade_advisory("postgresql", TARGET, lambda: [_job()]) == ""


def test_upgrade_advisory_reads_the_whole_window_not_the_last_200(monkeypatch):
    import services.oracle_session_scope as scope

    old = _job()
    old["job_id"] = "old"
    newer = []
    for i in range(450):
        j = _job(stamp={"utc_pinned": True})
        j["job_id"] = f"j{i}"
        newer.append(j)

    class _Store:
        calls: list = []

        def list_jobs(self, limit=50, *, destination_type="", date_from=None, offset=0, **_):
            self.calls.append((offset, date_from))
            return (newer + [old])[offset:offset + limit]

        def get_jobs(self, ids):
            return {j["job_id"]: j for j in newer + [old] if j["job_id"] in ids}

    store = _Store()
    jobs = scope._jobs_in_window(store)
    assert len(jobs) == 451 and scope.unpinned_history(jobs, TARGET)
    assert [c[0] for c in store.calls] == [0, 200, 400] and all(c[1] for c in store.calls)


class _StatusOnlyStore:
    """A job store with no ``update_job_fields``: the stamp goes through the status update."""

    def __init__(self):
        self.jobs: dict = {}

    def update_job_status(self, job_id, status, **kw):
        self.jobs.setdefault(job_id, {}).update(kw)
        self.jobs[job_id]["status"] = status
        return True

    def get_job(self, job_id):
        return self.jobs.get(job_id)


@pytest.mark.parametrize("make", ["fake_mongo", "status_only"])
def test_run_stamps_its_job_whatever_the_store_offers(monkeypatch, make):
    from services.oracle_session_scope import oracle_run_scope
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    _fake_sessions(monkeypatch)
    store = _FakeMongo() if make == "fake_mongo" else _StatusOnlyStore()
    store.update_job_status("j1", "running")
    with oracle_run_scope(_req(), "j1", store):
        pass
    assert store.get_job("j1")["oracle_session"]["utc_pinned"] is True
    assert store.get_job("j1")["status"] == "running"


def test_partial_range_append_declines_after_the_lock_and_leaves_no_mark(monkeypatch):
    """Review: an append into an occupied table whose key range is only partly
    present declines (copy_fast_path) after the share lock and falls back to the
    row writer — the server-side mark must not survive it."""
    import services.copy_oracle_oracle as coo
    from services.copy_fast_path import FastPathUnavailable

    class _Cur:
        def execute(self, sql, *a):
            self.last = sql

        def close(self):
            pass

    class _OraConn:
        def cursor(self):
            return _Cur()

        def commit(self):
            pass

        def close(self):
            pass

    same = {"host": "db1", "port": 1521, "service_name": "FREEPDB1", "username": "u", "password": "p"}
    monkeypatch.setenv("DATAFLOW_ORACLE_ORACLE_INSERT_SELECT", "1")
    monkeypatch.setattr(coo, "_oracle_connect", lambda cfg: _OraConn())
    monkeypatch.setattr(coo, "_ora_table_pk_and_types", lambda cur, schema, table, cols: (
        ["ID"], {"ID": "NUMBER(10)", "AT": "TIMESTAMP(6) WITH TIME ZONE"}))
    monkeypatch.setattr(coo, "_table_exists", lambda cur, schema, table: True)
    monkeypatch.setattr(coo, "_count", lambda cur, ref: 10)
    monkeypatch.setattr(coo, "_plan_pk_partitions", lambda *a: [
        {"lo": 1, "hi": 10, "null_shard": False, "source_count": 10, "predicate": "", "params": []}])
    monkeypatch.setattr(coo, "_range_count", lambda *a: 4)  # partly present
    with server_side_copy_scope():
        with pytest.raises(FastPathUnavailable, match="partly present"):
            coo.copy_oracle_to_oracle(
                source_cfg=same, source_table="S", dest_cfg=dict(same), dest_table="D",
                pairs=[("ID", "ID"), ("AT", "AT")],
                oracle_ddls=["NUMBER(10)", "TIMESTAMP(6) WITH TIME ZONE"], replace_destination=False,
            )
        assert not oracle_same_instance()
        from services.timezone_policy import resolve_timezone_policy

        assert not resolve_timezone_policy(
            "TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP(6) WITH TIME ZONE", dest_db="oracle").offset_label_preserved


def test_a_failed_insert_select_clears_the_mark(monkeypatch):
    import services.copy_oracle_oracle as coo

    class _Cur:
        def execute(self, sql, *a):
            if sql.lstrip().upper().startswith("INSERT"):
                raise RuntimeError("ORA-01653: unable to extend table")

        def close(self):
            pass

    class _OraConn:
        def cursor(self):
            return _Cur()

        def commit(self):
            pass

        def close(self):
            pass

    same = {"host": "db1", "port": 1521, "service_name": "FREEPDB1", "username": "u", "password": "p"}
    monkeypatch.setenv("DATAFLOW_ORACLE_ORACLE_INSERT_SELECT", "1")
    monkeypatch.setattr(coo, "_oracle_connect", lambda cfg: _OraConn())
    monkeypatch.setattr(coo, "_ora_table_pk_and_types", lambda cur, schema, table, cols: ([], {}))
    monkeypatch.setattr(coo, "_table_exists", lambda cur, schema, table: False)
    monkeypatch.setattr(coo, "settle_fast_path_create_on", lambda *a, **k: None)
    monkeypatch.setattr(coo, "_count", lambda cur, ref: 3)
    with server_side_copy_scope():
        with pytest.raises(RuntimeError, match="ORA-01653"):
            coo.copy_oracle_to_oracle(
                source_cfg=same, source_table="S", dest_cfg=dict(same), dest_table="D",
                pairs=[("AT", "AT")], oracle_ddls=["TIMESTAMP(6) WITH TIME ZONE"], replace_destination=False,
            )
        assert not oracle_same_instance()


def _route(**over):
    from src.transfer.models import EndpointConfig, TransferRequest

    src = dict(kind="database", format="oracle", host="db1", port=1521, database="FREEPDB1", table="S")
    dst = dict(kind="database", format="oracle", host="db1", port=1521, database="FREEPDB1", table="D")
    src.update(over.pop("src", {}))
    dst.update(over.pop("dst", {}))
    return TransferRequest(source=EndpointConfig(**src), destination=EndpointConfig(**dst), **over)


@pytest.mark.parametrize("a,b,cfgs_a,cfgs_b", [
    (_route(workspace_id="ws-a"), _route(workspace_id="ws-b"), None, None),
    (_route(dst={"connector_id": "prod"}), _route(dst={"connector_id": "test"}), None, None),
    (_route(dst={"host": "", "database": "", "connection_string": "oracle://u@prod-scan:1521/PROD"}),
     _route(dst={"host": "", "database": "", "connection_string": "oracle://u@test-scan:1521/TEST"}), None, None),
    (_route(dst={"extra": {"sid": "PROD"}, "database": ""}), _route(dst={"extra": {"sid": "TEST"}, "database": ""}),
     None, None),
    (_route(dst={"database": ""}), _route(dst={"database": ""}),
     {"destination": {"sid": "PROD"}}, {"destination": {"sid": "TEST"}}),  # SID only in the connector config
    (_route(dst={"extra": {"service_name": "PDB1"}}), _route(dst={"extra": {"service_name": "PDB2"}}), None, None),
])
def test_route_key_separates_routes_that_only_look_alike(a, b, cfgs_a, cfgs_b):
    from services.oracle_session_scope import route_key

    assert route_key(a, cfgs_a) != route_key(b, cfgs_b)
    assert route_key(a, cfgs_a) == route_key(a, cfgs_a)


def test_zone_acknowledgement_is_part_of_the_staged_fingerprint():
    from services.staged_preflight import request_preflight_fingerprint

    base = _route()
    acked = _route(session_zone_acknowledged=True)
    actor = _route(session_zone_acknowledged=True, session_zone_ack_actor="ops")
    reason = _route(session_zone_acknowledged=True, session_zone_ack_actor="ops",
                    session_zone_ack_reason="UTC is intended for this route")
    prints = {request_preflight_fingerprint(r) for r in (base, acked, actor, reason)}
    assert len(prints) == 4
