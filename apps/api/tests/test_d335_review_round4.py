"""D335 review round 4: warehouses typed, hold-backs bounded, the acknowledgement audited, no split keys.

* BigQuery (fake client - emulated): the keyless boundary read and the
  post-write duplicate check compare a TIMESTAMP parameter, not
  ``CAST(ts AS STRING)`` (``…+00``) against ISO spellings, and apply the
  BigQuery writer's truncation to the cursor;
* Snowflake (fakesnow - DuckDB-backed emulator): an incremental append through
  the real engine lands each row once;
* three consecutive hold-backs on a route escalate to the structural remedy
  in every mode;
* the acknowledgement is recorded only through the API by a user holding
  job.manage, with the session's actor and an audit event; a value typed into
  the destination's options is ignored;
* copies of one key split across a 5,000-row page are all counted;
* round 7: NUMERIC/TIMESTAMP keys are compared typed on BigQuery (and cast to
  the column type on Snowflake), the acknowledgement unblocks a route the
  third hold-back escalated, and it is keyed by connection + schema + table
  and withdrawn through an audited DELETE (job.manage).
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.boundary_ack_store as acks
    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    monkeypatch.setattr(acks, "STORE_PATH", tmp_path / "acks.json")
    monkeypatch.setattr(acks, "_db", lambda: None)
    return fake


def _run(fake, request):
    job_id = "d335u" + uuid.uuid4().hex[:12]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _pg(sql, params=None, fetch=False):
    from tests import r4_route_harness as h

    conn = h.pg_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        return cur.fetchall() if fetch else None
    finally:
        conn.close()


def _req(src: Path, dest: EndpointConfig, keys=(), mode="strict") -> TransferRequest:
    contract = {"name": "t", "cursor_field": "updated_at", "cursor_semantics": "modification_timestamp",
                "sync_mode": "incremental_append", "selected": True}
    if keys:
        contract["primary_key"] = list(keys)
    return TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=dest, sync_mode="incremental_append", stream_contracts=[contract],
        validation_mode=mode,
    )


# --- 1. BigQuery: typed parameters (fake client) -----------------------------------


def _bq_text(value: datetime) -> str:
    """BigQuery's CAST(TIMESTAMP AS STRING): space, trimmed fraction, ``+00``."""
    frac = f"{value.microsecond:06d}".rstrip("0")
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S") + (f".{frac}" if frac else "") + "+00"


class _FakeBigQuery:
    """Rows in memory; answers the two query shapes the product sends."""

    def __init__(self, rows):
        self.rows = rows
        self.sql: list[str] = []

    def get_table(self, _table_id):
        fields = [SimpleNamespace(name="id", field_type="INT64"),
                  SimpleNamespace(name="note", field_type="STRING"),
                  SimpleNamespace(name="amount", field_type="NUMERIC"),
                  SimpleNamespace(name="seen_at", field_type="TIMESTAMP"),
                  SimpleNamespace(name="updated_at", field_type="TIMESTAMP")]
        return SimpleNamespace(schema=fields)

    @staticmethod
    def _as_string(value):
        """BigQuery's CAST(x AS STRING): NUMERIC normalised, TIMESTAMP ``…+00``."""
        if isinstance(value, datetime):
            return _bq_text(value)
        if isinstance(value, Decimal):
            return format(value.normalize(), "f")
        return str(value)

    def _key_match(self, sql, named, row):
        import re

        terms: dict[str, list[bool]] = {}
        for cast_col, col, n, m in re.findall(r"(?:CAST\(`(\w+)` AS STRING\)|`(\w+)`) = @k(\d+)_(\d+)", sql):
            value = named[f"k{n}_{m}"].value
            stored = row[cast_col or col]
            terms.setdefault(n, []).append(
                self._as_string(stored) == value if cast_col else stored == value)
        return None if not terms else any(all(t) for t in terms.values())

    def query(self, sql, job_config=None):
        self.sql.append(sql)
        params = list(getattr(job_config, "query_parameters", None) or [])
        named = {p.name: p for p in params if getattr(p, "name", None)}
        out = []
        for r in self.rows:
            if "BETWEEN @lo AND @hi" in sql:
                ok = named["lo"].value <= r["updated_at"] <= named["hi"].value
            elif "CAST(`updated_at` AS STRING) IN" in sql:
                texts = {p.value for p in params if p.type_ == "STRING"}
                ok = _bq_text(r["updated_at"]) in texts
            else:
                ok = True
            keyed = self._key_match(sql, named, r)
            if ok and keyed is not None:
                ok = keyed
            if ok:
                out.append(SimpleNamespace(items=lambda r=r: list(r.items())))
        return SimpleNamespace(result=lambda timeout=None: iter(out))


_BQ_WM = "2026-11-24T10:10:00.123456+00:00"


def _bq_rows(copies: int):
    ts = datetime(2026, 11, 24, 10, 10, 0, 123456, tzinfo=timezone.utc)
    return [{"id": 30, "note": "n", "amount": Decimal("30.1"), "seen_at": datetime(2026, 11, 24, 9, tzinfo=timezone.utc),
             "updated_at": ts} for _ in range(copies)]


def test_bigquery_keyless_boundary_read_finds_the_stored_row(monkeypatch):
    import connectors.bigquery_conn as bqc
    from src.transfer.stream_boundary_keyed import read_boundary_candidates

    client = _FakeBigQuery(_bq_rows(1))
    monkeypatch.setattr(bqc, "get_client", lambda **_k: client)
    found = read_boundary_candidates(
        dialect="bigquery", dest_cfg={"database": "proj", "schema": "ds"}, table="t",
        cursor_col="updated_at", watermark=_BQ_WM, key_cols=[], keys=None,
        columns=["id", "note", "updated_at"], ddl="TIMESTAMP",
    )
    assert [r["id"] for r in found] == [30], client.sql


def test_bigquery_post_write_check_counts_a_doubled_watermark_row(monkeypatch):
    """Keyless: the stored copies of the watermark row are counted, typed."""
    import connectors.bigquery_conn as bqc
    from src.transfer.stream_boundary_keyed import BOUNDARY_TIE_KEY, _fp, boundary_duplicates

    client = _FakeBigQuery(_bq_rows(2))
    monkeypatch.setattr(bqc, "get_client", lambda **_k: client)
    types = {"updated_at": "TIMESTAMP", "id": "INT64", "note": "STRING"}
    tie = {"mode": "keyless", "cursor_column": "updated_at", "watermark": _BQ_WM, "types": types,
           "match_columns": ["id", "note"], "row_counts": {_fp((30, "n"), ["id", "note"], "bigquery", types): 1}}
    dups = boundary_duplicates(
        dest_summary={BOUNDARY_TIE_KEY: tie, "table": "t"},
        endpoint=EndpointConfig(kind="database", format="bigquery", database="proj", schema="ds", table="t"),
    )
    assert dups and dups[0]["copies"] == 2, (dups, client.sql)


def test_bigquery_seventh_digit_is_cut_like_the_writer(monkeypatch):
    """The writer cuts ``.1234569`` to ``.123456``; the boundary matches that row."""
    import connectors.bigquery_conn as bqc
    from src.transfer.stream_boundary_keyed import read_boundary_candidates

    client = _FakeBigQuery(_bq_rows(1))
    monkeypatch.setattr(bqc, "get_client", lambda **_k: client)
    found = read_boundary_candidates(
        dialect="bigquery", dest_cfg={"database": "proj", "schema": "ds"}, table="t",
        cursor_col="updated_at", watermark="2026-11-24T10:10:00.1234569+00:00", key_cols=[], keys=None,
        columns=["id", "updated_at"], ddl="TIMESTAMP",
    )
    assert len(found) == 1, client.sql


@pytest.mark.parametrize("key", [{"amount": "30.10"}, {"seen_at": "2026-11-24T09:00:00+00:00"},
                                 {"id": "30", "amount": "30.100", "seen_at": "2026-11-24 09:00:00.000000Z"}])
def test_bigquery_numeric_and_timestamp_keys_compare_typed(monkeypatch, key):
    """A NUMERIC key "30.10" is the stored 30.1; a TIMESTAMP key in ISO is the stored instant."""
    import connectors.bigquery_conn as bqc
    from src.transfer.stream_boundary_keyed import read_boundary_candidates

    client = _FakeBigQuery(_bq_rows(1))
    monkeypatch.setattr(bqc, "get_client", lambda **_k: client)
    found = read_boundary_candidates(
        dialect="bigquery", dest_cfg={"database": "proj", "schema": "ds"}, table="t",
        cursor_col="updated_at", watermark=_BQ_WM, key_cols=list(key), keys=[key],
        columns=["id", "amount", "seen_at", "updated_at"], ddl="TIMESTAMP",
    )
    assert [r["id"] for r in found] == [30], client.sql
    assert "AS STRING) = @k" not in client.sql[-1], client.sql[-1]


# --- 1b. Snowflake through the engine (fakesnow) ----------------------------------


@pytest.mark.parametrize("keys", [(), ("id",)])
def test_snowflake_keyless_append_lands_each_row_once(tmp_path, isolated, monkeypatch, keys):
    pytest.importorskip("fakesnow")
    import src.transfer.stream_boundary_warehouse as wh
    from tests import typed_fidelity_helpers as tf

    monkeypatch.setenv("FAKESNOW_DB_PATH", str(tmp_path / "fakesnow"))
    typed_sql: list[str] = []
    real_rows = wh.snowflake_rows

    def _spy(**kw):
        typed_sql.append(str(kw.get("ddl")))
        yield from real_rows(**kw)

    monkeypatch.setattr(wh, "snowflake_rows", _spy)
    src = tmp_path / "s.sqlite"
    table = f"d335sf_{uuid.uuid4().hex[:6]}"
    with sqlite3.connect(src) as c:
        c.execute("CREATE TABLE t (id INTEGER, note TEXT, updated_at TEXT)")
        c.executemany("INSERT INTO t VALUES (?,?,?)",
                      [(i, f"n{i}", f"2026-11-24 10:{i:02d}:00.{i * 1111:06d}") for i in range(1, 11)])
    first = _run(isolated, _req(src, tf.snowflake_endpoint(table), keys=keys, mode="balanced"))
    assert first.success, first.error
    with sqlite3.connect(src) as c:
        c.execute("INSERT INTO t VALUES (11, 'n11', '2026-11-24 10:10:00.011110')")  # tie peer
        c.execute("INSERT INTO t VALUES (12, 'n12', '2026-11-24 10:12:00.013332')")
    second = _run(isolated, _req(src, tf.snowflake_endpoint(table), keys=keys, mode="balanced"))
    assert second.success, second.error
    assert typed_sql, "the boundary was not read through the typed Snowflake path"
    import snowflake.connector

    conn = snowflake.connector.connect(user="test", password="test", account="localhost",
                                       database="dataflow", schema="public")
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT id, count(*) FROM {table} GROUP BY id HAVING count(*) > 1")
        assert cur.fetchall() == []
        cur.execute(f"SELECT count(*) FROM {table}")
        assert cur.fetchone()[0] == 12
    finally:
        conn.close()


# --- 2. hold-backs do not repeat forever ----------------------------------------


@pytest.mark.parametrize("mode", ["strict", "balanced"])
def test_third_consecutive_hold_back_names_the_remedy(tmp_path, isolated, monkeypatch, mode):
    from tests import typed_fidelity_helpers as tf
    import src.transfer.stream_boundary_keyed as sbk

    src = tmp_path / "s.sqlite"
    dst = f"d335h_{uuid.uuid4().hex[:8]}"
    _pg(f'CREATE TABLE public."{dst}" (id BIGINT, note TEXT, updated_at TIMESTAMP(6))')
    try:
        with sqlite3.connect(src) as c:
            c.execute("CREATE TABLE t (id INTEGER, note TEXT, updated_at TEXT)")
            c.executemany("INSERT INTO t VALUES (?,?,?)", [(i, "n", f"2026-11-24 10:{i:02d}:00") for i in range(1, 11)])
        assert _run(isolated, _req(src, tf.pg_endpoint(dst), mode=mode)).success

        def _timeout(**_k):
            raise RuntimeError("canceling statement due to statement timeout (fake)")

        monkeypatch.setattr(sbk, "read_candidates", _timeout)
        with sqlite3.connect(src) as c:
            c.execute("INSERT INTO t VALUES (11, 'n', '2026-11-24 10:11:00')")
        results = [_run(isolated, _req(src, tf.pg_endpoint(dst), mode=mode)) for _ in range(3)]
        texts = [str(r.error or r.explanation or "") for r in results]
        assert all("held back; the next run will retry them" in t for t in texts[:2]), texts
        assert "3 consecutive runs" in texts[2] and "Add an index on updated_at" in texts[2], texts[2]
        assert "the next run will retry" not in texts[2]
        assert isolated.jobs[results[2].job_id]["status"] == "failed"
        assert _pg(f'SELECT count(*) FROM public."{dst}"', fetch=True)[0][0] == 10
    finally:
        _pg(f'DROP TABLE IF EXISTS public."{dst}"')


def _saved_pg(tmp_path, monkeypatch, dst):
    """A saved PostgreSQL connection (file store) for ``dst``; its endpoint factory."""
    from tests import typed_fidelity_helpers as tf

    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    from services.connector_store import create_connector

    p = tf.pg_endpoint(dst)
    saved = create_connector({
        "name": f"pg-{dst}", "type": "postgresql", "host": p.host, "port": p.port,
        "database": p.database, "username": p.username, "password": p.password, "schema": "public",
    })

    def _dest() -> EndpointConfig:
        d = tf.pg_endpoint(dst)
        d.connector_id = saved.id
        return d

    return saved, _dest


def test_the_acknowledgement_unblocks_a_route_escalated_after_three_hold_backs(tmp_path, isolated, monkeypatch):
    """3 timeouts (the third refused, offering the acknowledgement) -> recorded -> the next run proceeds."""
    import inspect

    import src.transfer.stream_boundary_keyed as sbk
    from services import boundary_ack_store

    src = tmp_path / "s.sqlite"
    dst = f"d335e_{uuid.uuid4().hex[:8]}"
    _pg(f'CREATE TABLE public."{dst}" (id BIGINT, note TEXT, updated_at TIMESTAMP(6))')
    try:
        saved, dest = _saved_pg(tmp_path, monkeypatch, dst)
        with sqlite3.connect(src) as c:
            c.execute("CREATE TABLE t (id INTEGER, note TEXT, updated_at TEXT)")
            c.executemany("INSERT INTO t VALUES (?,?,?)", [(i, "n", f"2026-11-24 10:{i:02d}:00") for i in range(1, 11)])
        assert _run(isolated, _req(src, dest())).success
        real = sbk.read_candidates

        def _write_pass_times_out(**kw):
            # The write pass's watermark lookup times out on every run (an
            # unindexed cursor); the post-write count answers.
            if any(f.function == "append_boundary_rows_landed" for f in inspect.stack()):
                raise RuntimeError("canceling statement due to statement timeout (fake)")
            return real(**kw)

        monkeypatch.setattr(sbk, "read_candidates", _write_pass_times_out)
        with sqlite3.connect(src) as c:
            c.execute("INSERT INTO t VALUES (11, 'n', '2026-11-24 10:11:00')")
        three = [_run(isolated, _req(src, dest())) for _ in range(3)]
        refusal = str(three[2].error or "")
        assert "3 consecutive runs" in refusal and "accept boundary duplicates" in refusal, refusal
        assert _pg(f'SELECT count(*) FROM public."{dst}"', fetch=True)[0][0] == 10

        boundary_ack_store.record(saved.id, "public", dst, actor="ops-lead", reason="append log; dupes ok",
                                  workspace_id="ws")
        fourth = _run(isolated, _req(src, dest()))
        assert fourth.success, fourth.error
        job = isolated.jobs[fourth.job_id]
        assert job["status"] == "completed_with_warnings", job["status"]
        msg = str((fourth.reconciliation or {}).get("message") or "")
        assert "Boundary duplicates accepted by ops-lead: 1 this run" in msg, msg
        assert (fourth.reconciliation or {}).get("verification_verdict") == "explained"
        # The watermark row again (accepted) and row 11: 12 rows, one duplicate.
        assert _pg(f'SELECT count(*) FROM public."{dst}"', fetch=True)[0][0] == 12
        assert _pg(f'SELECT id, count(*) FROM public."{dst}" GROUP BY id HAVING count(*) > 1',
                   fetch=True) == [(10, 2)]
    finally:
        _pg(f'DROP TABLE IF EXISTS public."{dst}"')


# --- 3. the acknowledgement is an audited API decision ------------------------------


def test_options_typed_into_the_destination_are_not_an_acknowledgement(tmp_path, isolated, monkeypatch):
    from tests import typed_fidelity_helpers as tf
    import src.transfer.stream_boundary_keyed as sbk
    from services import boundary_ack_store
    from src.transfer.stream_boundary_read import BoundaryStructural

    src = tmp_path / "s.sqlite"
    dst = f"d335a_{uuid.uuid4().hex[:8]}"
    _pg(f'CREATE TABLE public."{dst}" (id BIGINT, note TEXT, updated_at TIMESTAMP(6))')
    try:
        with sqlite3.connect(src) as c:
            c.execute("CREATE TABLE t (id INTEGER, note TEXT, updated_at TEXT)")
            c.executemany("INSERT INTO t VALUES (?,?,?)", [(i, "n", f"2026-11-24 10:{i:02d}:00") for i in range(1, 11)])
        monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
        monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
        from services.connector_store import create_connector

        p = tf.pg_endpoint(dst)
        saved = create_connector({
            "name": f"pg-{dst}", "type": "postgresql", "host": p.host, "port": p.port,
            "database": p.database, "username": p.username, "password": p.password, "schema": "public",
        })

        def _dest(**extra) -> EndpointConfig:
            d = tf.pg_endpoint(dst)
            d.connector_id = saved.id
            if extra:
                d.extra = extra
            return d

        assert _run(isolated, _req(src, _dest())).success

        def _never(**_k):
            raise BoundaryStructural("the cursor column's type cannot be compared with the watermark")

        monkeypatch.setattr(sbk, "read_candidates", _never)
        with sqlite3.connect(src) as c:
            c.execute("INSERT INTO t VALUES (11, 'n', '2026-11-24 10:11:00')")
        typed = _dest(accept_boundary_duplicates={"actor": "anyone", "reason": "typed, not recorded"})
        refused = _run(isolated, _req(src, typed))
        assert not refused.success and "Declare a primary key" in str(refused.error), refused.error

        boundary_ack_store.record(saved.id, "public", dst, actor="ops-lead", reason="append log; dupes ok",
                                  workspace_id="ws")
        accepted = _run(isolated, _req(src, _dest()))
        assert accepted.success, accepted.error
        recon = accepted.reconciliation or {}
        msg = str(recon.get("message") or "")
        # Never green: explained, completed with warnings, the count named, every run.
        assert "Boundary duplicates accepted by ops-lead: 1 this run" in msg, msg
        assert "Append delta verified" not in msg, msg
        assert recon.get("verification_verdict") == "explained", recon.get("verification_verdict")
        job = isolated.jobs[accepted.job_id]
        assert job["status"] == "completed_with_warnings", job["status"]
        import json

        from services.job_trust import compute_job_trust

        trust = json.dumps(compute_job_trust(job))
        assert "boundary duplicates accepted by ops-lead: 1 this run" in trust, trust[:2000]
    finally:
        _pg(f'DROP TABLE IF EXISTS public."{dst}"')


def test_the_acknowledgement_route_needs_job_manage_and_records_the_session_actor(world, monkeypatch, tmp_path):
    from services import audit_log, boundary_ack_store
    from tests.test_tenancy_route_scoping import _seed_saved_connector

    monkeypatch.setattr(boundary_ack_store, "STORE_PATH", tmp_path / "acks.json")
    monkeypatch.setattr(boundary_ack_store, "_db", lambda: None)
    events = []
    real = audit_log.append_audit_event
    monkeypatch.setattr(audit_log, "append_audit_event", lambda **kw: events.append(kw) or real(**kw))
    connector_id = _seed_saved_connector(world).params["connector_id"]
    path = f"/api/v1/connectors/saved/{connector_id}/boundary-duplicates-acknowledgement"
    body = {"table": "orders", "reason": "append-only event log; duplicates are tolerated"}
    viewer = world.member("viewer-ack@tenancy.example.com", world.workspace_a, "viewer")
    assert viewer.post(path, json=body).status_code == 403
    ok = world.admin_a.post(path, json={**body, "actor": "someone-else"})
    assert ok.status_code == 200, ok.text
    recorded = boundary_ack_store.lookup(connector_id, "public", "orders")
    assert recorded["actor"] == "admin-a@tenancy.example.com", recorded
    assert any(e.get("action") == "route.boundary_duplicates_accepted" and e.get("actor") == recorded["actor"]
               for e in events), events


def test_the_acknowledgement_is_keyed_by_schema_and_withdrawn_by_job_manage(world, monkeypatch, tmp_path):
    from services import audit_log, boundary_ack_store
    from tests.test_tenancy_route_scoping import _seed_saved_connector

    monkeypatch.setattr(boundary_ack_store, "STORE_PATH", tmp_path / "acks.json")
    monkeypatch.setattr(boundary_ack_store, "_db", lambda: None)
    events = []
    real = audit_log.append_audit_event
    monkeypatch.setattr(audit_log, "append_audit_event", lambda **kw: events.append(kw) or real(**kw))
    connector_id = _seed_saved_connector(world).params["connector_id"]
    path = f"/api/v1/connectors/saved/{connector_id}/boundary-duplicates-acknowledgement"
    body = {"table": "orders", "schema": "sales", "reason": "append-only event log; duplicates are tolerated"}
    assert world.admin_a.post(path, json=body).status_code == 200
    assert boundary_ack_store.lookup(connector_id, "sales", "orders")
    assert boundary_ack_store.lookup(connector_id, "public", "orders") is None  # another schema's table
    viewer = world.member("viewer-ack2@tenancy.example.com", world.workspace_a, "viewer")
    assert viewer.delete(path, params={"table": "orders", "schema": "sales"}).status_code == 403
    assert boundary_ack_store.lookup(connector_id, "sales", "orders")
    gone = world.admin_a.delete(path, params={"table": "orders", "schema": "sales"})
    assert gone.status_code == 200 and gone.json()["removed"] is True, gone.text
    assert boundary_ack_store.lookup(connector_id, "sales", "orders") is None
    assert any(e.get("action") == "route.boundary_duplicates_acceptance_withdrawn"
               and e.get("actor") == "admin-a@tenancy.example.com" for e in events), events


# --- 4. a key's copies split across a page are all counted ---------------------------


def test_copies_straddling_a_page_boundary_are_all_counted(monkeypatch):
    import src.transfer.stream_boundary_read as sbr
    from src.transfer.stream_boundary_keyed import BOUNDARY_TIE_KEY, boundary_duplicates
    from tests import typed_fidelity_helpers as tf

    monkeypatch.setattr(sbr, "_PAGE_ROWS", 2)
    dst = f"d335p_{uuid.uuid4().hex[:8]}"
    _pg(f'CREATE TABLE public."{dst}" (id BIGINT, k2 BIGINT, updated_at TIMESTAMP(6))')
    try:
        # Key (1, 7) three times at the watermark: copies land on both sides of a page.
        _pg(f'INSERT INTO public."{dst}" VALUES (1, 6, %s), (1, 7, %s), (1, 7, %s), (1, 7, %s), (1, 8, %s)',
            tuple(["2026-11-24 10:10:00.5"] * 5))
        tie = {"mode": "keyed", "cursor_column": "updated_at", "watermark": "2026-11-24 10:10:00.5",
               "types": {}, "key_columns": ["id", "k2"],
               "keys": [{"id": "1", "k2": "6"}, {"id": "1", "k2": "7"}, {"id": "1", "k2": "8"}], "pre_copies": {}}
        dups = boundary_duplicates(dest_summary={BOUNDARY_TIE_KEY: tie, "table": dst}, endpoint=tf.pg_endpoint(dst))
        assert [(d["key"], d["copies"]) for d in dups] == [({"id": "1", "k2": "7"}, 3)], dups
    finally:
        _pg(f'DROP TABLE IF EXISTS public."{dst}"')


from tests.test_tenancy_route_scoping import tenancy_world, world  # noqa: E402,F401  (fixtures)
