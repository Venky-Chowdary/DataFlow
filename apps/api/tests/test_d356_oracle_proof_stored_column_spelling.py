"""D356: on an Oracle destination the transform proof really reads the table.

QA (Timescale/MariaDB -> Oracle, overwrite with trim/default:/upper): the
proof re-read the destination as ``SELECT "id", "name", "note" FROM ...``.
The Oracle writer creates unquoted columns, which Oracle stores upper-case, so
every read was ORA-00904, the error was swallowed and the job stayed green
with the transforms never compared — which is how D350 hid on Oracle.

The read now selects the catalog's stored spelling (``"NOTE" AS "note"``,
resolved through ``resolve_object_identity``), and a SQL oracle that did not
run no longer counts any step as "covered".
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
import src.transfer.reconcile_step as rs  # noqa: E402
from services.column_transform_chain import (  # noqa: E402
    compile_column_chains,
    split_column_transforms,
)
from services.shape_models import ShapeRecipe  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

pytestmark = pytest.mark.timeout(900)

_COLS = ["id", "name", "note"]
_ROWS = [
    (1, "ada", "  x  "),
    (2, "bob", ""),
    (3, "cy", "   "),
    (4, "di", None),
    (5, "eve", "real"),
]
_TRANSFORMS = {"note": ["trim", "default:NA"], "name": ["upper"]}
_DEST_WANT = [
    (1, "ADA", "x"),
    (2, "BOB", "NA"),
    (3, "CY", "NA"),
    (4, "DI", "NA"),
    (5, "EVE", "real"),
]


# --- unit: the generated read-back SQL ---------------------------------------


class _FakeOracleConn:
    """Answers the two catalog queries ``_oracle_identity`` sends."""

    def __init__(self, stored_cols: list[str]) -> None:
        self.stored_cols = stored_cols
        self.sql: list[str] = []

    def execute(self, stmt, params=None):  # noqa: ANN001
        text = str(stmt)
        self.sql.append(text)
        rows = (
            [("APP", "ORDERS")]
            if "all_tables" in text
            else [(c,) for c in self.stored_cols]
        )

        class _R:
            def fetchall(self_inner):  # noqa: N805
                return rows

        return _R()


def test_oracle_projection_selects_the_stored_upper_spelling():
    conn = _FakeOracleConn(["ID", "NAME", "NOTE", "first_name"])
    ep = EndpointConfig(kind="database", format="oracle", schema="app", table="orders")
    sel, ref = rs._oracle_projection(
        conn, ep, {"username": "app"}, ["id", "name", "note", "first name"]
    )
    assert sel == (
        '"ID" AS "id", "NAME" AS "name", "NOTE" AS "note", '
        '"first_name" AS "first name"'
    )
    assert ref("note") == '"NOTE"'
    # Resolved through the catalog, not folded by hand.
    assert any("all_tab_columns" in s for s in conn.sql)


def test_oracle_projection_keeps_an_unknown_column_so_oracle_reports_it():
    conn = _FakeOracleConn(["ID"])
    ep = EndpointConfig(kind="database", format="oracle", schema="app", table="orders")
    sel, _ref = rs._oracle_projection(conn, ep, {}, ["id", "ghost"])
    assert sel == '"ID" AS "id", "ghost"'


# --- the SQL oracle must not claim coverage it did not run --------------------


def _request(source, dest, mode: str = "full_refresh_overwrite") -> TransferRequest:
    _singles, chains = split_column_transforms(_TRANSFORMS)
    recipe, refusal = compile_column_chains(chains, _COLS)
    assert not refusal, refusal
    extra = {}
    if mode != "full_refresh_overwrite":
        extra["stream_contracts"] = [
            {"name": dest.table, "primary_key": ["id"], "sync_mode": mode, "selected": True}
        ]
    return TransferRequest(
        source=source,
        destination=dest,
        mappings=[
            {"source": c, "target": c, "approved": True, "confidence": 0.99} for c in _COLS
        ],
        sync_mode=mode,
        validation_mode="strict",
        shape_recipe=recipe,
        approved_shape_recipe_hash=ShapeRecipe.parse(recipe).recipe_hash,
        skip_preflight=True,
        **extra,
    )


@pytest.fixture()
def fake_jobs(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    class _Store(_FakeMongo):
        def update_job_fields(self, job_id, fields):
            self.jobs.setdefault(job_id, {}).update(fields)
            return True

    fake = _Store()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _run(fake, request: TransferRequest):
    job_id = "d356" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _seed_pg(table: str) -> EndpointConfig:
    from tests import r4_route_harness as h
    from tests import typed_fidelity_helpers as tf

    conn = h.pg_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            f'CREATE TABLE public."{table}" '  # nosec B608
            "(id integer PRIMARY KEY, name varchar(40), note varchar(40))"
        )
        for row in _ROWS:
            cur.execute(f'INSERT INTO public."{table}" VALUES (%s, %s, %s)', row)  # nosec B608
        if not conn.autocommit:
            conn.commit()
    finally:
        conn.close()
    return tf.pg_endpoint(table)


@pytest.mark.parametrize("broken", ["query_fails", "later_step_has_no_sql"])
def test_an_oracle_that_did_not_run_covers_no_step(tmp_path, fake_jobs, monkeypatch, broken):
    """PG -> SQLite: the run is fine, but the independent oracle is made not to
    run. The verdict may only say "recipe re-applied" with every step
    uncovered — the base still reported 3 of 3 (or 2 of 3) steps covered."""
    import sqlite3

    from tests import r4_route_harness as h

    original = rs._recipe_step_sql
    calls = {"n": 0}

    def _broken(op, opts, expr, params, **kw):
        calls["n"] += 1
        if broken == "query_fails":
            return "no_such_function_d356(" + str(expr) + ")"
        return None if calls["n"] >= 2 else original(op, opts, expr, params, **kw)

    monkeypatch.setattr(rs, "_recipe_step_sql", _broken)
    src_t = f"d356_src_{uuid.uuid4().hex[:8]}"
    dst = tmp_path / "dst.sqlite"
    source = _seed_pg(src_t)
    try:
        dest = EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t")
        result = _run(fake_jobs, _request(source, dest))
        assert result.success, result.error
        proof = (result.reconciliation or {}).get("transform_applied") or {}
        assert calls["n"] >= 1, "the oracle path was not exercised"
        assert proof.get("status") == "verified", proof
        assert proof.get("provenance") == "recipe re-applied", proof
        assert proof.get("oracle_coverage", {}).get("covered") == 0, proof
        assert len(proof.get("oracle_uncovered_steps") or []) == 3, proof
        with sqlite3.connect(str(dst)) as c:
            got = c.execute("SELECT id, name, note FROM t ORDER BY id").fetchall()
        assert got == _DEST_WANT
    finally:
        h.drop_table("postgresql", src_t)


# --- live: a real Oracle destination ------------------------------------------


def _oracle_conn():
    from services.brand_env import getenv_brand
    from tests import typed_fidelity_helpers as tf

    tf.require_oracle_env()
    oracledb = pytest.importorskip("oracledb")
    try:
        return oracledb.connect(
            user=getenv_brand("ORACLE_USER", "dataflow"),
            password=getenv_brand("ORACLE_PASSWORD", "dataflow"),
            dsn=f"{getenv_brand('ORACLE_HOST', 'localhost')}:"
            f"{getenv_brand('ORACLE_PORT', '1521')}/"
            f"{getenv_brand('ORACLE_SERVICE') or getenv_brand('ORACLE_DATABASE', 'ORCLPDB1')}",
        )
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Oracle not reachable: {type(exc).__name__}")


def _oracle_owner() -> str:
    from tests import typed_fidelity_helpers as tf

    return str(tf.oracle_endpoint("x").schema or "").upper()


def _oracle_rows(table: str) -> list[tuple]:
    conn = _oracle_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            f'SELECT "ID", "NAME", TO_CHAR("NOTE") FROM "{_oracle_owner()}"."{table.upper()}" '  # nosec B608
            'ORDER BY "ID"'
        )
        return [(int(r[0]), r[1] if isinstance(r[1], str) else r[1].read(), r[2]) for r in cur]
    finally:
        conn.close()


def _oracle_exec(sql: str) -> None:
    conn = _oracle_conn()
    try:
        conn.cursor().execute(sql)
        conn.commit()
    finally:
        conn.close()


def _drop_oracle(table: str) -> None:
    conn = _oracle_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT table_name FROM all_tables WHERE owner = :o AND UPPER(table_name) = :t",
            o=_oracle_owner(),
            t=table.upper(),
        )
        for (stored,) in cur.fetchall():
            cur.execute(f'DROP TABLE "{_oracle_owner()}"."{stored}" PURGE')  # nosec B608
    finally:
        conn.close()


def _seed_mariadb(table: str) -> EndpointConfig:
    from tests.test_d350_pipeline_default_on_empty import _mariadb_conn, _mariadb_ep

    conn = _mariadb_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            f"CREATE TABLE `{table}` (id integer PRIMARY KEY, name varchar(40), note varchar(40))"  # nosec B608
        )
        for row in _ROWS:
            cur.execute(f"INSERT INTO `{table}` VALUES (%s, %s, %s)", row)  # nosec B608
    finally:
        conn.close()
    return _mariadb_ep(table)


def _drop_source(kind: str, table: str) -> None:
    if kind == "mariadb":
        from tests.test_d350_pipeline_default_on_empty import _drop

        _drop("mariadb", table)
        return
    from tests import r4_route_harness as h

    h.drop_table("postgresql", table)


@pytest.mark.parametrize(
    ("src_kind", "mode"),
    [
        ("postgresql", "full_refresh_overwrite"),
        ("mariadb", "full_refresh_overwrite"),
        ("postgresql", "incremental_upsert"),
    ],
)
def test_live_oracle_destination_proof_runs_and_verifies(fake_jobs, src_kind, mode):
    """The proof reads the Oracle table and verifies with the independent SQL
    oracle; on the base it was "unverifiable" (ORA-00904 on "note")."""
    from tests import typed_fidelity_helpers as tf

    _oracle_conn().close()
    src_t = f"d356_src_{uuid.uuid4().hex[:8]}"
    dst_t = f"d356_dst_{uuid.uuid4().hex[:8]}"
    source = _seed_pg(src_t) if src_kind == "postgresql" else _seed_mariadb(src_t)
    try:
        result = _run(fake_jobs, _request(source, tf.oracle_endpoint(dst_t), mode))
        assert result.success, result.error
        proof = (result.reconciliation or {}).get("transform_applied") or {}
        assert proof.get("status") == "verified", proof
        assert proof.get("provenance") == "independent SQL oracle", proof
        assert proof.get("oracle_uncovered_steps") == [], proof
        assert _oracle_rows(dst_t) == _DEST_WANT
    finally:
        _drop_source(src_kind, src_t)
        _drop_oracle(dst_t)


def test_live_oracle_destination_proof_catches_a_wrong_value(fake_jobs, monkeypatch):
    """The check really compares now: a value changed in the Oracle table
    after the write fails the proof, naming the column and the key."""
    from tests import typed_fidelity_helpers as tf

    _oracle_conn().close()
    src_t = f"d356_src_{uuid.uuid4().hex[:8]}"
    dst_t = f"d356_dst_{uuid.uuid4().hex[:8]}"
    original = rs._transform_applied_proof

    def _tamper_then_prove(**kwargs):
        _oracle_exec(
            f'UPDATE "{_oracle_owner()}"."{dst_t.upper()}" '  # nosec B608
            "SET \"NOTE\" = 'wrong' WHERE \"ID\" = 2"
        )
        return original(**kwargs)

    monkeypatch.setattr(rs, "_transform_applied_proof", _tamper_then_prove)
    source = _seed_pg(src_t)
    try:
        result = _run(
            fake_jobs, _request(source, tf.oracle_endpoint(dst_t), "incremental_upsert")
        )
        proof = (result.reconciliation or {}).get("transform_applied") or {}
        assert proof.get("status") == "failed", proof
        assert proof.get("culprit_columns") == ["note"], proof
        assert not result.success
    finally:
        _drop_source("postgresql", src_t)
        _drop_oracle(dst_t)
