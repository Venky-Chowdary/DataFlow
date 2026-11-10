"""D350: ``default:TEXT`` inside a column_transforms pipeline replaces an empty
cell — NULL, ``''`` and whitespace-only text — as the catalog ("use literal
when cell is empty") and the single-step Map transform do.

QA (SQL Server -> MariaDB, ``note: ["trim", "default:NA"]``): ``''`` and
``'   '`` (trimmed to ``''``) landed as ``''``; only NULL became ``'NA'``. The
pipeline compiled ``default:`` to the shape step ``default_if_null``, whose
blank test on a typed (database) source is NULL only (X-1), and the G-1 SQL
oracle used the same rule, so the transform proof said "verified".

Not changed: ``trim`` alone on a typed source keeps ``''`` (X-1), and the
Transform Library step ``default_if_null`` still means NULL.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from services.column_transform_chain import (  # noqa: E402
    compile_column_chains,
    split_column_transforms,
)
from services.shape_apply import ShapeRunner  # noqa: E402
from services.shape_engine import ShapeEngine  # noqa: E402
from services.shape_expr import _BLANK_AS_NULL  # noqa: E402
from services.shape_models import ShapeRecipe  # noqa: E402
from services.transform_engine import TRANSFORM_SEMANTICS, apply_transform  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from src.transfer.reconcile_step import _recipe_step_sql  # noqa: E402

pytestmark = pytest.mark.timeout(600)

VALUES = ["", "   ", None, "real", "  padded  ", "\t"]
WANT = ["NA", "NA", "NA", "real", "padded", "NA"]


def _shape(chains, columns, rows, *, typed: bool):
    payload, refusal = compile_column_chains(chains, columns)
    assert refusal == ""
    engine = ShapeEngine(ShapeRecipe.parse(payload, source_columns=columns))
    token = _BLANK_AS_NULL.set(not typed)
    try:
        return engine.apply_batch(rows)
    finally:
        _BLANK_AS_NULL.reset(token)


@pytest.mark.parametrize("typed", [True, False])
def test_pipeline_trim_then_default_replaces_empty_whitespace_and_null(typed):
    out = _shape(
        {"note": ["trim", "default:NA"]}, ["note"], [{"note": v} for v in VALUES], typed=typed
    )
    assert [r["note"] for r in out] == WANT


@pytest.mark.parametrize("typed", [True, False])
def test_pipeline_default_matches_the_single_step_map_transform(typed):
    values = ["", "   ", None, "real", " keep spaces "]
    out = _shape(
        {"note": ["default:NA", "upper"]}, ["note"], [{"note": v} for v in values], typed=typed
    )
    single = [apply_transform(apply_transform(v, "default:NA")[0], "upper")[0] for v in values]
    assert [r["note"] for r in out] == single == ["NA", "NA", "NA", "REAL", " KEEP SPACES "]


def test_single_step_default_is_unchanged():
    assert [apply_transform(v, "default:NA")[0] for v in ["", "   ", None, "x"]] == [
        "NA", "NA", "NA", "x",
    ]
    assert "empty" in TRANSFORM_SEMANTICS["default:"]["semantics"]


def test_trim_alone_on_a_typed_source_keeps_empty_text():
    """X-1 is not reverted: '' is a stored value on a database source."""
    out = _shape({"note": ["trim", "upper"]}, ["note"], [{"note": v} for v in VALUES], typed=True)
    assert [r["note"] for r in out] == ["", "", None, "REAL", "PADDED", ""]


def test_library_default_if_null_still_means_null_on_a_typed_source():
    """The Transform Library step's name says NULL; saved recipes keep it."""
    recipe = ShapeRecipe.parse(
        {"steps": [{"op": "default_if_null", "column": "v", "options": {"value": "NA"}}]},
        source_columns=["v"],
    )
    out = ShapeRunner(recipe, typed_values=True).records(
        [{"v": ""}, {"v": "   "}, {"v": None}, {"v": "x"}]
    )
    assert [r["v"] for r in out] == ["", "   ", "NA", "x"]


def test_pipeline_default_compiles_to_the_empty_aware_step():
    payload, refusal = compile_column_chains({"note": ["trim", "default:NA"]}, ["note"])
    assert refusal == ""
    assert payload["steps"][1] == {
        "op": "default_if_null",
        "options": {"value": "NA", "include_empty": True},
        "column": "note",
    }


def test_include_empty_must_be_a_boolean():
    from services.shape_models import ShapeError

    with pytest.raises(ShapeError, match="include_empty"):
        ShapeRecipe.parse(
            {"steps": [{"op": "default_if_null", "column": "v",
                        "options": {"value": "NA", "include_empty": "yes"}}]},
            source_columns=["v"],
        )


@pytest.mark.parametrize("src_type", ["postgresql", "mysql", "mariadb", "sqlserver"])
def test_sql_oracle_defaults_empty_text_too(src_type):
    """The G-1 oracle states the same rule, so it cannot agree with a wrong ''."""
    params: dict = {}
    sql = _recipe_step_sql(
        "default_if_null", {"value": "NA", "include_empty": True}, "c", params,
        src_type=src_type,
    )
    assert sql is not None
    assert "IS NULL" in sql and "= ''" in sql
    assert "NA" in params.values()
    # The library step's NULL-only fragment is unchanged.
    plain = _recipe_step_sql("default_if_null", {"value": "NA"}, "c", {}, src_type=src_type)
    assert "= ''" not in plain


# ---------------------------------------------------------------------------
# Real runs through the engine: SQLite in-process; live typed pairs when the
# engines are configured. The proof must say verified only when it is true.
# ---------------------------------------------------------------------------

_COLS = ["id", "name", "note"]
_ROWS = [
    (1, "ada", "  x  "),
    (2, "bob", ""),
    (3, "cy", "   "),
    (4, "di", None),
    (5, "eve", "real"),
]
_DEST_WANT = [(1, "ada", "x"), (2, "bob", "NA"), (3, "cy", "NA"), (4, "di", "NA"), (5, "eve", "real")]
_TRANSFORMS = {"note": ["trim", "default:NA"]}


def _request(source, dest, mode: str = "full_refresh_overwrite") -> TransferRequest:
    """What MCP start_transfer stages for the column_transforms."""
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
    job_id = "d350" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _sqlite_pair(tmp_path: Path) -> tuple[Path, Path]:
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    conn = sqlite3.connect(str(src))
    with conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, note TEXT)")
        conn.executemany("INSERT INTO t VALUES (?, ?, ?)", _ROWS)
    conn.close()
    return src, dst


def _sqlite_ep(path: Path) -> EndpointConfig:
    return EndpointConfig(kind="database", format="sqlite", database=str(path), table="t")


def _sqlite_rows(path: Path) -> list[tuple]:
    conn = sqlite3.connect(str(path))
    try:
        return conn.execute("SELECT id, name, note FROM t ORDER BY id").fetchall()
    finally:
        conn.close()


@pytest.mark.parametrize("mode", ["full_refresh_overwrite", "incremental_upsert"])
def test_sqlite_run_writes_na_for_empty_and_the_proof_verifies(tmp_path, fake_jobs, mode):
    src, dst = _sqlite_pair(tmp_path)
    result = _run(fake_jobs, _request(_sqlite_ep(src), _sqlite_ep(dst), mode))

    assert result.success, result.error
    proof = (result.reconciliation or {}).get("transform_applied") or {}
    assert proof.get("status") == "verified", proof
    assert _sqlite_rows(dst) == _DEST_WANT


def test_proof_fails_when_the_destination_holds_empty_instead_of_the_default(
    tmp_path, fake_jobs, monkeypatch
):
    """The rubber stamp is gone: a '' where the recipe says 'NA' fails the proof."""
    import src.transfer.reconcile_step as rs

    src, dst = _sqlite_pair(tmp_path)
    original = rs._transform_applied_proof

    def _write_empty_then_prove(**kwargs):
        conn = sqlite3.connect(str(dst))
        with conn:
            conn.execute("UPDATE t SET note = '' WHERE id IN (2, 3)")
        conn.close()
        return original(**kwargs)

    monkeypatch.setattr(rs, "_transform_applied_proof", _write_empty_then_prove)
    result = _run(fake_jobs, _request(_sqlite_ep(src), _sqlite_ep(dst)))

    assert not result.success
    proof = (result.reconciliation or {}).get("transform_applied") or {}
    assert proof.get("status") == "failed", proof
    assert proof.get("culprit_columns") == ["note"], proof
    assert sorted((proof.get("culprit_keys") or {}).get("changed") or []) == ["2", "3"], proof


# --- live typed pairs --------------------------------------------------------

_MARIADB = {
    "host": os.getenv("DATAFLOW_TEST_MARIADB_HOST", "127.0.0.1"),
    "port": int(os.getenv("DATAFLOW_TEST_MARIADB_PORT", "3307")),
    "database": os.getenv("DATAFLOW_TEST_MARIADB_DB", "dataflow"),
    "username": os.getenv("DATAFLOW_TEST_MARIADB_USER", "dataflow"),
    "password": os.getenv("DATAFLOW_TEST_MARIADB_PASSWORD", "dataflow"),
}


def _mariadb_conn():
    pymysql = pytest.importorskip("pymysql")
    from tests import r4_route_harness as h

    if not h.port_up(_MARIADB["port"], _MARIADB["host"]):
        pytest.skip("MariaDB not reachable (DATAFLOW_TEST_MARIADB_*, :3307)")
    try:
        return pymysql.connect(
            host=_MARIADB["host"], port=_MARIADB["port"], user=_MARIADB["username"],
            password=_MARIADB["password"], database=_MARIADB["database"],
            charset="utf8mb4", autocommit=True, connect_timeout=5,
        )
    except pymysql.MySQLError as exc:
        pytest.skip(f"MariaDB not reachable: {exc}")


def _mariadb_ep(table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database", format="mariadb", host=_MARIADB["host"], port=_MARIADB["port"],
        database=_MARIADB["database"], username=_MARIADB["username"],
        password=_MARIADB["password"], schema=_MARIADB["database"], table=table,
    )


def _seed(kind: str, table: str) -> EndpointConfig:
    from tests import r4_route_harness as h
    from tests import typed_fidelity_helpers as tf

    if kind == "postgresql":
        conn = h.pg_conn()
        q, ph, ep = f'public."{table}"', "%s", tf.pg_endpoint(table)
    elif kind == "mysql":
        conn = h.mysql_conn()
        q, ph, ep = f"`{table}`", "%s", tf.mysql_endpoint(table)
    else:
        tf.require_sqlserver_drivers()
        conn = h.mssql_conn()
        q, ph, ep = f"[{table}]", "?", tf.sqlserver_endpoint(table)
    try:
        cur = conn.cursor()
        cur.execute(
            f"CREATE TABLE {q} (id integer PRIMARY KEY, name varchar(40), note varchar(40))"  # nosec B608
        )
        for row in _ROWS:
            cur.execute(f"INSERT INTO {q} VALUES ({ph}, {ph}, {ph})", row)  # nosec B608
    finally:
        conn.close()
    return ep


def _dest(kind: str, table: str) -> EndpointConfig:
    from tests import r4_route_harness as h
    from tests import typed_fidelity_helpers as tf

    if kind == "mariadb":
        _mariadb_conn().close()
        return _mariadb_ep(table)
    if kind == "mysql":
        h.dest_config("mysql")
        return tf.mysql_endpoint(table)
    h.dest_config("postgresql")
    return tf.pg_endpoint(table)


def _read_dest(kind: str, table: str) -> list[tuple]:
    from tests import r4_route_harness as h

    sql = "SELECT id, name, note FROM {} ORDER BY id"
    if kind == "mariadb":
        conn = _mariadb_conn()
        try:
            cur = conn.cursor()
            cur.execute(sql.format(f"`{table}`"))  # nosec B608
            return [tuple(r) for r in cur.fetchall()]
        finally:
            conn.close()
    return [tuple(r) for r in h.query(kind, sql.format(h.quote(kind, table)))]  # nosec B608


def _drop(kind: str, table: str) -> None:
    from tests import r4_route_harness as h

    if kind == "mariadb":
        conn = _mariadb_conn()
        try:
            conn.cursor().execute(f"DROP TABLE IF EXISTS `{table}`")  # nosec B608
        finally:
            conn.close()
        return
    h.drop_table(kind, table)


@pytest.mark.parametrize(
    ("src_kind", "dst_kind"),
    [("sqlserver", "mariadb"), ("postgresql", "mysql"), ("mysql", "postgresql")],
)
def test_live_typed_pair_defaults_empty_and_the_proof_verifies(fake_jobs, src_kind, dst_kind):
    """The QA pair (SQL Server -> MariaDB) and two more typed sources.

    On PostgreSQL and MySQL the SQL oracle recomputes the recipe in the source
    database; had it kept the NULL-only rule it would disagree with the
    writer's 'NA' and fail the run.
    """
    src_t = f"d350_src_{uuid.uuid4().hex[:8]}"
    dst_t = f"d350_dst_{uuid.uuid4().hex[:8]}"
    dest = _dest(dst_kind, dst_t)
    try:
        source = _seed(src_kind, src_t)
        result = _run(fake_jobs, _request(source, dest, "incremental_upsert"))
        assert result.success, result.error
        proof = (result.reconciliation or {}).get("transform_applied") or {}
        assert proof.get("status") == "verified", proof
        if src_kind != "sqlserver":
            assert proof.get("provenance") == "independent SQL oracle", proof
            assert proof.get("oracle_uncovered_steps") == [], proof
        assert _read_dest(dst_kind, dst_t) == _DEST_WANT
    finally:
        _drop(src_kind, src_t)
        _drop(dst_kind, dst_t)


@pytest.mark.parametrize("src_kind", ["postgresql", "mysql", "sqlserver"])
def test_live_oracle_fragment_equals_the_pipeline_value(src_kind):
    """The oracle's SQL for [trim, default:NA], run in the source database,
    gives exactly what the shape pipeline writes — cell by cell."""
    import sqlalchemy as sa

    from services.dest_precount import warehouse_sql_engine
    from src.transfer.adapters import resolve_connector_config

    table = f"d350_orc_{uuid.uuid4().hex[:8]}"
    source = _seed(src_kind, table)
    try:
        payload, refusal = compile_column_chains(_TRANSFORMS, _COLS)
        assert refusal == ""
        qs = {"mysql": "`", "sqlserver": "["}.get(src_kind, '"')
        qe = "]" if qs == "[" else qs
        expr, params = f"{qs}note{qe}", {}
        for step in payload["steps"]:
            expr = _recipe_step_sql(
                step["op"], step["options"], expr, params, src_type=src_kind
            )
            assert expr is not None, step
        ref = {"postgresql": f'public."{table}"', "mysql": f"`{table}`"}.get(
            src_kind, f"[dbo].[{table}]"
        )
        stmt = sa.text(f"SELECT id, {expr} FROM {ref} ORDER BY id").bindparams(**params)  # nosec B608
        cfg = resolve_connector_config(source)
        with warehouse_sql_engine(src_kind, cfg) as eng, eng.connect() as conn:
            got = [tuple(r) for r in conn.execute(stmt).fetchall()]
        want = [(r[0], r[2]) for r in _DEST_WANT]
        assert got == want
    finally:
        _drop(src_kind, table)
