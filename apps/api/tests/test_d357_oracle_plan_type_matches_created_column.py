"""D357: the Oracle plan states the column type the table is created with.

QA (Timescale -> Oracle, overwrite with transforms): the plan said the text
columns would be ``VARCHAR2(4000)``; the table had ``CLOB``. A transformed
column (and PostgreSQL's unsized ``varchar``) reaches the planner as a
``VARCHAR`` with no length. The planner stamped the logical-string default
``VARCHAR2(4000)``, an invented width; the writer never clamps an unsized
source to an invented width and created ``CLOB`` — the same rule as a width
over the VARCHAR2 cap and as ``VARCHAR(MAX)``. The plan now says ``CLOB``;
a bounded ``varchar(n)`` still plans and creates ``VARCHAR2(n CHAR)``.
"""

from __future__ import annotations

import re
import uuid

import pytest

from services.type_system import create_new_mapping_target_type

pytestmark = pytest.mark.timeout(900)


# --- unit: the plan's stamp and the writer's DDL for the same source type ----


@pytest.mark.parametrize("src", ["VARCHAR", "varchar", "character varying", "VARCHAR2"])
def test_unsized_varchar_plans_clob_on_oracle(src):
    assert create_new_mapping_target_type(src, "oracle") == "CLOB"


@pytest.mark.parametrize(
    ("src", "want"),
    [
        ("varchar(40)", "VARCHAR2(40 CHAR)"),
        ("VARCHAR(5000)", "CLOB"),
        ("TEXT", "CLOB"),
        # A logical string (no declared type) keeps the platform default.
        ("string", "VARCHAR2(4000)"),
    ],
)
def test_other_string_stamps_on_oracle_are_unchanged(src, want):
    assert create_new_mapping_target_type(src, "oracle") == want


def test_other_destinations_are_unchanged():
    assert create_new_mapping_target_type("VARCHAR", "postgresql") == "TEXT"
    assert create_new_mapping_target_type("VARCHAR", "sqlserver") != "CLOB"


@pytest.mark.parametrize("src", ["VARCHAR", "character varying", "varchar(40)", "TEXT"])
def test_writer_ddl_compiles_to_the_planned_type(src):
    """The writer's CREATE type for the stamp, and for the source type it
    widens an invented stamp to, are both the type the plan states."""
    from sqlalchemy.dialects import oracle

    from connectors.generic_sql import _sa_type_for_logical

    stamp = create_new_mapping_target_type(src, "oracle")
    d = oracle.dialect()
    for carrier in (stamp, src):
        ddl = _sa_type_for_logical(carrier, "oracle", "oracle").compile(dialect=d)
        assert ddl.replace(" ", "") == stamp.replace(" ", ""), (src, carrier, ddl, stamp)


# --- live: plan_transfer -> start_transfer -> the real Oracle catalog --------


@pytest.fixture
def client(tmp_path, monkeypatch):
    from tests import r4_route_harness as h

    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "false")
    monkeypatch.setenv("DATAFLOW_TRAINING", "off")
    from fastapi.testclient import TestClient

    from src.main import app

    with TestClient(app) as c:
        yield c


def _oracle_cfg() -> dict:
    from services.brand_env import getenv_brand
    from tests import r4_route_harness as h
    from tests import typed_fidelity_helpers as tf

    tf.require_oracle_env()
    pytest.importorskip("oracledb")
    host = getenv_brand("ORACLE_HOST", "localhost")
    port = int(getenv_brand("ORACLE_PORT", "1521"))
    if not h.port_up(port, host):
        pytest.skip("Oracle not reachable")
    svc = getenv_brand("ORACLE_SERVICE") or getenv_brand("ORACLE_DATABASE", "ORCLPDB1")
    user = getenv_brand("ORACLE_USER", "dataflow")
    return {
        "type": "oracle", "host": host, "port": port, "database": svc, "service_name": svc,
        "username": user, "password": getenv_brand("ORACLE_PASSWORD", "dataflow"),
        "schema": getenv_brand("ORACLE_SCHEMA") or user.upper(),
    }


def _oracle_catalog(cfg: dict, table: str) -> dict[str, tuple]:
    """column -> (data_type, char_length, char_used); drops the table."""
    import oracledb

    conn = oracledb.connect(
        user=cfg["username"], password=cfg["password"],
        dsn=f"{cfg['host']}:{cfg['port']}/{cfg['database']}",
    )
    owner = str(cfg["schema"]).upper()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT column_name, data_type, char_length, char_used FROM all_tab_columns "
            "WHERE owner = :o AND table_name = :t",
            o=owner, t=table.upper(),
        )
        out = {str(r[0]).lower(): (r[1], int(r[2] or 0), r[3]) for r in cur.fetchall()}
        cur.execute(
            "SELECT table_name FROM all_tables WHERE owner = :o AND UPPER(table_name) = :t",
            o=owner, t=table.upper(),
        )
        for (stored,) in cur.fetchall():
            cur.execute(f'DROP TABLE "{owner}"."{stored}" PURGE')  # nosec B608
        return out
    finally:
        conn.close()


def _as_catalog(planned: str) -> tuple:
    """The ALL_TAB_COLUMNS row a planned Oracle string type creates."""
    t = planned.strip().upper()
    if t in {"CLOB", "NCLOB"}:
        return (t, 0, None)
    m = re.fullmatch(r"(N?VARCHAR2|N?CHAR)\(\s*(\d+)\s*(CHAR|BYTE)?\s*\)", t)
    assert m, f"unexpected planned string type {planned!r}"
    return (m.group(1), int(m.group(2)), "C" if m.group(3) == "CHAR" else "B")


_DDL = {
    "postgresql": (
        'CREATE TABLE "{t}" (id integer PRIMARY KEY, title varchar(40), body text, '
        "code varchar, note varchar(5000), memo text)"
    ),
    "mariadb": (
        "CREATE TABLE `{t}` (id integer PRIMARY KEY, title varchar(40), body text, "
        "code varchar(30), note varchar(5000), memo text)"
    ),
}
_ROWS = [(1, "ada", "  long text  ", "c1", "n1", " m "), (2, " bo ", "", "c2", None, None)]


def _seed(kind: str, table: str) -> dict:
    if kind == "postgresql":
        from tests import r4_route_harness as h

        h.execute("postgresql", _DDL[kind].format(t=table))
        for r in _ROWS:
            vals = ", ".join("NULL" if v is None else "'" + str(v) + "'" for v in r)
            h.execute("postgresql", f'INSERT INTO "{table}" VALUES ({vals})')  # nosec B608
        return h.dest_config("postgresql")
    from tests.test_d350_pipeline_default_on_empty import _MARIADB, _mariadb_conn

    conn = _mariadb_conn()
    try:
        cur = conn.cursor()
        cur.execute(_DDL[kind].format(t=table))
        for r in _ROWS:
            cur.execute(f"INSERT INTO `{table}` VALUES (%s, %s, %s, %s, %s, %s)", r)  # nosec B608
    finally:
        conn.close()
    return {"type": "mariadb", **_MARIADB}


def _drop(kind: str, table: str) -> None:
    if kind == "postgresql":
        from tests import r4_route_harness as h

        h.drop_table("postgresql", table)
    else:
        from tests.test_d350_pipeline_default_on_empty import _drop as _drop_maria

        _drop_maria("mariadb", table)


@pytest.mark.parametrize("src_kind", ["postgresql", "mariadb"])
def test_live_plan_types_equal_the_created_oracle_columns(client, src_kind):
    """Overwrite with transforms on text columns, as QA ran it: every string
    column the plan names is created with exactly that type."""
    from services.connector_store import create_connector
    from tests import r4_route_harness as h

    ora = _oracle_cfg()
    tok = uuid.uuid4().hex[:8]
    src_t, dst_t = f"d357_s_{tok}", f"d357_d_{tok}"
    src_cfg = _seed(src_kind, src_t)
    try:
        src = create_connector(
            {"name": f"d357-s-{tok}", "role": "source", "workspace_id": "", "ssl": False, **src_cfg}
        )
        dst = create_connector(
            {"name": f"d357-d-{tok}", "role": "destination", "workspace_id": "", "ssl": False, **ora}
        )
        args = {
            "source_connector_id": src.id, "source_table": src_t,
            "dest_connector_id": dst.id, "dest_table": dst_t,
            "sync_mode": "full_refresh_overwrite",
            "column_transforms": {
                "body": ["trim", "upper"], "title": ["upper"], "memo": ["trim", "default:NA"],
            },
        }
        ok, plan = h.mcp_call(client, "plan_transfer", dict(args))
        assert ok, plan
        planned = {
            str(c["target_column"]).lower(): str(c["to_type"])
            for c in (plan.get("plan") or plan).get("type_conversions") or []
        }
        strings = {c: t for c, t in planned.items() if c != "id"}
        assert set(strings) == {"title", "body", "code", "note", "memo"}, planned

        h.stage_and_run(client, "start_transfer", dict(args))
        created = _oracle_catalog(ora, dst_t)
        for col, typ in strings.items():
            assert created[col] == _as_catalog(typ), (col, typ, created[col])
        # QA's case: the transformed text columns are CLOB in plan and table.
        assert strings["body"] == strings["memo"] == "CLOB", strings
    finally:
        _drop(src_kind, src_t)
