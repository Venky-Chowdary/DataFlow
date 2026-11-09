"""QA1 audit of PR #372: no unapproved / zero-write DDL; create-new approval; operator key.

Real-service tests run against local PostgreSQL / MySQL (docker df-pg / df-mysql)
and skip with the reason when the service is not reachable.
"""

from __future__ import annotations

import os
from types import SimpleNamespace

import pytest

os.environ.setdefault("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")

PG = dict(host="127.0.0.1", port=5432, username="dataflow", password="dataflow", database="dataflow")
MY = dict(host="127.0.0.1", port=3306, username="root", password="dataflow", database="dataflow")


def _pg():
    psycopg2 = pytest.importorskip("psycopg2")
    try:
        c = psycopg2.connect(host=PG["host"], port=PG["port"], user=PG["username"],
                             password=PG["password"], dbname=PG["database"], connect_timeout=3)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    c.autocommit = True
    return c


def _my():
    pymysql = pytest.importorskip("pymysql")
    try:
        c = pymysql.connect(host=MY["host"], port=MY["port"], user=MY["username"],
                            password=MY["password"], database=MY["database"],
                            autocommit=True, connect_timeout=3)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"MySQL not reachable: {exc}")
    return c


def _write(dest, **kw):
    if dest == "pg":
        from connectors.postgresql_writer import write_mapped_rows

        return write_mapped_rows(**PG, schema="public", connection_string="", ssl=False, **kw)
    from connectors.mysql_writer import write_mapped_rows

    return write_mapped_rows(**MY, schema="", connection_string="", ssl=False, **kw)


def _columns(dest, conn, table):
    cur = conn.cursor()
    if dest == "pg":
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name=%s", (table,))
    else:
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='dataflow' AND table_name=%s", (table,))
    return {r[0].lower() for r in cur.fetchall()}


def _keys(dest, conn, table):
    cur = conn.cursor()
    if dest == "pg":
        cur.execute(
            "SELECT tc.constraint_type, kcu.column_name FROM information_schema.table_constraints tc "
            "JOIN information_schema.key_column_usage kcu ON tc.constraint_name=kcu.constraint_name "
            "AND tc.table_schema=kcu.table_schema WHERE tc.table_schema='public' AND tc.table_name=%s "
            "AND tc.constraint_type IN ('PRIMARY KEY','UNIQUE')", (table,))
    else:
        cur.execute("SELECT constraint_name, column_name FROM information_schema.key_column_usage "
                    "WHERE table_schema='dataflow' AND table_name=%s", (table,))
    return {(str(a), str(b).lower()) for a, b in cur.fetchall()}


def _count(conn, table):
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    return int(cur.fetchone()[0])


def _connect(dest):
    return _pg() if dest == "pg" else _my()


# --- 1. ADD COLUMN: never on a zero-write run, never unapproved under manual_review ---

ADD_TABLE = "qa1_add_col_dest"
ADD_HEADERS = ["id", "price", "active"]
ADD_MAPS = [
    {"source": "id", "target": "id", "target_type": "INT"},
    {"source": "price", "target": "price", "target_type": "DECIMAL(10,2)"},
    {"source": "active", "target": "active", "target_type": "BOOLEAN", "create_new": True},
]


def _add_col_run(dest, frac, policy):
    conn = _connect(dest)
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {ADD_TABLE}")
    cur.execute(f"CREATE TABLE {ADD_TABLE} (id INT PRIMARY KEY, price DECIMAL(10,2))")
    rows = [[str(i), f"{i}.{frac}", "true"] for i in range(1, 13)]
    res = _write(
        dest, table_name=ADD_TABLE, headers=ADD_HEADERS, data_rows=rows, mappings=ADD_MAPS,
        column_types={"id": "integer", "price": "numeric(12,5)", "active": "boolean"},
        create_table=True, write_mode="insert", schema_policy=policy, validation_mode="balanced",
    )
    return conn, res


@pytest.mark.parametrize("dest", ["pg", "mysql"])
def test_zero_write_run_issues_no_add_column(dest):
    conn, res = _add_col_run(dest, "12345", "propagate_columns")
    assert res.rows_written == 0
    assert len(res.rejected_details or []) >= 12, "every row must be quarantined, not dropped"
    assert _count(conn, ADD_TABLE) == 0
    assert "active" not in _columns(dest, conn, ADD_TABLE)


@pytest.mark.parametrize("dest", ["pg", "mysql"])
def test_rows_that_land_still_get_the_approved_add_column(dest):
    conn, res = _add_col_run(dest, "12", "propagate_columns")
    assert res.ok and res.rows_written == 12
    assert "active" in _columns(dest, conn, ADD_TABLE)


# --- 2. create-new CREATE TABLE needs approval under manual_review ---


def test_validate_blocks_unapproved_create_new_and_discloses_ddl():
    from services.create_ddl_gate import create_ddl_gate

    kw = dict(schema_policy="manual_review", table_exists=False, can_create=True,
              destination_table="public.qa_new",
              mappings=[{"source": "sku", "target": "sku", "target_type": "TEXT"}])
    gate = create_ddl_gate(acknowledged=False, **kw)
    assert gate["status"] == "block"
    assert gate["details"]["requires_approval"] is True
    assert gate["details"]["remediation_kind"] == "approve_ddl_or_dedupe"
    assert gate["details"]["planned_ddl"].startswith("CREATE TABLE public.qa_new (sku TEXT")
    assert create_ddl_gate(acknowledged=True, **kw)["status"] == "pass"
    assert create_ddl_gate(acknowledged=False, **{**kw, "table_exists": True}) is None
    assert create_ddl_gate(acknowledged=False, **{**kw, "schema_policy": "propagate_columns"}) is None


def test_execute_refuses_unapproved_create_new():
    from services.create_ddl_gate import refuse_unapproved_create

    req = SimpleNamespace(schema_policy="manual_review", ddl_changes_acknowledged=False,
                          destination=SimpleNamespace(table="qa_new"),
                          model_fields_set={"schema_policy"})
    with pytest.raises(ValueError, match="CREATE TABLE was not approved"):
        refuse_unapproved_create(req, False)
    refuse_unapproved_create(req, True)
    req.ddl_changes_acknowledged = True
    refuse_unapproved_create(req, False)
    # The request default (no operator-declared policy) is the golden path.
    default = SimpleNamespace(schema_policy="manual_review", ddl_changes_acknowledged=False,
                              destination=SimpleNamespace(table="qa_new"), model_fields_set=set())
    refuse_unapproved_create(default, False)


def test_preflight_surfaces_create_new_ddl_gate():
    from services.preflight_service import run_file_preflight

    common = dict(
        columns=["sku", "qty"],
        column_types={"sku": "string", "qty": "integer"},
        row_count=1,
        mappings=[{"source": "sku", "target": "sku", "target_type": "TEXT"},
                  {"source": "qty", "target": "qty", "target_type": "INTEGER"}],
        sample_rows=[{"sku": "A1", "qty": "1"}],
        destination_table="qa_new", destination_connected=True,
        destination_can_create=True, destination_table_exists=False,
        destination_db_type="postgresql", schema_policy="manual_review",
        schema_policy_explicit=True,
    )
    pf = run_file_preflight(**common)
    gate = next((g for g in pf["gates"] if g["id"] == "g_create_dest_ddl"), None)
    assert gate is not None and gate["status"] == "block"
    assert not pf["passed"]
    ok = run_file_preflight(**common, ddl_changes_acknowledged=True)
    assert next(g for g in ok["gates"] if g["id"] == "g_create_dest_ddl")["status"] == "pass"
    # The request default (manual_review, not declared) stays the golden path.
    default = run_file_preflight(**{**common, "schema_policy_explicit": False})
    assert all(g["id"] != "g_create_dest_ddl" for g in default["gates"])


# --- 3. operator-chosen key lands in CREATE TABLE on create-new append ---


@pytest.mark.parametrize("dest", ["pg", "mysql"])
@pytest.mark.parametrize("key", [["sku"], []])
def test_operator_key_materializes_on_create_new_append(dest, key):
    conn = _connect(dest)
    table = "qa1_key_new"
    conn.cursor().execute(f"DROP TABLE IF EXISTS {table}")
    res = _write(
        dest, table_name=table, headers=["sku", "name", "qty"],
        data_rows=[["A1", "x", "1"], ["B2", "y", "2"]],
        mappings=[{"source": h, "target": h} for h in ["sku", "name", "qty"]],
        column_types={"sku": "string", "name": "string", "qty": "integer"},
        create_table=True, write_mode="insert", conflict_columns=key, validation_mode="balanced",
    )
    assert res.ok and res.rows_written == 2, res.error
    assert _count(conn, table) == 2
    keyed = {col for _, col in _keys(dest, conn, table)}
    if key:
        assert keyed == {"sku"}
    else:
        assert "sku" not in keyed


# --- 1b. Validate's cell preview predicts the writer's decimal-scale quarantine ---


@pytest.mark.parametrize("frac,expected", [("12345", 12), ("12", 0)])
def test_cell_preview_predicts_decimal_scale_quarantine(frac, expected):
    from services.transform_engine import preview_quarantine_cells

    out = preview_quarantine_cells(
        headers=["id", "price"],
        sample_rows=[[str(i), f"{i}.{frac}"] for i in range(1, 13)],
        mappings=[{"source": "id", "target": "id", "target_type": "INT"},
                  {"source": "price", "target": "price", "target_type": "DECIMAL(10,2)"}],
        column_types={"id": "integer", "price": "numeric(12,5)"},
        dest_db="mysql",
        database_extract=True,
    )
    assert out["quarantine_count"] == expected
