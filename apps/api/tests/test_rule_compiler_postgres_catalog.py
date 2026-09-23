"""Live Postgres catalog bind — two real tables, same column name.

Skip when Postgres is not accepting connections. This is not a mock catalog:
columns come from information_schema after CREATE TABLE.
"""

from __future__ import annotations

import subprocess
import uuid

import pytest

from services.rule_compiler.compile import compile_rule_workbook


def _psql(sql: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["sudo", "-n", "-u", "postgres", "psql", "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-At", "-c", sql],
        capture_output=True,
        text=True,
    )


def _postgres_up() -> bool:
    ready = subprocess.run(["pg_isready"], capture_output=True, text=True)
    if ready.returncode != 0:
        return False
    probe = _psql("SELECT 1")
    return probe.returncode == 0


pytestmark = pytest.mark.skipif(not _postgres_up(), reason="Postgres is not accepting connections")


def test_live_postgres_customers_and_orders_id_is_not_silent():
    suffix = uuid.uuid4().hex[:8]
    customers = f"df_rule_mt_customers_{suffix}"
    orders = f"df_rule_mt_orders_{suffix}"
    try:
        created = _psql(
            f"""
            CREATE TABLE {customers} (
                id integer PRIMARY KEY,
                email text NOT NULL,
                full_name text
            );
            CREATE TABLE {orders} (
                id integer PRIMARY KEY,
                customer_id integer NOT NULL,
                amount numeric(12,2)
            );
            INSERT INTO {customers}(id, email, full_name)
            VALUES (1, 'a@example.com', 'Ada');
            INSERT INTO {orders}(id, customer_id, amount)
            VALUES (10, 1, 19.50);
            """
        )
        assert created.returncode == 0, created.stderr

        catalog_out = _psql(
            "SELECT table_name || '=' || string_agg(column_name, ',' ORDER BY ordinal_position) "
            "FROM information_schema.columns "
            f"WHERE table_schema = 'public' AND table_name IN ('{customers}', '{orders}') "
            "GROUP BY table_name ORDER BY table_name"
        )
        assert catalog_out.returncode == 0, catalog_out.stderr
        catalog: dict[str, list[str]] = {}
        for line in catalog_out.stdout.splitlines():
            if "=" not in line:
                continue
            table, cols = line.split("=", 1)
            catalog[table] = [c for c in cols.split(",") if c]
        assert set(catalog) == {customers, orders}
        assert "id" in catalog[customers] and "id" in catalog[orders]
        assert "email" in catalog[customers]
        assert "amount" in catalog[orders]

        csv = (
            "Source,Source Column,Destination Column,Rule\n"
            f"{customers},id,customer_id,Direct\n"
            f"{orders},id,order_id,Direct\n"
            ",id,mystery_id,Direct\n"
            f"{customers},email,email,lowercase\n"
            f"{orders},amount,amount,Direct\n"
        ).encode()
        report = compile_rule_workbook(
            "live-postgres.csv",
            csv,
            source_tables=[customers, orders],
            source_catalog=catalog,
            dest_columns=["customer_id", "order_id", "mystery_id", "email", "amount"],
        )
        by = {(r.get("source_table"), r.get("source_column")): r for r in report["rules"]}
        assert by[(customers, "id")]["status"] == "executable"
        assert by[(customers, "id")]["dest_column"] == "customer_id"
        assert by[(orders, "id")]["status"] == "executable"
        assert by[(orders, "id")]["dest_column"] == "order_id"
        mystery = next(
            r for r in report["rules"]
            if r.get("dest_column") == "mystery_id"
            or (r.get("source_column") == "id" and not r.get("source_table"))
        )
        assert mystery["status"] == "needs_confirmation"
        assert any("Name Table.column" in issue or "silent remap" in issue for issue in mystery["issues"])
        assert by[(customers, "email")]["status"] == "executable"
        assert by[(customers, "email")]["kind"] == "case_lower"
        assert by[(customers, "email")]["transform"] in {"lower", "case_lower"}
        assert by[(orders, "amount")]["status"] == "executable"
        rows = _psql(f"SELECT count(*) FROM {customers}").stdout.strip()
        assert rows == "1"
        assert any(item["source_table"] == customers for item in report["projection"])
    finally:
        _psql(f"DROP TABLE IF EXISTS {orders}; DROP TABLE IF EXISTS {customers};")


def _catalog_and_types(tables: list[str]) -> tuple[dict[str, list[str]], dict[str, str]]:
    listed = ", ".join(f"'{name}'" for name in tables)
    catalog_out = _psql(
        "SELECT table_name || '=' || string_agg(column_name, ',' ORDER BY ordinal_position) "
        "FROM information_schema.columns "
        f"WHERE table_schema = 'public' AND table_name IN ({listed}) "
        "GROUP BY table_name"
    )
    assert catalog_out.returncode == 0, catalog_out.stderr
    catalog: dict[str, list[str]] = {}
    for line in catalog_out.stdout.splitlines():
        if "=" not in line:
            continue
        table, cols = line.split("=", 1)
        catalog[table] = [c for c in cols.split(",") if c]
    types_out = _psql(
        "SELECT table_name || '.' || column_name || '=' || "
        "CASE WHEN is_identity = 'YES' THEN "
        "  data_type || ' GENERATED ALWAYS AS IDENTITY' "
        "WHEN data_type = 'numeric' THEN "
        "  'NUMERIC(' || numeric_precision || ',' || numeric_scale || ')' "
        "ELSE data_type END "
        "FROM information_schema.columns "
        f"WHERE table_schema = 'public' AND table_name IN ({listed})"
    )
    assert types_out.returncode == 0, types_out.stderr
    types: dict[str, str] = {}
    for line in types_out.stdout.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        types[key] = value
    return catalog, types


def test_live_postgres_dest_catalog_identity_and_precision():
    suffix = uuid.uuid4().hex[:8]
    customers = f"df_rule_src_cust_{suffix}"
    orders = f"df_rule_src_ord_{suffix}"
    dim = f"df_rule_dim_cust_{suffix}"
    fact = f"df_rule_fact_ord_{suffix}"
    try:
        created = _psql(
            f"""
            CREATE TABLE {customers} (
                id integer PRIMARY KEY,
                email text,
                pay numeric(18,6)
            );
            CREATE TABLE {orders} (
                id integer PRIMARY KEY,
                amount numeric(12,2)
            );
            CREATE TABLE {dim} (
                id integer PRIMARY KEY,
                email text,
                amount numeric(10,2)
            );
            CREATE TABLE {fact} (
                id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                customer_id integer,
                amount numeric(12,2)
            );
            INSERT INTO {customers} VALUES (1, 'a@example.com', 12.345678);
            INSERT INTO {orders} VALUES (10, 19.50);
            """
        )
        assert created.returncode == 0, created.stderr
        src_cat, src_types = _catalog_and_types([customers, orders])
        dst_cat, dst_types = _catalog_and_types([dim, fact])
        assert "GENERATED ALWAYS AS IDENTITY" in dst_types[f"{fact}.id"]

        csv = (
            "Source,Source Column,Destination,Destination Column,Rule\n"
            f"{customers},id,{dim},id,Direct\n"
            f"{orders},id,{fact},id,Direct\n"
            f"{customers},email,{dim},email,lowercase\n"
            f"{customers},pay,{dim},amount,Direct\n"
            f"{orders},amount,{fact},amount,Direct\n"
        ).encode()
        report = compile_rule_workbook(
            "live-dest.csv",
            csv,
            source_tables=[customers, orders],
            source_catalog=src_cat,
            dest_tables=[dim, fact],
            dest_catalog=dst_cat,
            source_types=src_types,
            dest_types=dst_types,
        )
        by = {
            (r.get("source_table"), r.get("source_column"), r.get("dest_table")): r
            for r in report["rules"]
        }
        assert by[(customers, "id", dim)]["status"] == "executable"
        assert by[(orders, "id", fact)]["status"] == "needs_confirmation"
        assert any("identity" in i.lower() or "generated" in i.lower() for i in by[(orders, "id", fact)]["issues"])
        assert by[(customers, "email", dim)]["status"] == "executable"
        pay = by[(customers, "pay", dim)]
        assert pay["status"] == "needs_confirmation"
        assert any("precision" in i.lower() or "scale" in i.lower() for i in pay["issues"])
        assert by[(orders, "amount", fact)]["status"] == "executable"
        assert {item["source_table"] for item in report["projection"]} >= {customers, orders}
    finally:
        _psql(
            f"DROP TABLE IF EXISTS {fact}; DROP TABLE IF EXISTS {dim}; "
            f"DROP TABLE IF EXISTS {orders}; DROP TABLE IF EXISTS {customers};"
        )
