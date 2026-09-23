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
    finally:
        _psql(f"DROP TABLE IF EXISTS {orders}; DROP TABLE IF EXISTS {customers};")
