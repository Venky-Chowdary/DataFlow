"""One answer to "how does this engine enforce upsert identity?" (D182/D101/CR-D126).

The capability registry (``connectors.merge_registry.upsert_identity``) is read
by Validate's disclosure, the DDL writers and the remediation text. These pin
that answer; the live routes are proven in ``test_r4_upsert_identity_e2e.py``.
"""
from __future__ import annotations

import pytest


@pytest.mark.parametrize(
    "dialect,identity",
    [
        ("postgresql", "unique_index"),
        ("mysql", "unique_index"),
        ("mariadb", "unique_index"),
        ("snowflake", "merge"),
        ("sqlserver", "merge"),
        ("bigquery", "merge"),
        ("databricks", "merge"),
        ("oracle", "merge"),
        ("redshift", "merge"),
        ("sqlite", "merge"),
    ],
)
def test_registry_answers_upsert_identity(dialect, identity):
    from connectors.merge_registry import upsert_identity
    from services.dest_dialect_facts import dest_upsert_needs_unique_index

    assert upsert_identity(dialect) == identity
    assert dest_upsert_needs_unique_index(dialect) is (identity == "unique_index")


@pytest.mark.parametrize("dialect", ["snowflake", "sqlserver", "bigquery", "databricks"])
def test_merge_engines_never_get_index_ddl(dialect):
    from services.upsert_identity import plan_upsert_identity

    plan = plan_upsert_identity(dialect, table_name="t", key_columns=["id"])
    assert plan.mode == "merge"
    assert plan.ddl == ()


def _mysql_query(data_type, longest, charset="utf8mb4", collation="utf8mb4_bin"):
    def query(sql, params):
        if "INFORMATION_SCHEMA.COLUMNS" in sql:
            return [("id", data_type, "YES", charset, collation, 65535)]
        return [(longest,)]

    return query


def test_mysql_text_key_plan_is_full_width_never_a_prefix():
    from services.upsert_identity import plan_upsert_identity

    plan = plan_upsert_identity(
        "mysql", table_name="t", key_columns=["id"], query=_mysql_query("text", 192)
    )
    assert plan.mode == "unique_index"
    assert plan.ddl[0].startswith("ALTER TABLE `t` MODIFY `id` VARCHAR(768)")
    assert plan.ddl[1] == "CREATE UNIQUE INDEX `uidx_t_id` ON `t` (`id`)"
    assert "(191)" not in plan.disclosure


@pytest.mark.parametrize(
    "data_type,longest,needle",
    [("json", 0, "cannot index"), ("longblob", 0, "cannot index"), ("text", 900, "900 characters")],
)
def test_mysql_key_that_cannot_be_enforced_is_impossible_with_a_reason(data_type, longest, needle):
    from services.upsert_identity import plan_upsert_identity

    plan = plan_upsert_identity(
        "mysql", table_name="t", key_columns=["id"], query=_mysql_query(data_type, longest)
    )
    assert plan.mode == "impossible"
    assert plan.ddl == ()
    assert needle in plan.reason and "prefix" in plan.reason


def test_unbounded_text_key_widths_fit_each_engine_key_limit():
    import sqlalchemy as sa

    from services.upsert_identity import unbounded_key_widths

    mssql = unbounded_key_widths("mssql", ["id"], lambda c: sa.VARCHAR())
    assert str(mssql["id"].compile()) == "NVARCHAR(450)"
    ora = unbounded_key_widths("oracle", ["a", "b"], lambda c: sa.Text())
    assert ora["a"].length == 500 and ora["b"].length == 500
    assert unbounded_key_widths("mssql", ["id"], lambda c: sa.String(40)) == {}


def test_duplicate_keys_on_a_merge_engine_never_recommend_index_ddl():
    from services.root_cause_engine import apply_root_causes_to_preflight

    details = {
        "rule_id": "g_upsert_dest_key",
        "upsert_key_columns": ["ID"],
        "planned_ddl": "",
        "duplicate_key_count": 1,
        "duplicate_row_count": 2,
        "upsert_identity": "merge",
    }
    out = apply_root_causes_to_preflight(
        {
            "passed": False,
            "gates": [{"id": "g_upsert_dest_key", "status": "block", "message": "dup", "details": details}],
            "blockers": [{"id": "g_upsert_dest_key", "message": "dup", "details": details}],
        }
    )
    rc = next(r for r in out.get("root_causes") or [] if r.get("kind") == "upsert_dest_ddl")
    text = str(rc)
    assert "approve the disclosed DDL" not in text
    assert "Create a unique constraint" not in text
    assert "Deduplicate the destination" in rc.get("recommended_fix", "")
