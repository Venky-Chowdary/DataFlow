"""UNIQUE / PK catalog helpers — unit-level (no live DB required)."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.schema_introspect import (  # noqa: E402
    _mysql_fetch_foreign_keys,
    _mysql_fetch_unique_keys,
    _oracle_fetch_unique_keys,
    _pg_fetch_foreign_keys,
    _pg_fetch_unique_keys,
    _sqlserver_fetch_unique_keys,
)
from services.type_system import (  # noqa: E402
    parse_case_insensitive_index_expression,
    row_matches_unique_filter,
    unique_equality_key,
    unique_key_forces_casefold,
    unique_key_nulls_collide,
    unique_key_row_in_scope,
)


def test_pg_fetch_unique_keys_groups_pk_and_unique():
    cur = MagicMock()
    # 1) information_schema constraints
    # 2) attribute map
    # 3) pg_index rows (exprs, pred, indexdef, indkey, nulls_not_distinct)
    cur.fetchall.side_effect = [
        [
            ("users_pkey", "PRIMARY KEY", "id", 1),
            ("users_email_key", "UNIQUE", "email", 1),
            ("users_code_key", "UNIQUE", "org", 1),
            ("users_code_key", "UNIQUE", "code", 2),
        ],
        [(1, "id"), (2, "email"), (3, "org"), (4, "code")],
        [
            ("users_pkey", True, "", "", "CREATE UNIQUE INDEX ...", "1", False),
            ("users_email_key", False, "", "", "CREATE UNIQUE INDEX ...", "2", False),
            ("users_code_key", False, "", "", "CREATE UNIQUE INDEX ...", "3 4", False),
            (
                "users_email_ci",
                False,
                "lower((email)::text)",
                "",
                "CREATE UNIQUE INDEX users_email_ci ON public.users USING btree (lower((email)::text))",
                "0",
                False,
            ),
            (
                "users_active_email",
                False,
                "",
                "(status = 'active'::text)",
                "CREATE UNIQUE INDEX ... WHERE (status = 'active'::text)",
                "2",
                True,
            ),
        ],
    ]
    meta = _pg_fetch_unique_keys(cur, "public", "users")
    assert meta["primary_key_columns"] == ["id"]
    names = {u["name"]: u for u in meta["unique_keys"]}
    assert names["users_email_key"]["columns"] == ["email"]
    assert names["users_code_key"]["columns"] == ["org", "code"]
    assert names["users_email_ci"]["case_insensitive"] is True
    assert "email" in names["users_email_ci"]["expression_columns"]
    assert names["users_active_email"]["filter_predicate"] == "(status = 'active'::text)"
    assert names["users_active_email"]["nulls_not_distinct"] is True


def test_mysql_fetch_unique_keys_primary_and_unique():
    cur = MagicMock()
    cur.fetchall.return_value = [
        ("PRIMARY", "id", 1, 0, None),
        ("uq_email", "email", 1, 0, None),
    ]
    meta = _mysql_fetch_unique_keys(cur, "app", "users")
    assert meta["primary_key_columns"] == ["id"]
    assert any(
        u["name"] == "uq_email" and u["columns"] == ["email"] for u in meta["unique_keys"]
    )


def test_pg_fetch_foreign_keys_groups_columns():
    cur = MagicMock()
    cur.fetchall.return_value = [
        ("orders_customer_fkey", "customer_id", 1, "public", "customers", "id"),
    ]
    fks = _pg_fetch_foreign_keys(cur, "public", "orders")
    assert len(fks) == 1
    assert fks[0]["columns"] == ["customer_id"]
    assert fks[0]["referenced_table"] == "customers"
    assert fks[0]["referenced_columns"] == ["id"]
    assert fks[0]["referenced_schema"] == "public"


def test_mysql_fetch_foreign_keys():
    cur = MagicMock()
    cur.fetchall.return_value = [
        ("fk_ord_cust", "customer_id", 1, "app", "customers", "id"),
    ]
    fks = _mysql_fetch_foreign_keys(cur, "app", "orders")
    assert len(fks) == 1
    assert fks[0]["name"] == "fk_ord_cust"
    assert fks[0]["referenced_table"] == "customers"


def test_sqlserver_fetch_unique_keys():
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = [
        ("PK_users", True, "id", 1, None, None),
        ("UQ_users_email", False, "email", 1, None, None),
        ("UQ_email_ci", False, "email_lower", 1, "(lower([email]))", "([active]=(1))"),
    ]
    meta = _sqlserver_fetch_unique_keys(conn, "dbo", "users")
    assert meta["primary_key_columns"] == ["id"]
    names = {u["name"]: u for u in meta["unique_keys"]}
    assert "UQ_users_email" in names
    assert names["UQ_email_ci"]["case_insensitive"] is True
    assert names["UQ_email_ci"]["filter_predicate"] == "([active]=(1))"


def test_oracle_fetch_unique_keys():
    conn = MagicMock()
    conn.execute.return_value.fetchall.side_effect = [
        [
            ("SYS_C001", "P", "ID", 1),
            ("UQ_EMAIL", "U", "EMAIL", 1),
        ],
        [],
    ]
    meta = _oracle_fetch_unique_keys(conn, "APP", "USERS")
    assert meta["primary_key_columns"] == ["ID"]
    assert any(u["name"] == "UQ_EMAIL" and u["columns"] == ["EMAIL"] for u in meta["unique_keys"])


def test_expression_unique_forces_casefold():
    assert parse_case_insensitive_index_expression("lower((email)::text)") == ["email"]
    assert parse_case_insensitive_index_expression("(org, code)") == []
    uks = [
        {
            "name": "users_email_ci",
            "columns": [],
            "expression_columns": ["email"],
            "case_insensitive": True,
        }
    ]
    assert unique_key_forces_casefold("email", ddl_type="VARCHAR", unique_keys=uks) is True
    assert unique_key_forces_casefold("org", ddl_type="VARCHAR", unique_keys=uks) is False
    assert unique_equality_key("Abc", "VARCHAR", force_casefold=True) == unique_equality_key(
        "abc", "VARCHAR", force_casefold=True
    )


def test_partial_unique_filter_and_nulls_not_distinct():
    assert row_matches_unique_filter({"status": "active"}, "(status = 'active'::text)")
    assert not row_matches_unique_filter({"status": "archived"}, "(status = 'active'::text)")
    assert row_matches_unique_filter({"email": "a@b.c"}, "email IS NOT NULL")
    assert not row_matches_unique_filter({"email": None}, "(email IS NOT NULL)")
    uks = [
        {
            "name": "uq_active_email",
            "columns": ["email"],
            "filter_predicate": "(status = 'active'::text)",
            "nulls_not_distinct": True,
        }
    ]
    assert unique_key_row_in_scope(
        {"email": "a", "status": "active"}, "email", unique_keys=uks
    )
    assert not unique_key_row_in_scope(
        {"email": "a", "status": "archived"}, "email", unique_keys=uks
    )
    assert unique_key_nulls_collide("email", unique_keys=uks) is True
    assert unique_equality_key(None, null_sentinel="\x00NULL\x00") == "\x00NULL\x00"


def test_integrity_blocks_ci_expression_unique():
    from services.data_integrity import _check_duplicate_keys

    result = _check_duplicate_keys(
        [{"source": "email", "target": "email"}],
        [{"email": "Abc"}, {"email": "abc"}],
        "strict",
        dest_kind="postgresql",
        primary_key="email",
        sync_mode="append",
        destination_unique_keys=[
            {
                "name": "users_email_ci",
                "columns": [],
                "expression_columns": ["email"],
                "case_insensitive": True,
            }
        ],
        target_types={"email": "VARCHAR(100)"},
    )
    assert result["passed"] is False
    assert result["blocks_transfer"] is True


def test_integrity_partial_unique_ignores_out_of_filter_dupes():
    from services.data_integrity import _check_duplicate_keys

    result = _check_duplicate_keys(
        [{"source": "email", "target": "email"}],
        [
            {"email": "same@x.com", "status": "active"},
            {"email": "same@x.com", "status": "archived"},
            {"email": "same@x.com", "status": "archived"},
        ],
        "strict",
        dest_kind="postgresql",
        primary_key="email",
        sync_mode="append",
        destination_unique_keys=[
            {
                "name": "uq_active_email",
                "columns": ["email"],
                "filter_predicate": "(status = 'active'::text)",
            }
        ],
        target_types={"email": "VARCHAR(100)"},
    )
    assert result["passed"] is True


def test_integrity_nulls_not_distinct_blocks_multi_null():
    from services.data_integrity import _check_duplicate_keys

    result = _check_duplicate_keys(
        [{"source": "code", "target": "code"}],
        [{"code": None}, {"code": ""}, {"code": "x"}],
        "strict",
        dest_kind="postgresql",
        primary_key="code",
        sync_mode="append",
        destination_unique_keys=[
            {
                "name": "uq_code",
                "columns": ["code"],
                "nulls_not_distinct": True,
            }
        ],
        target_types={"code": "VARCHAR(40)"},
    )
    assert result["passed"] is False
    assert result["blocks_transfer"] is True


def test_composite_unique_allows_same_code_different_org():
    """UNIQUE(org, code) must not invent single-column uniqueness on code."""
    from services.data_integrity import _check_duplicate_keys

    result = _check_duplicate_keys(
        [
            {"source": "org", "target": "org"},
            {"source": "code", "target": "code"},
        ],
        [
            {"org": "1", "code": "A"},
            {"org": "2", "code": "A"},
        ],
        "strict",
        dest_kind="postgresql",
        primary_key="code",
        sync_mode="append",
        destination_unique_keys=[
            {"name": "uq_org_code", "columns": ["org", "code"]},
        ],
        target_types={"org": "VARCHAR(10)", "code": "VARCHAR(10)"},
    )
    assert result["passed"] is True


def test_composite_unique_blocks_full_tuple_dupes():
    from services.data_integrity import _check_duplicate_keys

    result = _check_duplicate_keys(
        [
            {"source": "org", "target": "org"},
            {"source": "code", "target": "code"},
            {"source": "id", "target": "id"},
        ],
        [
            {"org": "1", "code": "A", "id": "1"},
            {"org": "1", "code": "A", "id": "2"},
        ],
        "strict",
        dest_kind="postgresql",
        primary_key="id",
        sync_mode="append",
        destination_pk_columns=["id"],
        destination_unique_keys=[
            {"name": "uq_org_code", "columns": ["org", "code"]},
        ],
        target_types={
            "org": "VARCHAR(10)",
            "code": "VARCHAR(10)",
            "id": "INTEGER",
        },
    )
    assert result["passed"] is False
    assert any("uq_org_code" in i or "UNIQUE" in i for i in result["issues"])
