"""UNIQUE / PK catalog helpers — unit-level (no live DB required)."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.schema_introspect import (  # noqa: E402
    _fetch_foreign_keys,
    _mysql_fetch_unique_keys,
    _oracle_fetch_unique_keys,
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
    assert "index_valid" not in names["users_email_key"]
    assert "index_ready" not in names["users_email_key"]


def test_pg_invalid_unique_index_stays_visible_and_splits_the_write_rule():
    """``indisvalid`` false is not existing-row proof.

    A seven-column row does not invent the bits. ``indisready`` false does
    not reject a new duplicate. ``indisready`` true still does.
    """
    from services.data_integrity import _check_duplicate_keys, _unique_constraint_enforced
    from services.unique_key_introspect import (
        postgres_uniqueness_proof,
        read_postgres_uniqueness_rows,
    )

    assert postgres_uniqueness_proof(
        [
            ("users_pkey", "id", True, True),
            ("users_email_invalid", "email", False, True),
            ("users_email_also", "email", True, True),
        ]
    ) == {frozenset({"id"}): "", frozenset({"email"}): ""}
    assert postgres_uniqueness_proof(
        [("users_email_invalid", "email", False, True)]
    ) == {frozenset({"email"}): "not_checked"}
    assert postgres_uniqueness_proof(
        [("users_email_building", "email", False, False)]
    ) == {frozenset({"email"}): "not_ready"}
    assert postgres_uniqueness_proof(
        [("users_pkey", "id")]
    ) == {frozenset({"id"}): "unreported"}
    assert postgres_uniqueness_proof(
        [("users_active_email", "email", True, True, "(status = 'active'::text)")]
    ) == {frozenset({"email"}): "partial"}
    assert postgres_uniqueness_proof(
        [("users_email_key", "email", True, True)]
    ) == {frozenset({"email"}): ""}
    assert postgres_uniqueness_proof(
        [
            ("users_active_email", "email", True, True, "(status = 'active'::text)"),
            ("users_email_key", "email", True, True, None),
        ]
    ) == {frozenset({"email"}): ""}

    cur = MagicMock()
    cur.fetchall.side_effect = [
        [],
        [(1, "email")],
        [
            (
                "users_email_invalid",
                False,
                "",
                "",
                "CREATE UNIQUE INDEX users_email_invalid ON users (email)",
                "1",
                False,
                False,
                True,
            )
        ],
    ]
    meta = _pg_fetch_unique_keys(cur, "public", "users")
    key = meta["unique_keys"][0]
    assert key["name"] == "users_email_invalid"
    assert key["columns"] == ["email"]
    assert key["index_valid"] is False
    assert "index_ready" not in key
    assert "enforced" not in key
    sql = str(cur.execute.call_args_list[-1].args[0]).lower()
    assert "i.indisvalid" in sql
    assert "i.indisready" in sql
    assert "and i.indisvalid" not in sql

    not_ready = MagicMock()
    not_ready.fetchall.side_effect = [
        [],
        [(1, "email")],
        [
            (
                "users_email_building",
                False,
                "",
                "",
                "CREATE UNIQUE INDEX users_email_building ON users (email)",
                "1",
                False,
                False,
                False,
            )
        ],
    ]
    building = _pg_fetch_unique_keys(not_ready, "public", "users")["unique_keys"][0]
    assert building["index_ready"] is False
    assert building["enforced"] is False

    class _Broken:
        def execute(self, sql, params=()):
            raise RuntimeError("pg_index unavailable")

    assert read_postgres_uniqueness_rows(_Broken(), "public", "users") is None
    proof_conn = MagicMock()
    proof_conn.execute.return_value.fetchall.return_value = []
    assert read_postgres_uniqueness_rows(proof_conn, "", "users") == []
    proof_sql = str(proof_conn.execute.call_args.args[0]).lower()
    assert "pg_get_expr(i.indpred" in proof_sql
    assert "and i.indisvalid" not in proof_sql

    assert _unique_constraint_enforced(
        {"name": "users_email_invalid", "columns": ["email"], "index_valid": False},
        dest_kind="postgresql",
    ) is True
    blocked = _check_duplicate_keys(
        [{"source": "email", "target": "email"}],
        [{"email": "a"}, {"email": "a"}],
        "strict",
        dest_kind="postgres",
        primary_key="id",
        sync_mode="append",
        destination_unique_keys=[
            {
                "name": "users_email_invalid",
                "columns": ["email"],
                "index_valid": False,
            }
        ],
        target_types={"email": "text"},
    )
    assert blocked["passed"] is False
    assert blocked["blocks_transfer"] is True
    assert any("indisvalid" in warning for warning in blocked["warnings"])

    assert _unique_constraint_enforced(
        {
            "name": "users_email_building",
            "columns": ["email"],
            "index_valid": False,
            "index_ready": False,
        },
        dest_kind="azure_postgres",
    ) is False
    warned = _check_duplicate_keys(
        [{"source": "email", "target": "email"}],
        [{"email": "a"}, {"email": "a"}],
        "strict",
        dest_kind="azure_postgres",
        primary_key="id",
        sync_mode="append",
        destination_unique_keys=[
            {
                "name": "users_email_building",
                "columns": ["email"],
                "index_ready": False,
                "index_valid": False,
                "enforced": False,
            }
        ],
        target_types={"email": "text"},
    )
    assert warned["passed"] is True
    assert warned["blocks_transfer"] is False
    assert any("indisready" in warning for warning in warned["warnings"])


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
    # pg_constraint row: name, col, ref schema, ref table, ref col,
    # confdeltype, confupdtype, convalidated, ordinal.
    cur.fetchall.return_value = [
        ("orders_customer_fkey", "customer_id", "public", "customers", "id", "c", "a", False, 1),
    ]
    fks, meta = _fetch_foreign_keys("postgresql", cur, "public", "orders")
    assert meta["status"] == "measured"
    assert fks[0]["on_delete"] == "CASCADE"
    assert len(fks) == 1
    assert fks[0]["columns"] == ["customer_id"]
    assert fks[0]["referenced_table"] == "customers"
    assert fks[0]["referenced_columns"] == ["id"]
    assert fks[0]["referenced_schema"] == "public"
    assert fks[0]["validated"] is False


def test_mysql_fetch_foreign_keys():
    cur = MagicMock()
    cur.fetchall.return_value = [
        ("fk_ord_cust", "customer_id", "app", "customers", "id", "CASCADE", "NO ACTION"),
    ]
    fks, meta = _fetch_foreign_keys("mysql", cur, "app", "orders")
    assert meta["status"] == "measured"
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


def test_sqlserver_disabled_unique_index_does_not_block():
    """``is_disabled`` is not invented for a six-column row. 1 is not a write block."""
    from services.data_integrity import _check_duplicate_keys, _unique_constraint_enforced
    from services.unique_key_introspect import (
        read_sqlserver_uniqueness_rows,
        sqlserver_uniqueness_proof,
    )

    assert sqlserver_uniqueness_proof(
        [("UQ_EMAIL", False, "EMAIL", 1, None, None, 1)]
    ) == {frozenset({"email"}): "not_checked"}
    assert sqlserver_uniqueness_proof(
        [("PK_users", True, "id", 1, None, None, 0)]
    ) == {frozenset({"id"}): ""}
    assert sqlserver_uniqueness_proof(
        [("PK_users", True, "id", 1, None, None)]
    ) == {frozenset({"id"}): "unreported"}
    assert sqlserver_uniqueness_proof(
        [("UQ_active", False, "email", 1, None, "([active]=(1))", 0)]
    ) == {frozenset({"email"}): "partial"}
    assert sqlserver_uniqueness_proof(
        [
            ("UQ_active", False, "email", 1, None, "([active]=(1))", 0),
            ("UQ_email", False, "email", 1, None, None, 0),
        ]
    ) == {frozenset({"email"}): ""}
    assert sqlserver_uniqueness_proof(
        [
            ("UQ_active", False, "email", 1, None, "([active]=(1))", 0),
            ("UQ_off", False, "email", 1, None, None, 1),
        ]
    ) == {frozenset({"email"}): "partial"}

    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = [
        ("UQ_EMAIL", False, "email", 1, None, None, 1),
    ]
    meta = _sqlserver_fetch_unique_keys(conn, "dbo", "users")
    key = meta["unique_keys"][0]
    assert key["disabled"] is True
    assert key["enforced"] is False
    assert "is_disabled" in str(conn.execute.call_args.args[0]).lower()

    enabled = MagicMock()
    enabled.execute.return_value.fetchall.return_value = [
        ("PK_users", True, "id", 1, None, None),
    ]
    plain = _sqlserver_fetch_unique_keys(enabled, "dbo", "users")
    assert "disabled" not in plain["unique_keys"][0]
    assert "enforced" not in plain["unique_keys"][0]

    class _Broken:
        def execute(self, sql, params=()):
            raise RuntimeError("is_disabled unavailable")

    assert read_sqlserver_uniqueness_rows(_Broken(), "dbo", "users") is None
    assert _sqlserver_fetch_unique_keys(_Broken(), "dbo", "users") == {
        "primary_key_columns": [],
        "unique_keys": [],
    }

    assert _unique_constraint_enforced(
        {"name": "UQ_EMAIL", "columns": ["email"], "disabled": True},
        dest_kind="sqlserver",
    ) is False
    warned = _check_duplicate_keys(
        [{"source": "email", "target": "EMAIL"}],
        [{"email": "a"}, {"email": "a"}],
        "strict",
        dest_kind="mssql",
        primary_key="id",
        sync_mode="append",
        destination_unique_keys=[
            {"name": "UQ_EMAIL", "columns": ["EMAIL"], "disabled": True}
        ],
        target_types={"EMAIL": "VARCHAR"},
    )
    assert warned["passed"] is True
    assert warned["blocks_transfer"] is False
    assert any("disabled" in warning for warning in warned["warnings"])


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


def test_integrity_partial_unique_blocks_in_filter_dupes():
    from services.data_integrity import _check_duplicate_keys

    result = _check_duplicate_keys(
        [{"source": "email", "target": "email"}],
        [
            {"email": "same@x.com", "status": "active"},
            {"email": "same@x.com", "status": "active"},
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
    assert result["passed"] is False
    assert result["blocks_transfer"] is True


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


def test_sqlite_partial_unique_where_limits_the_write_block(tmp_path: Path) -> None:
    """A SQLite partial unique index blocks only rows that match its WHERE.

    Proved against a real sqlite_master catalog, then through the duplicate
    probe. A full unique on the same columns still blocks the other rows.
    """
    import sqlite3

    from services.data_integrity import _check_duplicate_keys
    from services.unique_key_introspect import (
        _sqlite_fetch_unique_keys,
        _sqlite_index_where,
    )

    where_in_literal = (
        "CREATE UNIQUE INDEX u ON t (email) "
        "WHERE note != 'x) WHERE y' AND status = 'active'"
    )
    assert _sqlite_index_where(where_in_literal) == (
        "note != 'x) WHERE y' AND status = 'active'"
    )
    assert _sqlite_index_where("CREATE UNIQUE INDEX u ON t (email)") == ""

    db = tmp_path / "partial.db"
    con = sqlite3.connect(db)
    try:
        con.execute(
            "CREATE TABLE people (id INTEGER PRIMARY KEY, email TEXT, status TEXT)"
        )
        con.execute(
            "CREATE UNIQUE INDEX ux_email ON people (email) "
            "WHERE status = 'active'"
        )
        con.execute("CREATE UNIQUE INDEX ux_status ON people (status)")
        cur = con.cursor()
        info = list(cur.execute('PRAGMA table_info("people")'))
        meta = _sqlite_fetch_unique_keys(cur, '"people"', info)
    finally:
        con.close()

    names = {u["name"]: u for u in meta["unique_keys"]}
    assert names["ux_email"]["filter_predicate"] == "status = 'active'"
    assert names["ux_email"]["enforced"] is True
    assert names["ux_email"]["columns"] == ["email"]
    assert names["ux_status"]["filter_predicate"] == ""
    assert names["PRIMARY"]["filter_predicate"] == ""

    partial_only = [names["ux_email"]]
    out_of_filter = _check_duplicate_keys(
        [
            {"source": "email", "target": "email"},
            {"source": "status", "target": "status"},
        ],
        [
            {"email": "same@x.com", "status": "archived"},
            {"email": "same@x.com", "status": "archived"},
        ],
        "strict",
        dest_kind="sqlite",
        primary_key="email",
        sync_mode="append",
        destination_unique_keys=partial_only,
        target_types={"email": "TEXT", "status": "TEXT"},
    )
    assert out_of_filter["passed"] is True
    in_filter = _check_duplicate_keys(
        [
            {"source": "email", "target": "email"},
            {"source": "status", "target": "status"},
        ],
        [
            {"email": "same@x.com", "status": "active"},
            {"email": "same@x.com", "status": "active"},
        ],
        "strict",
        dest_kind="sqlite",
        primary_key="email",
        sync_mode="append",
        destination_unique_keys=partial_only,
        target_types={"email": "TEXT", "status": "TEXT"},
    )
    assert in_filter["passed"] is False
    assert in_filter["blocks_transfer"] is True

    sibling = _check_duplicate_keys(
        [
            {"source": "email", "target": "email"},
            {"source": "status", "target": "status"},
        ],
        [
            {"email": "same@x.com", "status": "archived"},
            {"email": "same@x.com", "status": "archived"},
        ],
        "strict",
        dest_kind="sqlite",
        primary_key="email",
        sync_mode="append",
        destination_unique_keys=[
            names["ux_email"],
            {"name": "uq_email", "columns": ["email"], "filter_predicate": ""},
        ],
        target_types={"email": "TEXT", "status": "TEXT"},
    )
    assert sibling["passed"] is False
    assert sibling["blocks_transfer"] is True


def test_sqlite_partial_bit_without_where_stays_in_the_duplicate_probe() -> None:
    """Partial flag with no stored WHERE does not drop the write block."""
    from services.unique_key_introspect import _sqlite_fetch_unique_keys

    cur = MagicMock()
    cur.fetchall.side_effect = [
        [(0, "ux", 1, "c", 1)],
        [(0, 0, "email")],
    ]
    cur.fetchone.return_value = (None,)
    info_rows = [(0, "email", "TEXT", 0, None, 0)]
    meta = _sqlite_fetch_unique_keys(cur, '"people"', info_rows)
    names = {u["name"]: u for u in meta["unique_keys"]}
    assert names["ux"]["filter_predicate"] == ""
    assert names["ux"]["enforced"] is True
    executed = " ".join(str(call.args[0]) for call in cur.execute.call_args_list)
    assert "sqlite_master" in executed
