"""Snowflake UNIQUE catalog + Oracle GENERATED/NLSSORT — enterprise SSOT."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.schema_introspect import _snowflake_fetch_unique_keys  # noqa: E402
from services.type_system import (  # noqa: E402
    is_generated_always_column,
    parse_case_insensitive_index_expression,
    row_matches_unique_filter,
    unique_key_forces_casefold,
)


def test_snowflake_fetch_unique_keys_hybrid_enforced():
    cur = MagicMock()
    cur.fetchall.return_value = [
        ("SYS_PK", "PRIMARY KEY", "ID", 1, "YES"),
        ("UQ_EMAIL", "UNIQUE", "EMAIL", 1, "YES"),
    ]
    meta = _snowflake_fetch_unique_keys(cur, "PUBLIC", "USERS")
    assert meta["primary_key_columns"] == ["ID"]
    names = {u["name"]: u for u in meta["unique_keys"]}
    assert names["UQ_EMAIL"]["enforced"] is True
    assert names["UQ_EMAIL"]["columns"] == ["EMAIL"]


def test_snowflake_not_enforced_flagged():
    cur = MagicMock()
    cur.fetchall.return_value = [
        ("UQ_EMAIL", "UNIQUE", "EMAIL", 1, "NO"),
    ]
    meta = _snowflake_fetch_unique_keys(cur, "PUBLIC", "USERS")
    assert meta["unique_keys"][0]["enforced"] is False


def test_integrity_skips_snowflake_not_enforced_unique():
    from services.data_integrity import _check_duplicate_keys

    result = _check_duplicate_keys(
        [{"source": "email", "target": "EMAIL"}],
        [{"email": "a"}, {"email": "a"}],
        "strict",
        dest_kind="snowflake",
        primary_key="email",
        sync_mode="append",
        destination_unique_keys=[
            {
                "name": "UQ_EMAIL",
                "columns": ["EMAIL"],
                "enforced": False,
            }
        ],
        target_types={"EMAIL": "VARCHAR"},
    )
    # Advisory UNIQUE must not invent a transfer block.
    assert result["passed"] is True


def test_integrity_does_not_block_on_a_stamped_snowflake_enforced_flag():
    """Dialect name does not say hybrid. A stamped True is not a write block."""
    from services.data_integrity import _check_duplicate_keys

    result = _check_duplicate_keys(
        [{"source": "email", "target": "EMAIL"}],
        [{"email": "a"}, {"email": "a"}],
        "strict",
        dest_kind="snowflake",
        primary_key="id",
        sync_mode="append",
        destination_unique_keys=[
            {
                "name": "UQ_EMAIL",
                "columns": ["EMAIL"],
                "enforced": True,
            }
        ],
        target_types={"EMAIL": "VARCHAR", "ID": "INTEGER"},
    )
    assert result["passed"] is True


def test_measured_hybrid_table_blocks_a_duplicate_enforced_key():
    """IS_HYBRID YES plus ENFORCED YES is a write rule. Either fact alone is not."""
    from services.data_integrity import _check_duplicate_keys

    rows = [{"email": "a"}, {"email": "a"}]
    mappings = [{"source": "email", "target": "EMAIL"}]
    types = {"EMAIL": "VARCHAR", "ID": "INTEGER"}

    def _run(**key):
        return _check_duplicate_keys(
            mappings,
            rows,
            "strict",
            dest_kind="snowflake_aws",
            primary_key="id",
            sync_mode="append",
            destination_unique_keys=[
                {
                    "name": "UQ_EMAIL",
                    "columns": ["EMAIL"],
                    "enforced": True,
                    **key,
                }
            ],
            target_types=types,
        )

    hybrid = _run(table_kind="YES")
    assert hybrid["passed"] is False
    assert hybrid["blocks_transfer"] is True
    assert any("UQ_EMAIL" in issue for issue in hybrid["issues"])

    standard = _run(table_kind="NO")
    assert standard["passed"] is True

    iceberg = _run(table_kind="iceberg")
    assert iceberg["passed"] is True

    missing_enforced = _check_duplicate_keys(
        mappings,
        rows,
        "strict",
        dest_kind="snowflake",
        primary_key="id",
        sync_mode="append",
        destination_unique_keys=[
            {"name": "UQ_EMAIL", "columns": ["EMAIL"], "table_kind": "hybrid"}
        ],
        target_types=types,
    )
    assert missing_enforced["passed"] is True

    other_engine = _check_duplicate_keys(
        mappings,
        rows,
        "strict",
        dest_kind="bigquery",
        primary_key="id",
        sync_mode="append",
        destination_unique_keys=[
            {
                "name": "UQ_EMAIL",
                "columns": ["EMAIL"],
                "enforced": True,
                "table_kind": "hybrid",
            }
        ],
        target_types=types,
    )
    assert other_engine["passed"] is True


def test_snowflake_fetch_records_is_hybrid():
    """The table-kind read is INFORMATION_SCHEMA.TABLES.IS_HYBRID, after the keys."""
    cur = MagicMock()
    cur.fetchall.side_effect = [
        [("UQ_EMAIL", "UNIQUE", "EMAIL", 1, "YES")],
        [("YES",)],
    ]
    meta = _snowflake_fetch_unique_keys(cur, "PUBLIC", "ORDERS")
    assert meta["table_kind"] == "hybrid"
    assert meta["unique_keys"][0]["table_kind"] == "hybrid"
    assert meta["unique_keys"][0]["enforced"] is True
    sql = " ".join(str(call.args[0]) for call in cur.execute.call_args_list)
    assert "is_hybrid" in sql
    assert "information_schema.tables" in sql

    failed = MagicMock()
    failed.fetchall.return_value = [("UQ_EMAIL", "UNIQUE", "EMAIL", 1, "NO")]
    failed.execute.side_effect = [
        None,
        RuntimeError("table kind columns unavailable"),
        RuntimeError("IS_HYBRID unavailable"),
    ]
    unread = _snowflake_fetch_unique_keys(failed, "PUBLIC", "ORDERS")
    assert unread["table_kind"] == ""
    assert "table_kind" not in unread["unique_keys"][0]
    assert unread["unique_keys"][0]["enforced"] is False


def test_oracle_nlssort_binary_ci_forces_casefold():
    expr = "NLSSORT(\"EMAIL\",'NLS_SORT=BINARY_CI')"
    assert parse_case_insensitive_index_expression(expr) == ["EMAIL"]
    uks = [
        {
            "name": "UQ_EMAIL_CI",
            "columns": [],
            "expression_columns": ["EMAIL"],
            "case_insensitive": True,
        }
    ]
    assert unique_key_forces_casefold("EMAIL", unique_keys=uks) is True


def test_oracle_virtual_identity_generated_tokens():
    assert is_generated_always_column("NUMBER GENERATED ALWAYS") is True
    assert is_generated_always_column("NUMBER GENERATED BY DEFAULT") is False


def test_partial_unique_and_conjunction():
    pred = "([active]=(1) AND [email] IS NOT NULL)"
    assert row_matches_unique_filter({"active": 1, "email": "a@b.c"}, pred)
    assert not row_matches_unique_filter({"active": 0, "email": "a@b.c"}, pred)
    assert not row_matches_unique_filter({"active": 1, "email": None}, pred)
