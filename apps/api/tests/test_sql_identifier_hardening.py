"""Scenario tests: malicious identifiers are sanitized/quoted, never raw SQL fragments."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.sql_identifiers import (
    quote_column_list,
    quote_sql_identifier,
    quote_table_ref,
    require_safe_identifier,
    sanitize_identifier,
)


MALICIOUS = 'orders"; DROP TABLE users;--'


def test_sanitize_strips_injection_payload() -> None:
    cleaned = sanitize_identifier(MALICIOUS)
    assert ";" not in cleaned
    assert "DROP" not in cleaned.upper() or "drop" in cleaned  # may become drop as word fragment
    assert '"' not in cleaned
    assert cleaned == "orders_drop_table_users"


def test_quote_table_ref_mysql_never_embeds_raw_payload() -> None:
    ref = quote_table_ref(MALICIOUS, dialect="mysql")
    assert "DROP TABLE" not in ref
    assert ";" not in ref
    assert '"' not in ref
    assert ref.startswith("`") and ref.endswith("`")
    assert "orders_DROP_TABLE_users" in ref


def test_quote_table_ref_postgresql_schema_table() -> None:
    ref = quote_table_ref("orders", "public", dialect="postgresql")
    assert ref == '"public"."orders"'


def test_quote_table_ref_snowflake() -> None:
    ref = quote_table_ref("ORDERS", "PUBLIC", dialect="snowflake")
    assert ref == '"PUBLIC"."ORDERS"'


def test_quote_table_ref_bigquery() -> None:
    ref = quote_table_ref(
        "events",
        dialect="bigquery",
        project="my-proj",
        dataset="raw",
    )
    # hyphen in project is sanitized to underscore
    assert ref == "`my_proj.raw.events`"
    assert "DROP" not in ref


def test_quote_table_ref_rejects_empty() -> None:
    with pytest.raises(ValueError):
        quote_table_ref("", dialect="mysql")


def test_require_safe_identifier_rejects_null_bytes_when_raw() -> None:
    with pytest.raises(ValueError):
        require_safe_identifier("bad\x00name", allow_raw=True)


def test_quote_column_list_escapes_quotes() -> None:
    cols = quote_column_list(['a"b', "c"], quote_char='"')
    assert cols == '"a""b", "c"'


def test_quote_sql_identifier_mysql_backticks() -> None:
    assert quote_sql_identifier("a`b", "`") == "`a``b`"


def test_malicious_table_not_in_from_clause_shape() -> None:
    """Simulates the SELECT COUNT shape used by reconciliation / readers."""
    ref = quote_table_ref(MALICIOUS, "public", dialect="postgresql")
    sql = f"SELECT COUNT(*) FROM {ref}"
    assert "DROP TABLE" not in sql
    assert ";" not in sql
    assert sql == 'SELECT COUNT(*) FROM "public"."orders_DROP_TABLE_users"'
