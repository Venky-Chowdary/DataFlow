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
    split_qualified_table,
)


MALICIOUS = 'orders"; DROP TABLE users;--'


def test_sanitize_strips_injection_payload() -> None:
    cleaned = sanitize_identifier(MALICIOUS)
    assert ";" not in cleaned
    assert "DROP" not in cleaned.upper() or "drop" in cleaned  # may become drop as word fragment
    assert '"' not in cleaned
    # Underscore runs are preserved: collapsing them merged distinct columns
    # (first__name onto first_name). The security property is unchanged —
    # the quote, the semicolon and the statement break are all gone.
    assert cleaned == "orders___drop_table_users___"


def test_quote_table_ref_mysql_never_embeds_raw_payload() -> None:
    ref = quote_table_ref(MALICIOUS, dialect="mysql")
    assert "DROP TABLE" not in ref
    assert ";" not in ref
    assert '"' not in ref
    assert ref.startswith("`") and ref.endswith("`")
    assert "orders___DROP_TABLE_users___" in ref


def test_quote_table_ref_postgresql_schema_table() -> None:
    ref = quote_table_ref("orders", "public", dialect="postgresql")
    assert ref == '"public"."orders"'


def test_split_qualified_table_does_not_double_prefix() -> None:
    """Studio stores ``public.case_a_src`` while the connector has schema=public."""
    assert split_qualified_table("public.case_a_src", "public") == ("public", "case_a_src")
    assert split_qualified_table("case_a_src", "public") == ("public", "case_a_src")
    assert split_qualified_table("other.case_a_src", "public") == ("other", "case_a_src")
    assert split_qualified_table('"public"."case_a_src"', "public") == ("public", "case_a_src")
    assert split_qualified_table('"public.case_a_src"', "public") == ("public", "public.case_a_src")
    assert split_qualified_table("case_a_src", None) == (None, "case_a_src")
    assert quote_table_ref("public.case_a_src", "public", dialect="postgresql") == (
        '"public"."case_a_src"'
    )
    assert quote_table_ref("case_a_src", "public", dialect="postgresql") == (
        '"public"."case_a_src"'
    )
    compiled_wrong = 'public."public.case_a_src"'
    assert compiled_wrong not in quote_table_ref(
        "public.case_a_src", "public", dialect="postgresql"
    )


def test_quote_table_ref_snowflake() -> None:
    ref = quote_table_ref("ORDERS", "PUBLIC", dialect="snowflake")
    assert ref == '"PUBLIC"."ORDERS"'


def test_quote_table_ref_snowflake_folds_postgres_public_default() -> None:
    """UI often stores schema=public (Postgres default). Snowflake needs PUBLIC."""
    from connectors.sql_identifiers import snowflake_fold_identifier

    assert snowflake_fold_identifier("public") == "PUBLIC"
    ref = quote_table_ref("jobs", "public", dialect="snowflake")
    assert ref == '"PUBLIC"."JOBS"'
    assert '"public"' not in ref


def test_quote_table_ref_snowflake_preserves_mixed_case() -> None:
    ref = quote_table_ref("MyTable", "MySchema", dialect="snowflake")
    assert ref == '"MySchema"."MyTable"'


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
    assert sql == 'SELECT COUNT(*) FROM "public"."orders___DROP_TABLE_users___"'
