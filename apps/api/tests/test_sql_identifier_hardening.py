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
    # A GCP project ID's hyphen is part of the name (lowercase letters, digits,
    # hyphens: cloud.google.com/resource-manager/docs/creating-managing-projects);
    # ``my_proj`` would be another, invalid project (D314).
    assert ref == "`my-proj.raw.events`"
    assert "DROP" not in ref
    # Anything that is not a project ID is still sanitized segment by segment.
    hostile = quote_table_ref(
        "events", dialect="bigquery", project="my-proj`; DROP TABLE x; --", dataset="raw"
    )
    assert hostile.count("`") == 2
    assert ";" not in hostile and " " not in hostile


def test_quote_table_ref_bigquery_keeps_a_legal_table_name_as_spelled() -> None:
    """D314 for tables: BigQuery table names may hold Unicode letters, marks,
    numbers, connectors (_), dashes (-) and spaces, up to 1024 UTF-8 bytes
    (cloud.google.com/bigquery/docs/tables#table_naming). Rewriting
    ``daily-events`` to ``daily_events`` read a different table, which may
    exist. A legal name is quoted as spelled; a dataset name stays letters,
    digits and underscores (cloud.google.com/bigquery/docs/datasets#dataset-naming)."""
    for name in ("daily-events", "daily events", "étudiant-01", "ग्राहक", "00_お客様", "table 01"):
        assert quote_table_ref(name, dialect="bigquery", project="my-proj", dataset="raw") == (
            f"`my-proj.raw.{name}`"
        )
    # The dataset may also come with the table: only the table segment is a table name.
    assert quote_table_ref("raw.daily-events", dialect="bigquery", project="my-proj") == (
        "`my-proj.raw.daily-events`"
    )
    # Characters a table name cannot hold are never passed through into the SQL.
    hostile = quote_table_ref("a`b; DROP TABLE x", dialect="bigquery", project="my-proj", dataset="raw")
    assert hostile.count("`") == 2 and ";" not in hostile
    too_long = "t" * 1025
    assert quote_table_ref(too_long, dialect="bigquery", project="my-proj", dataset="raw") != (
        f"`my-proj.raw.{too_long}`"
    )


def test_bigquery_reader_queries_the_hyphenated_table_it_was_given(monkeypatch) -> None:
    """The reader's SQL names the table the operator picked, not a look-alike."""
    pytest.importorskip("google.cloud.bigquery")
    import connectors.bigquery_conn as bq_conn
    import connectors.bigquery_reader as reader

    seen: list[str] = []

    class _Client:
        def get_table(self, ref):
            raise RuntimeError("stop after the reference is built")

        def query(self, sql, *a, **kw):
            seen.append(sql)
            raise RuntimeError("stop after the query is built")

    monkeypatch.setattr(bq_conn, "get_client", lambda **_kw: _Client())
    with pytest.raises(Exception):
        reader.read_table_batch(
            host="", port=0, database="my-proj", username="", password="", schema="raw",
            connection_string="", ssl=False, table="daily-events", columns=["id"], limit=5,
        )
    assert seen and all("`my-proj.raw.daily-events`" in sql for sql in seen), seen
    assert not any("daily_events" in sql for sql in seen), seen


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


def test_a_spaced_sqlite_table_is_read_under_its_own_name(tmp_path) -> None:
    import sqlite3

    from services.preflight_sample import engine_sample_rows

    path = tmp_path / "spaced.db"
    conn = sqlite3.connect(path)
    conn.execute('create table "order details"(id integer primary key, name text)')
    conn.execute("insert into \"order details\" values (1, 'bolt')")
    conn.commit()
    conn.close()
    sample = engine_sample_rows(
        source_kind="database",
        source_format="sqlite",
        source_config={"type": "sqlite", "database": str(path)},
        source_table="order details",
        limit=10,
    )
    assert sample.unavailable_reason == ""
    assert sample.rows == [{"id": "1", "name": "bolt"}]


def test_a_space_in_a_table_name_stays_inside_the_quotes() -> None:
    """DEF-C-047: sanitizing the space addressed ``Order_Details``, which does not exist."""
    assert quote_table_ref("qa6c7_Order Details", "public", dialect="postgresql") == (
        '"public"."qa6c7_Order Details"'
    )
    assert quote_table_ref("qa6c7_Order Details", "dbo", dialect="sqlserver") == (
        "[dbo].[qa6c7_Order Details]"
    )
    assert quote_table_ref("qa6c7_Order Details", dialect="mysql") == "`qa6c7_Order Details`"
    assert quote_table_ref("qa6c7_Order Details", dialect="sqlite") == '"qa6c7_Order Details"'


def test_malicious_table_not_in_from_clause_shape() -> None:
    """Simulates the SELECT COUNT shape used by reconciliation / readers."""
    ref = quote_table_ref(MALICIOUS, "public", dialect="postgresql")
    sql = f"SELECT COUNT(*) FROM {ref}"
    assert "DROP TABLE" not in sql
    assert ";" not in sql
    assert sql == 'SELECT COUNT(*) FROM "public"."orders___DROP_TABLE_users___"'
