"""Postgres column probe finds tables outside the requested schema."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from services.schema_introspect import _introspect_postgresql


def test_pg_introspect_finds_table_in_alternate_schema():
    """UI schema may be blank/wrong; jobs often lives in public while form says railway."""
    cur = MagicMock()
    # _pg_fetch_columns unpacks ≥5 fields (name, dtype, nullable, identity, default, …).
    _col = lambda name, dtype: (name, dtype, "YES", "", None, "", True, "", 0, "")
    responses = [
        [],  # list tables in schema "railway"
        [],  # _pg_fetch_columns in "railway"/"jobs"
        [("public", "jobs")],  # cross-schema hit
        [_col("id", "text"), _col("title", "text")],  # columns
    ]

    def _fetchall():
        if responses:
            return responses.pop(0)
        return []  # unique / FK / enum follow-ups

    cur.fetchall.side_effect = _fetchall
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = False

    with patch(
        "connectors.postgresql_conn.get_connection",
        return_value=conn,
    ), patch(
        "services.schema_introspect._refine_columns_by_samples",
        side_effect=lambda _c, cols, *_a: cols,
    ):
        result = _introspect_postgresql(
            host="h",
            port=5432,
            database="railway",
            username="u",
            password="p",
            schema="railway",
            connection_string="",
            ssl=True,
            table="jobs",
        )

    assert result["ok"] is True
    assert result["schema"] == "public"
    assert [c["name"] for c in result["columns"]] == ["id", "title"]
