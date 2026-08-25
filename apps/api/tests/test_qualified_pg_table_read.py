"""Studio ``public.table`` must read ``public.table``, not ``public.public.table``."""

from __future__ import annotations

from connectors.postgresql_reader import _bind, count_table_rows
from connectors.sql_identifiers import split_qualified_table


def test_pg_reader_bind_does_not_double_prefix() -> None:
    assert _bind("public", "public.case_a_src") == ("public", "case_a_src")
    assert _bind("public", "case_a_src") == ("public", "case_a_src")
    assert split_qualified_table("public.case_a_dst", "public") == ("public", "case_a_dst")


def test_live_pg_count_accepts_qualified_studio_name() -> None:
    from tests.typed_fidelity_helpers import require_ports

    require_ports(5432)
    qualified = count_table_rows(
        host="127.0.0.1",
        port=5432,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        schema="public",
        connection_string="",
        ssl=False,
        table="public.case_a_src",
    )
    bare = count_table_rows(
        host="127.0.0.1",
        port=5432,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        schema="public",
        connection_string="",
        ssl=False,
        table="case_a_src",
    )
    assert qualified == bare == 3
