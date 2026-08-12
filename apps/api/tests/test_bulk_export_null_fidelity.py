"""Bulk COPY export must keep SQL NULL distinct from the text that looks like it.

The keyset and OFFSET readers emit ``SQL_NULL_SENTINEL`` for SQL NULL, so the
bulk path has to agree or enabling ``DATAFLOW_BULK_EXPORT`` silently changes what
a NULL means and the checksum diverges from a keyset read of the same table.

The subtle half is a column holding the literal characters ``\\N``. COPY CSV
distinguishes that from NULL by quoting one and not the other, but ``csv.reader``
strips quoting before any of our code sees it, so both arrived identical. COPY
TEXT escapes instead of quoting, which is why the reader uses it — and it makes
the read the exact inverse of ``_copy_text_value`` on the write side.
"""

from __future__ import annotations

import socket
import uuid

import pytest

from connectors.bulk_export import _copy_cell
from services.value_serializer import SQL_NULL_SENTINEL


def _pg_up() -> bool:
    try:
        with socket.create_connection(("localhost", 5432), timeout=1):
            return True
    except OSError:
        return False


def test_unescaped_null_marker_is_sql_null():
    assert _copy_cell("\\N") == SQL_NULL_SENTINEL


def test_escaped_null_marker_is_literal_text():
    """``\\\\N`` on the wire is a column holding the characters ``\\N``."""
    assert _copy_cell("\\\\N") == "\\N"


@pytest.mark.parametrize(
    "wire,plain",
    [
        ("\\t", "\t"),
        ("\\n", "\n"),
        ("\\r", "\r"),
        ("C:\\\\temp", "C:\\temp"),
        ("plain", "plain"),
        ("", ""),
    ],
)
def test_text_escapes_are_reversed(wire, plain):
    assert _copy_cell(wire) == plain


@pytest.mark.skipif(not _pg_up(), reason="PostgreSQL not reachable on localhost:5432")
def test_copy_export_round_trips_nulls_and_escapes():
    """The only proof that matters: read the values back through COPY."""
    import psycopg2

    from connectors.bulk_export import iter_postgresql_copy_batches

    table = "bulk_null_" + uuid.uuid4().hex[:8]
    rows = [
        (1, None),
        (2, "\\N"),          # literal backslash-N, not NULL
        (3, "tab\there"),
        (4, "line\nbreak"),
        (5, "C:\\temp"),
        (6, ""),             # empty string, not NULL
    ]
    conn = psycopg2.connect(
        host="localhost", port=5432, dbname="dataflow", user="dataflow", password="dataflow"
    )
    conn.autocommit = True
    try:
        cur = conn.cursor()
        cur.execute(f'CREATE TABLE public."{table}" (id int, name text)')
        cur.executemany(f'INSERT INTO public."{table}" VALUES (%s,%s)', rows)

        pages = list(
            iter_postgresql_copy_batches(
                host="localhost", port=5432, database="dataflow",
                username="dataflow", password="dataflow", schema="public",
                connection_string="", ssl=False, table=table,
                columns=["id", "name"], batch_rows=100,
            )
        )
    finally:
        try:
            conn.cursor().execute(f'DROP TABLE IF EXISTS public."{table}"')
        finally:
            conn.close()

    got = {r[0]: r[1] for page in pages for r in page.rows}
    assert got == {
        "1": SQL_NULL_SENTINEL,
        "2": "\\N",
        "3": "tab\there",
        "4": "line\nbreak",
        "5": "C:\\temp",
        "6": "",
    }
