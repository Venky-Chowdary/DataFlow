"""Gate-8's keyed read-back must survive a source/destination key type mismatch.

The keyed ``IN (...)`` read binds source key values, so their Python type follows
the *source* column. When the destination stores that key as text — routine for
create-new onto document and vector targets, and for any mapping that widens an
integer id to a string — PostgreSQL refuses ``text = integer`` outright rather
than coercing. The sample then came back as "could not read destination sample",
and a write that had landed correctly was reported as unproven.
"""

from __future__ import annotations

import socket
import uuid

import pytest

from services.keyed_read import is_operand_type_mismatch
from services.target_sample import read_target_sample


def _pg_up() -> bool:
    try:
        with socket.create_connection(("localhost", 5432), timeout=1):
            return True
    except OSError:
        return False


def test_operand_mismatch_is_recognised():
    assert is_operand_type_mismatch(Exception("operator does not exist: text = integer"))
    assert is_operand_type_mismatch(Exception("could not identify an equality operator"))
    # An unrelated failure must still propagate rather than trigger a retry.
    assert not is_operand_type_mismatch(Exception('relation "orders" does not exist'))


@pytest.mark.skipif(not _pg_up(), reason="PostgreSQL not reachable on localhost:5432")
def test_keyed_read_back_survives_int_keys_against_text_key_column():
    import psycopg2

    table = "textkey_" + uuid.uuid4().hex[:8]
    conn = psycopg2.connect(
        host="localhost", port=5432, dbname="dataflow", user="dataflow", password="dataflow"
    )
    conn.autocommit = True
    try:
        cur = conn.cursor()
        cur.execute(f'CREATE TABLE public."{table}" (id TEXT PRIMARY KEY, amount NUMERIC)')
        cur.execute(f"""INSERT INTO public."{table}" VALUES ('1', 10.5), ('2', 20.25)""")

        rows = read_target_sample(
            "postgresql",
            {
                "host": "localhost",
                "port": 5432,
                "database": "dataflow",
                "username": "dataflow",
                "password": "dataflow",
            },
            schema="public",
            table_name=table,
            columns=["id", "amount"],
            limit=10,
            sort_key="id",
            key_values=[1, 2],  # integers, as the source declared them
        )
    finally:
        try:
            conn.cursor().execute(f'DROP TABLE IF EXISTS public."{table}"')
        finally:
            conn.close()

    assert [r["id"] for r in rows] == ["1", "2"]
    assert len(rows) == 2, rows
