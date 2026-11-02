"""COPY TEXT escaping — bytea and jsonb must survive the bulk-load path.

``COPY ... WITH (FORMAT text)`` interprets backslash sequences in its input, so
any field rendered without escaping is re-read by the server as something else.
Two branches used to return unescaped text:

* bytes rendered ``\\x68656c6c6f``, which COPY consumed as the byte 0x68 plus the
  literal characters ``656c6c6f`` — b"hello" was stored as b"h656c6c6f".
* ``json.dumps`` output carried JSON's own backslash escapes, so ``C:\\temp``
  reached jsonb as ``C:<tab>emp``, or was rejected outright when the surviving
  escape was not valid JSON.

Both wrote through the upsert path while full-refresh happened to bind
parameters, so the corruption depended on sync mode.
"""

from __future__ import annotations

import io
import socket
import uuid

import pytest

from connectors.postgresql_writer import _copy_text_value


def _pg_up() -> bool:
    try:
        with socket.create_connection(("localhost", 5432), timeout=1):
            return True
    except OSError:
        return False


def test_bytes_render_an_escaped_hex_literal():
    """COPY must receive ``\\\\x…`` so the server emits the field ``\\x…``."""
    assert _copy_text_value(b"hello") == "\\\\x68656c6c6f"


def test_json_backslashes_are_escaped_for_copy():
    rendered = _copy_text_value({"path": "C:\\temp"})
    # json.dumps produces C:\\temp; COPY input must carry C:\\\\temp.
    assert "\\\\\\\\temp" in rendered


def test_tabs_and_newlines_never_break_the_row():
    assert _copy_text_value("a\tb\nc\rd") == "a\\tb\\nc\\rd"


@pytest.mark.skipif(not _pg_up(), reason="PostgreSQL not reachable on localhost:5432")
def test_copy_round_trips_bytea_json_and_text_exactly():
    """The only proof that matters: read the values back out of PostgreSQL."""
    import psycopg2

    conn = psycopg2.connect(
        host="localhost", port=5432, dbname="dataflow", user="dataflow", password="dataflow"
    )
    table = "copy_escape_" + uuid.uuid4().hex[:8]
    cases = [
        (1, b"hello", {"path": "C:\\temp\\x41"}, "plain"),
        (2, b"\x00\x01\xff", {"note": "line1\nline2\ttab"}, "back\\slash\ttab"),
        (3, b"", {"q": 'he said "hi"', "nested": {"a": [1, 2]}}, "\\x41 is not a byte here"),
    ]
    try:
        cur = conn.cursor()
        cur.execute(f'CREATE TABLE public."{table}" (id int, b bytea, j jsonb, s text)')
        buf = io.StringIO()
        for row in cases:
            buf.write("\t".join(_copy_text_value(v) for v in row) + "\n")
        buf.seek(0)
        cur.copy_expert(
            f'COPY public."{table}" (id,b,j,s) FROM STDIN '
            "WITH (FORMAT text, DELIMITER E'\\t', NULL '\\\\N')",
            buf,
        )
        cur.execute(f'SELECT id, b, j, s FROM public."{table}" ORDER BY id')
        stored = [(r[0], bytes(r[1]), r[2], r[3]) for r in cur.fetchall()]
    finally:
        conn.rollback()
        conn.close()

    assert stored == cases
