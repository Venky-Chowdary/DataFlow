"""Large values must arrive whole — the failure that dominates the incumbent.

AWS DMS lists incorrect LOB settings first among the causes of source/target
data mismatch, and its default is Limited LOB mode with a 32KB cap: anything
larger is truncated and the notice goes only to the task log. Its own best
practice page says truncation and rejected rows "are only written in the task
log". PostgreSQL JSON is handled as a LOB there too, so a large JSON document
truncates under the same default.

That is the shape of failure this product exists to refuse, so it is pinned
rather than asserted. The sizes below straddle that 32KB default deliberately,
and both transfer paths are covered: the server-to-server COPY used when types
are proven identical, and the row path that carries every other route.
"""

from __future__ import annotations

import socket
import uuid

import pytest

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest

#: 32KB is exactly the DMS Limited-LOB default; the others prove the cap is not
#: merely larger somewhere else.
SIZES = [(1, 32 * 1024), (2, 1024 * 1024), (3, 4 * 1024 * 1024)]


def _pg_reachable() -> bool:
    try:
        socket.create_connection(("127.0.0.1", 5432), timeout=1).close()
        return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(
    not _pg_reachable(), reason="PostgreSQL not reachable on 127.0.0.1:5432"
)


def _endpoint(table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="postgresql",
        host="127.0.0.1",
        port=5432,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        schema="public",
        table=table,
    )


@pytest.fixture()
def seeded():
    psycopg2 = pytest.importorskip("psycopg2")
    import psycopg2.extras

    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        dbname="dataflow",
        user="dataflow",
        password="dataflow",
    )
    conn.autocommit = True
    suffix = uuid.uuid4().hex[:8]
    src, dst = f"lob_src_{suffix}", f"lob_dst_{suffix}"
    with conn.cursor() as cur:
        cur.execute(
            f'CREATE TABLE "{src}" '
            "(id bigint, big_text text, big_bytes bytea, big_json jsonb)"
        )
        for row_id, size in SIZES:
            cur.execute(
                f'INSERT INTO "{src}" VALUES (%s, %s, %s, %s)',
                (
                    row_id,
                    "x" * size,
                    psycopg2.Binary(b"b" * size),
                    psycopg2.extras.Json({"pad": "y" * size}),
                ),
            )
    try:
        yield conn, src, dst
    finally:
        with conn.cursor() as cur:
            for name in (src, dst):
                cur.execute(f'DROP TABLE IF EXISTS "{name}"')
        conn.close()


def _lengths(conn, table: str):
    with conn.cursor() as cur:
        cur.execute(
            f'SELECT id, length(big_text), length(big_bytes), '  # nosec B608
            f'length(big_json::text) FROM "{table}" ORDER BY id'
        )
        return cur.fetchall()


def _run(src: str, dst: str, sync_mode: str):
    columns = ["id", "big_text", "big_bytes", "big_json"]
    return UniversalTransferEngine().execute_tracked(
        TransferRequest(
            source=_endpoint(src),
            destination=_endpoint(dst),
            mappings=[{"source": c, "target": c, "confidence": 0.99} for c in columns],
            sync_mode=sync_mode,
            validation_mode="strict",
            skip_preflight=True,
            stream_contracts=[
                {
                    "name": src,
                    "primary_key": "id",
                    "selected": True,
                    "sync_mode": sync_mode,
                }
            ],
        ),
        uuid.uuid4().hex[:24],
    )


def test_server_to_server_copy_keeps_large_values_whole(seeded):
    conn, src, dst = seeded
    result = _run(src, dst, "full_refresh_overwrite")
    assert result.success, result.error
    assert _lengths(conn, dst) == _lengths(conn, src)


def test_row_path_keeps_large_values_whole(seeded):
    """Append does not qualify for the copy path, so this exercises row carry."""
    conn, src, dst = seeded
    result = _run(src, dst, "full_refresh_append")
    assert result.success, result.error
    assert _lengths(conn, dst) == _lengths(conn, src)


def test_no_value_is_capped_at_the_limited_lob_default(seeded):
    """Stated separately because 32KB is the specific cap that bites elsewhere."""
    conn, src, dst = seeded
    assert _run(src, dst, "full_refresh_overwrite").success
    landed = {row[0]: row for row in _lengths(conn, dst)}
    for row_id, size in SIZES:
        assert landed[row_id][1] == size, "text truncated"
        assert landed[row_id][2] == size, "binary truncated"
        # JSON carries its own punctuation, so it is larger than the padding.
        assert landed[row_id][3] > size, "json truncated"


def test_byte_content_survives_not_just_length(seeded):
    """Equal length with different bytes would pass a length check and be wrong."""
    conn, src, dst = seeded
    assert _run(src, dst, "full_refresh_overwrite").success
    with conn.cursor() as cur:
        cur.execute(
            f'SELECT count(*) FROM "{src}" s JOIN "{dst}" d USING (id) '  # nosec B608
            "WHERE s.big_text IS DISTINCT FROM d.big_text "
            "OR s.big_bytes IS DISTINCT FROM d.big_bytes "
            "OR s.big_json IS DISTINCT FROM d.big_json"
        )
        assert cur.fetchone()[0] == 0
