"""Sub-second precision has to survive the default timestamp mapping.

The mapper stamps the ``datetime`` transform on every timestamp column, so the
canonical wire form it produces is what almost every timestamp in the product
passes through. It rendered seconds and nothing finer, which meant a PostgreSQL
``timestamp(6)`` copied into an identical ``timestamp(6)`` arrived with its
microseconds gone — every route, every row, no finding raised.

The whole-second rendering is pinned alongside, because that is the shape the
rest of the system and its tests expect and the fix must not change it.
"""

from __future__ import annotations

import socket
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from services.transform_engine import apply_transform


def _dt(value: str) -> str:
    out, err = apply_transform(value, "datetime")
    assert err is None, err
    return out


def test_microseconds_survive_a_naive_timestamp():
    assert _dt("2026-08-13T16:42:32.677645") == "2026-08-13T16:42:32.677645"


def test_microseconds_survive_a_utc_timestamp():
    assert _dt("2026-08-13T16:42:32.677645+00:00") == "2026-08-13T16:42:32.677645Z"


def test_microseconds_survive_an_offset_timestamp():
    assert _dt("2026-08-13T16:42:32.677645+05:30") == "2026-08-13T16:42:32.677645+05:30"


def test_trailing_zero_fraction_is_kept_as_written():
    """.500000 is half a second, not five hundred thousand of nothing."""
    assert _dt("2026-08-13T16:42:32.500000") == "2026-08-13T16:42:32.500000"


def test_whole_seconds_render_exactly_as_before():
    assert _dt("2026-08-13T16:42:32") == "2026-08-13T16:42:32"
    assert _dt("2026-08-13T16:42:32+00:00") == "2026-08-13T16:42:32Z"
    assert _dt("2026-08-13T16:42:32+05:30") == "2026-08-13T16:42:32+05:30"


def test_naive_still_refuses_to_invent_a_zone():
    """The rule this function already had must survive the precision fix."""
    assert not _dt("2026-08-13T16:42:32.677645").endswith("Z")
    assert "+" not in _dt("2026-08-13T16:42:32.677645")


def _pg_reachable() -> bool:
    try:
        socket.create_connection(("127.0.0.1", 5432), timeout=1).close()
        return True
    except OSError:
        return False


@pytest.mark.skipif(not _pg_reachable(), reason="PostgreSQL not reachable")
def test_postgresql_roundtrip_keeps_microseconds():
    """End to end: identical timestamp(6) columns must arrive identical."""
    psycopg2 = pytest.importorskip("psycopg2")
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    suffix = uuid.uuid4().hex[:8]
    src, dst = f"usec_src_{suffix}", f"usec_dst_{suffix}"
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        dbname="dataflow",
        user="dataflow",
        password="dataflow",
    )
    conn.autocommit = True
    stamps = [
        datetime(2026, 8, 13, 16, 42, 32, 677645),
        datetime(2024, 1, 1, 0, 0, 0, 1),
        datetime(2024, 6, 30, 23, 59, 59, 999999),
        datetime(2024, 3, 3, 12, 0, 0),
    ]
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'CREATE TABLE "{src}" (id bigint primary key, created_at timestamp(6))'
            )
            for i, stamp in enumerate(stamps, start=1):
                cur.execute(f'INSERT INTO "{src}" VALUES (%s, %s)', (i, stamp))

        def endpoint(table: str) -> EndpointConfig:
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

        result = UniversalTransferEngine().execute_tracked(
            TransferRequest(
                source=endpoint(src),
                destination=endpoint(dst),
                mappings=[
                    {"source": "id", "target": "id", "confidence": 0.99},
                    {"source": "created_at", "target": "created_at", "confidence": 0.99},
                ],
                sync_mode="full_refresh_overwrite",
                validation_mode="strict",
                skip_preflight=True,
                stream_contracts=[
                    {
                        "name": src,
                        "primary_key": "id",
                        "selected": True,
                        "sync_mode": "full_refresh_overwrite",
                    }
                ],
            ),
            uuid.uuid4().hex[:24],
        )
        assert result.success, result.error

        with conn.cursor() as cur:
            cur.execute(f'SELECT id, created_at FROM "{dst}" ORDER BY id')
            landed = {row[0]: row[1] for row in cur.fetchall()}
        assert landed == dict(enumerate(stamps, start=1))
        # Stated separately: an equal count with truncated values is the exact
        # failure this test exists for, so assert the microseconds themselves.
        assert landed[1].microsecond == 677645
        assert landed[3].microsecond == 999999
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{src}"')
            cur.execute(f'DROP TABLE IF EXISTS "{dst}"')
        conn.close()


@pytest.mark.skipif(not _pg_reachable(), reason="PostgreSQL not reachable")
def test_offset_timestamps_keep_their_instant_and_precision():
    psycopg2 = pytest.importorskip("psycopg2")
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    suffix = uuid.uuid4().hex[:8]
    src, dst = f"utz_src_{suffix}", f"utz_dst_{suffix}"
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        dbname="dataflow",
        user="dataflow",
        password="dataflow",
    )
    conn.autocommit = True
    ist = timezone(timedelta(hours=5, minutes=30))
    stamp = datetime(2026, 8, 13, 16, 42, 32, 677645, tzinfo=ist)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'CREATE TABLE "{src}" (id bigint primary key, seen_at timestamptz)'
            )
            cur.execute(f'INSERT INTO "{src}" VALUES (1, %s)', (stamp,))

        def endpoint(table: str) -> EndpointConfig:
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

        result = UniversalTransferEngine().execute_tracked(
            TransferRequest(
                source=endpoint(src),
                destination=endpoint(dst),
                mappings=[
                    {"source": "id", "target": "id", "confidence": 0.99},
                    {"source": "seen_at", "target": "seen_at", "confidence": 0.99},
                ],
                sync_mode="full_refresh_overwrite",
                validation_mode="strict",
                skip_preflight=True,
                stream_contracts=[
                    {
                        "name": src,
                        "primary_key": "id",
                        "selected": True,
                        "sync_mode": "full_refresh_overwrite",
                    }
                ],
            ),
            uuid.uuid4().hex[:24],
        )
        assert result.success, result.error

        with conn.cursor() as cur:
            cur.execute(f'SELECT seen_at FROM "{dst}"')
            landed = cur.fetchone()[0]
        assert landed == stamp
        assert landed.microsecond == 677645
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{src}"')
            cur.execute(f'DROP TABLE IF EXISTS "{dst}"')
        conn.close()
