"""Redis writer unit tests — duplicate-key fail-closed and full-refresh cleanup."""

from __future__ import annotations

import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.redis_writer import write_mapped_rows  # noqa: E402


def _redis_client():
    import redis

    return redis.Redis(host="localhost", port=6379, db=0, socket_timeout=5)


def _prefix():
    return f"test_redis_writer_{uuid.uuid4().hex[:8]}"


def _cleanup(prefix: str) -> None:
    client = _redis_client()
    try:
        for key in client.scan_iter(match=f"{prefix}:*", count=500):
            client.delete(key)
    finally:
        client.close()


@pytest.mark.skipif(
    not _redis_client().ping(),
    reason="Redis not reachable on localhost:6379",
)
def test_redis_writer_uses_id_by_default():
    prefix = _prefix()
    _cleanup(prefix)
    try:
        result = write_mapped_rows(
            host="localhost",
            port=6379,
            database="0",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            table_name=prefix,
            headers=["id", "code", "name"],
            data_rows=[["1", "US", "USA"], ["2", "CA", "Canada"]],
            mappings=[
                {"source": "id", "target": "id", "confidence": 0.95},
                {"source": "code", "target": "code", "confidence": 0.95},
                {"source": "name", "target": "name", "confidence": 0.95},
            ],
            column_types={"id": "integer", "code": "string", "name": "string"},
        )
        assert result.ok is True, result.error
        assert result.rows_written == 2
        client = _redis_client()
        assert len(client.keys(f"{prefix}:*")) == 2
        client.close()
    finally:
        _cleanup(prefix)


@pytest.mark.skipif(
    not _redis_client().ping(),
    reason="Redis not reachable on localhost:6379",
)
def test_redis_writer_fails_on_duplicate_key():
    prefix = _prefix()
    _cleanup(prefix)
    try:
        result = write_mapped_rows(
            host="localhost",
            port=6379,
            database="0",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            table_name=prefix,
            headers=["id", "code", "name"],
            data_rows=[
                ["1", "US", "USA"],
                ["2", "US", "United States"],
                ["3", "CA", "Canada"],
            ],
            mappings=[
                {"source": "id", "target": "id", "confidence": 0.95},
                {"source": "code", "target": "code", "confidence": 0.95},
                {"source": "name", "target": "name", "confidence": 0.95},
            ],
            column_types={"id": "integer", "code": "string", "name": "string"},
            conflict_columns=["code"],
        )
        assert result.ok is False
        assert "Duplicate Redis key" in (result.error or "")
    finally:
        _cleanup(prefix)


@pytest.mark.skipif(
    not _redis_client().ping(),
    reason="Redis not reachable on localhost:6379",
)
def test_redis_writer_clears_prefix_on_full_refresh_overwrite():
    prefix = _prefix()
    client = _redis_client()
    try:
        # Seed old keys
        for i in range(5):
            client.set(f"{prefix}:{i}", '{"old": true}')
        assert len(client.keys(f"{prefix}:*")) == 5

        result = write_mapped_rows(
            host="localhost",
            port=6379,
            database="0",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            table_name=prefix,
            headers=["id", "name"],
            data_rows=[["10", "New"], ["20", "Data"]],
            mappings=[
                {"source": "id", "target": "id", "confidence": 0.95},
                {"source": "name", "target": "name", "confidence": 0.95},
            ],
            column_types={"id": "integer", "name": "string"},
            sync_mode="full_refresh_overwrite",
        )
        assert result.ok is True, result.error
        assert result.rows_written == 2
        assert len(client.keys(f"{prefix}:*")) == 2
    finally:
        client.close()
        _cleanup(prefix)
