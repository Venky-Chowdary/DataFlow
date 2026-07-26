"""Redis writer unit tests — duplicate-key fail-closed and full-refresh cleanup."""

from __future__ import annotations

import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.redis_writer import (  # noqa: E402
    _infer_redis_conflict_columns,
    _resolve_redis_key_id,
    write_mapped_rows,
)


def test_redis_prefers_code_over_capital_for_countries():
    """Regression: countries column order often puts capital before code alphabetically.

    Using capital as Redis key caused Duplicate Redis key 'countries:Abu_Dhabi'.
    """
    cols = ["capital", "code", "continent", "name"]
    mappings = [{"source": c, "target": c, "confidence": 0.95} for c in cols]
    inferred = _infer_redis_conflict_columns(cols, mappings, None)
    assert inferred == ["code"], inferred

    key, col = _resolve_redis_key_id(
        {
            "capital": "Abu Dhabi",
            "code": "AE",
            "continent": "AS",
            "name": "United Arab Emirates",
        },
        cols,
        conflict_columns=None,
        row_index=0,
    )
    assert col == "code"
    assert key == "AE"


def test_redis_prefers_id_over_natural_key():
    cols = ["code", "id", "name"]
    inferred = _infer_redis_conflict_columns(
        cols,
        [{"source": c, "target": c} for c in cols],
        None,
    )
    assert inferred == ["id"]


def test_humanize_duplicate_redis_key():
    from services.error_handling import humanize_transfer_failure

    human = humanize_transfer_failure(
        "Duplicate Redis key 'countries:Abu_Dhabi' for rows 1 and 2 (conflict on 'capital')."
    )
    assert human["code"] == "duplicate_primary_key"
    assert "primary key" in human["fix"].lower() or "Map" in human["fix"]


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


def _redis_reachable() -> bool:
    try:
        return bool(_redis_client().ping())
    except Exception:
        return False


@pytest.mark.skipif(not _redis_reachable(), reason="Redis not reachable on localhost:6379")
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


@pytest.mark.skipif(not _redis_reachable(), reason="Redis not reachable on localhost:6379")
def test_redis_writer_countries_uses_code_not_capital():
    """Live proof: two rows sharing a capital still write under unique country codes."""
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
            headers=["capital", "code", "name"],
            data_rows=[
                ["Abu Dhabi", "AE", "United Arab Emirates"],
                ["Abu Dhabi", "XX", "Fake Emirate"],
            ],
            mappings=[
                {"source": "capital", "target": "capital", "confidence": 0.95},
                {"source": "code", "target": "code", "confidence": 0.95},
                {"source": "name", "target": "name", "confidence": 0.95},
            ],
            column_types={"capital": "string", "code": "string", "name": "string"},
        )
        assert result.ok is True, result.error
        assert result.rows_written == 2
        client = _redis_client()
        keys = sorted(
            k.decode() if isinstance(k, bytes) else k for k in client.keys(f"{prefix}:*")
        )
        assert f"{prefix}:AE" in keys
        assert f"{prefix}:XX" in keys
        assert f"{prefix}:Abu_Dhabi" not in keys
        client.close()
    finally:
        _cleanup(prefix)


@pytest.mark.skipif(not _redis_reachable(), reason="Redis not reachable on localhost:6379")
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


@pytest.mark.skipif(not _redis_reachable(), reason="Redis not reachable on localhost:6379")
def test_redis_writer_clears_prefix_on_full_refresh_overwrite():
    prefix = _prefix()
    client = _redis_client()
    try:
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
