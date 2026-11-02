"""Memory job list must order mixed datetime and ISO created_at values."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.mongodb_service import MemoryMongoDBService


def test_list_jobs_sorts_mixed_datetime_and_iso_created_at():
    store = MemoryMongoDBService()
    store._jobs["old"] = {
        "_id": "old",
        "created_at": "2020-01-01T00:00:00+00:00",
        "status": "completed",
        "workspace_id": "ws",
    }
    store._jobs["new"] = {
        "_id": "new",
        "created_at": datetime(2024, 1, 1, tzinfo=timezone.utc),
        "status": "completed",
        "workspace_id": "ws",
    }
    store._jobs["blank"] = {
        "_id": "blank",
        "created_at": None,
        "status": "failed",
        "workspace_id": "ws",
    }
    listed = store.list_jobs()
    assert [job["_id"] for job in listed] == ["new", "old", "blank"]


def test_list_connectors_sorts_mixed_datetime_and_iso_created_at():
    store = MemoryMongoDBService()
    store._connectors["old"] = {
        "_id": "old",
        "created_at": "2020-01-01T00:00:00+00:00",
        "name": "old",
        "type": "postgres",
    }
    store._connectors["new"] = {
        "_id": "new",
        "created_at": datetime(2024, 6, 1, tzinfo=timezone.utc),
        "name": "new",
        "type": "postgres",
    }
    listed = store.list_connectors()
    assert [row["_id"] for row in listed] == ["new", "old"]
