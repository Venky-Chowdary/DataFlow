"""Unit tests for Qdrant identity-COPY audit fixes."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_qdrant_common import _qdrant_assert_upsert_ok  # noqa: E402


def _resp(status_code: int, payload: dict) -> SimpleNamespace:
    import json

    content = json.dumps(payload).encode("utf-8")

    class _R:
        def __init__(self) -> None:
            self.status_code = status_code
            self.content = content
            self.text = content.decode("utf-8")

    return _R()


def test_qdrant_upsert_ok_accepts_completed():
    _qdrant_assert_upsert_ok(
        _resp(200, {"status": "ok", "result": {"status": "completed", "operation_id": 1}})
    )


def test_qdrant_upsert_rejects_failed_operation_status():
    with pytest.raises(ValueError, match="operation status not completed"):
        _qdrant_assert_upsert_ok(
            _resp(200, {"status": "ok", "result": {"status": "failed"}})
        )


def test_qdrant_upsert_rejects_bad_http_status():
    with pytest.raises(ValueError, match="Qdrant upsert failed: 500"):
        _qdrant_assert_upsert_ok(_resp(500, {"status": "error"}))
