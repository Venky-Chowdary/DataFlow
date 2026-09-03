"""Unit tests for Weaviate identity-COPY batch ack validation."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_weaviate_common import _weaviate_assert_batch_ok  # noqa: E402


def test_weaviate_batch_ok_accepts_clean_response():
    batch = [{"class": "C", "id": "1"}]
    response = [{"id": "1", "result": {"status": "SUCCESS"}}]
    _weaviate_assert_batch_ok(batch, response)


def test_weaviate_batch_rejects_per_object_failure():
    batch = [{"class": "C", "id": "1"}, {"class": "C", "id": "2"}]
    response = [
        {"id": "1", "result": {"status": "SUCCESS"}},
        {"id": "2", "result": {"status": "FAILED", "errors": {"error": "bad vector"}}},
    ]
    with pytest.raises(ValueError, match="rejected 1 batch"):
        _weaviate_assert_batch_ok(batch, response)


def test_weaviate_batch_rejects_incomplete_ack():
    batch = [{"class": "C", "id": "1"}, {"class": "C", "id": "2"}]
    with pytest.raises(ValueError, match="incomplete per-object"):
        _weaviate_assert_batch_ok(batch, [{"id": "1"}])
