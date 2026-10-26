"""D16 regression tests — a non-paginated REST endpoint must ingest, not
be refused as a "repeated page".

P3.8: a 500-row single-page array ("comments") failed with "REST
pagination returned a repeated page" because the API ignored the
offset/page params and returned the whole array on every request. The
guard is kept for real broken pagers (same <=page_limit rows again).
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.rest_api import read_object


def _make_response(json_data: Any, headers: dict | None = None, status: int = 200):
    m = mock.MagicMock()
    m.status_code = status
    m.headers = headers or {}
    m.json.return_value = json_data
    m.raise_for_status = mock.MagicMock()
    return m


def _array(n: int) -> list[dict[str, Any]]:
    return [{"id": i, "name": f"row-{i}", "active": True} for i in range(n)]


def test_rest_single_page_array():
    """Non-paginated endpoint: same full array each call → ingest it once."""
    pages = _array(500)
    with mock.patch(
        "connectors.rest_api.requests.request",
        return_value=_make_response(pages),
    ):
        batch = read_object(
            cfg={"host": "https://api.example.com", "table": "comments"},
            object="comments",
            limit=1000,
        )
    assert len(batch.rows) == 500
    assert batch.total_rows == 500  # whole-array read is authoritative
    ids = {r[0] for r in batch.rows}
    assert ids == {str(i) for i in range(500)}


def test_rest_repeated_page_still_refused():
    """A pager returning the same <=page_limit slice is still refused —
    the tail would be silently lost otherwise."""
    page = _array(100)  # full pages only — a short last page just ends the walk
    with mock.patch(
        "connectors.rest_api.requests.request",
        return_value=_make_response(page),
    ):
        with pytest.raises(RuntimeError, match="repeated page"):
            read_object(
                cfg={"host": "https://api.example.com", "table": "comments"},
                object="comments",
                limit=200,
            )


def test_rest_offset_pages_still_paginate():
    """Real pagers keep working: distinct pages are collected."""
    calls = []

    def fake_request(method, url, **kwargs):
        params = kwargs.get("params") or {}
        calls.append(dict(params))
        off = int(params.get("offset", 0) or 0)
        return _make_response(_array(250)[off : off + 100])

    with mock.patch("connectors.rest_api.requests.request", side_effect=fake_request):
        batch = read_object(
            cfg={"host": "https://api.example.com", "table": "comments"},
            object="comments",
            limit=250,
        )
    assert len(batch.rows) == 250
    assert len(calls) == 3


def test_rest_generic_types_inferred():
    """Generic REST must not report every column TEXT when values are typed."""
    rows = [{"id": 1, "score": 1.5, "active": True, "meta": {"x": 1}, "name": "a"}]
    with mock.patch(
        "connectors.rest_api.requests.request",
        return_value=_make_response(rows),
    ):
        batch = read_object(
            cfg={"host": "https://api.example.com", "table": "t"},
            object="t",
            limit=10,
        )
    types = (batch.meta or {}).get("native_types") or {}
    assert types.get("id") == "INTEGER"
    assert types.get("score") == "FLOAT"
    assert types.get("active") == "BOOLEAN"
    assert types.get("meta.x") == "INTEGER"
    assert types.get("name") == "TEXT"
