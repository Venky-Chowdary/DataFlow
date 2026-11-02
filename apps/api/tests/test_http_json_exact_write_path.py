"""HTTP source JSON uses load_http_json, not Response.json().

stdlib Response.json() is json.loads, so 1.234567890123456789 in an API
cell collapsed to IEEE before flatten/bind. IEEE-exact 1.5 stays float.
Invalid bodies raise.
"""

from __future__ import annotations

import json
import sys
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.rest_api import _read_page  # noqa: E402
from services.value_serializer import load_http_json  # noqa: E402

LONG = "1.234567890123456789"


def _resp(text: str):
    return SimpleNamespace(text=text, content=text.encode("utf-8"), headers={})


def test_load_http_json_keeps_long_fraction():
    raw = f'{{"amt": {LONG}, "n": 1.5, "id": 1}}'
    body = load_http_json(_resp(raw))
    assert body["amt"] == Decimal(LONG)
    assert body["amt"] != json.loads(raw)["amt"]
    assert body["n"] == 1.5
    assert body["id"] == 1


def test_load_http_json_from_content_bytes():
    raw = f'{{"amt": {LONG}}}'
    body = load_http_json(SimpleNamespace(content=raw.encode("utf-8")))
    assert body["amt"] == Decimal(LONG)


def test_load_http_json_tree_double_passthrough():
    tree = {"amt": Decimal(LONG)}
    body = load_http_json(SimpleNamespace(json=lambda: tree))
    assert body is tree


def test_load_http_json_invalid_raises():
    try:
        load_http_json(_resp("{not-json}"))
    except (json.JSONDecodeError, ValueError):
        return
    raise AssertionError("invalid HTTP JSON must refuse")


def test_rest_read_page_keeps_long_fraction():
    raw = f'[{{"amt": {LONG}, "n": 1.5}}]'
    cfg = {
        "host": "https://example.test",
        "object_path": "orders",
        "data_path": "",
        "pagination_type": "none",
        "limit_param": "limit",
        "offset_param": "offset",
        "page_param": "page",
        "cursor_param": "cursor",
        "auth_header": "",
        "auth_prefix": "Bearer",
        "auth_query": "api_key",
        "extra_headers": {},
    }

    class _Fake:
        def raise_for_status(self):
            return None

        text = raw
        headers = {}

    with patch("connectors.rest_api._request_with_retry", return_value=_Fake()):
        records, nxt, more = _read_page(cfg, {})
    assert len(records) == 1
    assert records[0]["amt"] == Decimal(LONG)
    assert records[0]["n"] == 1.5
    assert nxt is None
    assert more is False
