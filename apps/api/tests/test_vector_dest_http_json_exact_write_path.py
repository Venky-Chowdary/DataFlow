"""Vector dest-sample HTTP reread uses load_http_json, not Response.json().

stdlib Response.json() collapsed 1.234567890123456789 in Pinecone /
Weaviate / Qdrant / Milvus metadata before Gate-8 compared cells.
IEEE-exact 1.5 stays float. Stats/count JSON stays on its own path.
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

from services.target_sample import read_target_sample  # noqa: E402

LONG = "1.234567890123456789"


def _ok(text: str, status: int = 200):
    return SimpleNamespace(
        status_code=status,
        text=text,
        content=text.encode("utf-8"),
    )


def test_pinecone_target_sample_keeps_long_fraction():
    raw = (
        '{"vectors":{"v1":{"metadata":{"source_id":"d1","amt": '
        + LONG
        + ', "n": 1.5}}}}'
    )

    class _Sess:
        def get(self, url, params=None, headers=None, timeout=None):
            return _ok(raw)

    with patch("connectors.pinecone_writer._requests_session", return_value=_Sess()):
        rows = read_target_sample(
            "pinecone",
            {"host": "https://idx.svc.pinecone.io", "api_key": "k"},
            schema="",
            table_name="docs",
            key_values=["v1"],
            limit=1,
        )
    assert rows[0]["amt"] == Decimal(LONG)
    assert rows[0]["n"] == 1.5
    assert rows[0]["amt"] != json.loads(raw)["vectors"]["v1"]["metadata"]["amt"]


def test_weaviate_target_sample_keeps_long_fraction():
    raw = '{"objects":[{"id":"1","properties":{"source_id":"d1","amt": ' + LONG + ', "n": 1.5}}]}'

    class _Sess:
        def get(self, url, params=None, headers=None, timeout=None):
            return _ok(raw)

    with patch("connectors.weaviate_writer._requests_session", return_value=_Sess()):
        rows = read_target_sample(
            "weaviate",
            {"host": "127.0.0.1", "port": 8080},
            schema="",
            table_name="docs",
            limit=1,
        )
    assert rows[0]["amt"] == Decimal(LONG)
    assert rows[0]["n"] == 1.5


def test_qdrant_target_sample_keeps_long_fraction():
    raw = (
        '{"result":{"points":[{"id":"1","payload":{"source_id":"d1","amt": '
        + LONG
        + ', "n": 1.5}}]}}'
    )

    class _Sess:
        def post(self, url, data=None, headers=None, timeout=None):
            return _ok(raw)

    with patch("connectors.qdrant_writer._requests_session", return_value=_Sess()):
        rows = read_target_sample(
            "qdrant",
            {"host": "127.0.0.1", "port": 6333},
            schema="",
            table_name="docs",
            limit=1,
        )
    assert rows[0]["amt"] == Decimal(LONG)
    assert rows[0]["n"] == 1.5


def test_milvus_target_sample_keeps_long_fraction():
    raw = '{"code":0,"data":[{"id":"1","source_id":"d1","amt": ' + LONG + ', "n": 1.5}]}'

    class _Sess:
        def post(self, url, data=None, headers=None, timeout=None):
            return _ok(raw)

    with patch("connectors.milvus_writer._requests_session", return_value=_Sess()):
        rows = read_target_sample(
            "milvus",
            {"host": "127.0.0.1", "port": 19530},
            schema="",
            table_name="chunks",
            limit=1,
        )
    assert rows[0]["amt"] == Decimal(LONG)
    assert rows[0]["n"] == 1.5
