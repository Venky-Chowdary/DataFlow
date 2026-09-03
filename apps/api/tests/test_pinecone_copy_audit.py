"""Pinecone fetch miss must fail closed (no silent partial copy)."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_pinecone_common import pinecone_list_fetch_upsert  # noqa: E402


def test_pinecone_fetch_miss_raises():
    session = MagicMock()
    list_resp = MagicMock()
    list_resp.status_code = 200
    list_resp.content = b'{"vectors":[{"id":"a"},{"id":"b"}],"pagination":{}}'
    fetch_resp = MagicMock()
    fetch_resp.status_code = 200
    fetch_resp.content = b'{"vectors":{"a":{"id":"a","values":[0.1]}}}'
    session.get.return_value = list_resp
    session.post.return_value = fetch_resp

    cfg = {
        "host": "https://idx.svc.pinecone.io",
        "api_key": "k",
        "table": "ns",
    }

    with patch("services.copy_pinecone_common._pinecone_session", return_value=(session, "https://idx.svc.pinecone.io", {})):
        with patch("services.copy_pinecone_common._pinecone_list_supported", return_value=True):
            with patch("services.copy_pinecone_common.load_http_json") as load_json:
                load_json.side_effect = [
                    {"vectors": [{"id": "a"}, {"id": "b"}], "pagination": {}},
                    {"vectors": {"a": {"id": "a", "values": [0.1]}}},
                ]
                with pytest.raises(ValueError, match="fetch missing"):
                    pinecone_list_fetch_upsert(
                        source_cfg=cfg,
                        dest_cfg=cfg,
                        src_namespace="ns",
                        dest_namespace="dest",
                    )
