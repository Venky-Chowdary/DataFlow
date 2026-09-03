"""Pinecone identity-COPY: fetch miss, upsert ack, sparse vectors, happy path."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_fast_path import FastPathUnavailable  # noqa: E402
from services.copy_pinecone_common import (  # noqa: E402
    _pinecone_assert_upsert_ok,
    _pinecone_copy_vector,
    pinecone_list_fetch_upsert,
)


def _resp(status_code: int, payload: dict) -> SimpleNamespace:
    content = json.dumps(payload).encode("utf-8")
    return SimpleNamespace(
        status_code=status_code,
        content=content,
        text=content.decode("utf-8"),
    )


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


def test_pinecone_copy_vector_includes_sparse():
    out = _pinecone_copy_vector(
        "v1",
        {
            "values": [0.1, 0.2],
            "sparseValues": {"indices": [1, 4], "values": [0.9, 0.3]},
            "metadata": {"k": "x"},
        },
    )
    assert out["id"] == "v1"
    assert out["values"] == [0.1, 0.2]
    assert out["sparseValues"]["indices"] == [1, 4]
    assert out["metadata"]["k"] == "x"


def test_pinecone_copy_vector_sparse_only():
    out = _pinecone_copy_vector(
        "v2",
        {"sparseValues": {"indices": [0], "values": [1.0]}},
    )
    assert out["values"] == []
    assert out["sparseValues"]["indices"] == [0]


def test_pinecone_copy_vector_neither_raises():
    with pytest.raises(ValueError, match="neither values nor sparseValues"):
        _pinecone_copy_vector("v3", {"metadata": {"k": "x"}})


def test_pinecone_upsert_ok_matching_count():
    _pinecone_assert_upsert_ok(_resp(200, {"upsertedCount": 2}), 2)


def test_pinecone_upsert_rejects_partial_count():
    with pytest.raises(ValueError, match="upsertedCount 1 != batch 2"):
        _pinecone_assert_upsert_ok(_resp(200, {"upsertedCount": 1}), 2)


def test_pinecone_upsert_rejects_missing_count():
    with pytest.raises(ValueError, match="missing upsertedCount"):
        _pinecone_assert_upsert_ok(_resp(200, {}), 2)


def test_pinecone_list_unsupported_declines():
    session = MagicMock()
    cfg = {
        "host": "https://idx.svc.pinecone.io",
        "api_key": "k",
        "table": "ns",
    }
    with patch(
        "services.copy_pinecone_common._pinecone_session",
        return_value=(session, "https://idx.svc.pinecone.io", {}),
    ):
        with patch(
            "services.copy_pinecone_common._pinecone_list_supported",
            return_value=False,
        ):
            with pytest.raises(FastPathUnavailable, match="/vectors/list"):
                pinecone_list_fetch_upsert(
                    source_cfg=cfg,
                    dest_cfg=cfg,
                    src_namespace="ns",
                    dest_namespace="dest",
                )


def test_pinecone_happy_path_list_fetch_upsert():
    session = MagicMock()
    list_resp = _resp(
        200, {"vectors": [{"id": "a"}, {"id": "b"}], "pagination": {}}
    )
    fetch_resp = _resp(
        200,
        {
            "vectors": {
                "a": {
                    "id": "a",
                    "values": [0.1],
                    "sparseValues": {"indices": [2], "values": [0.5]},
                    "metadata": {"k": "a"},
                },
                "b": {"id": "b", "values": [0.2]},
            }
        },
    )
    upsert_resp = _resp(200, {"upsertedCount": 2})
    session.get.return_value = list_resp

    def _post(url, **_kwargs):
        if "/fetch" in url:
            return fetch_resp
        return upsert_resp

    session.post.side_effect = _post
    cfg = {
        "host": "https://idx.svc.pinecone.io",
        "api_key": "k",
        "table": "ns",
    }
    with patch(
        "services.copy_pinecone_common._pinecone_session",
        return_value=(session, "https://idx.svc.pinecone.io", {}),
    ):
        with patch(
            "services.copy_pinecone_common._pinecone_list_supported",
            return_value=True,
        ):
            copied = pinecone_list_fetch_upsert(
                source_cfg=cfg,
                dest_cfg=cfg,
                src_namespace="ns",
                dest_namespace="dest",
            )
    assert copied == 2
    upsert_calls = [
        c for c in session.post.call_args_list if "/upsert" in str(c.args[0])
    ]
    assert len(upsert_calls) == 1
    payload = json.loads(upsert_calls[0].kwargs["data"])
    assert payload["namespace"] == "dest"
    by_id = {v["id"]: v for v in payload["vectors"]}
    assert by_id["a"]["sparseValues"]["indices"] == [2]
    assert by_id["b"]["values"] == [0.2]
