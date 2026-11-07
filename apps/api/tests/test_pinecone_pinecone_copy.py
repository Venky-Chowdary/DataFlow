"""Pinecone → Pinecone list+fetch+upsert — dest vectorCount, never DISTINCT source_id."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_fast_path import FastPathUnavailable  # noqa: E402
from services.copy_pinecone_common import (  # noqa: E402
    pinecone_family_name,
    pinecone_type_is_copy_safe,
)
from services.copy_pinecone_pinecone import (  # noqa: E402
    copy_pinecone_to_pinecone,
    pinecone_pinecone_copy_enabled,
)


def _pinecone_cfg(namespace: str) -> dict:
    return {
        "type": "pinecone",
        "format": "pinecone",
        "host": "https://test-index.svc.pinecone.io",
        "api_key": "test-key",
        "database": namespace,
        "table": namespace,
    }


def test_pinecone_family_and_copy_safe_types():
    assert pinecone_family_name("pinecone") == "pinecone"
    assert pinecone_family_name("pinecone_serverless") == "pinecone"
    assert pinecone_type_is_copy_safe("text") is True
    assert pinecone_type_is_copy_safe("integer") is True
    assert pinecone_type_is_copy_safe("join") is False


def test_pinecone_pinecone_copy_kill_switch(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PINECONE_PINECONE_COPY", "0")
    assert pinecone_pinecone_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_pinecone_to_pinecone(
            source_cfg=_pinecone_cfg("src"),
            source_table="src",
            dest_cfg=_pinecone_cfg("dst"),
            dest_table="dst",
            pairs=[("content", "content")],
            pinecone_ddls=["text"],
            replace_destination=True,
        )


def test_pinecone_pinecone_same_namespace_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_PINECONE_PINECONE_COPY", raising=False)
    cfg = _pinecone_cfg("same_ns")
    with pytest.raises(FastPathUnavailable, match="same namespace"):
        copy_pinecone_to_pinecone(
            source_cfg=cfg,
            source_table="same_ns",
            dest_cfg=cfg,
            dest_table="same_ns",
            pairs=[("content", "content")],
            pinecone_ddls=["text"],
            replace_destination=True,
        )


def test_pinecone_pinecone_cross_index_declines():
    src = _pinecone_cfg("a")
    dest = {
        **_pinecone_cfg("b"),
        "host": "https://other-index.svc.pinecone.io",
    }
    with pytest.raises(FastPathUnavailable, match="cross-index"):
        copy_pinecone_to_pinecone(
            source_cfg=src,
            source_table="a",
            dest_cfg=dest,
            dest_table="b",
            pairs=[("content", "content")],
            pinecone_ddls=["text"],
            replace_destination=True,
        )


def test_pinecone_pinecone_public_proxy_declines():
    dest = {
        **_pinecone_cfg("b"),
        "host": "",
        "connection_string": "https://caboose.proxy.rlwy.net",
    }
    with pytest.raises(FastPathUnavailable, match="public proxy"):
        copy_pinecone_to_pinecone(
            source_cfg=_pinecone_cfg("a"),
            source_table="a",
            dest_cfg=dest,
            dest_table="b",
            pairs=[("content", "content")],
            pinecone_ddls=["text"],
            replace_destination=True,
        )


def test_pinecone_pinecone_column_rename_declines():
    with pytest.raises(FastPathUnavailable, match="rename"):
        copy_pinecone_to_pinecone(
            source_cfg=_pinecone_cfg("a"),
            source_table="a",
            dest_cfg=_pinecone_cfg("b"),
            dest_table="b",
            pairs=[("content", "other")],
            pinecone_ddls=["text"],
            replace_destination=True,
        )


def test_pinecone_pinecone_missing_host_declines():
    cfg = {"type": "pinecone", "api_key": "k", "table": "ns"}
    with pytest.raises(FastPathUnavailable, match="index host"):
        copy_pinecone_to_pinecone(
            source_cfg=cfg,
            source_table="ns",
            dest_cfg={**cfg, "table": "other"},
            dest_table="other",
            pairs=[("content", "content")],
            pinecone_ddls=["text"],
            replace_destination=True,
        )


def test_pinecone_skip_complete_when_counts_match(monkeypatch):
    monkeypatch.delenv("DATAFLOW_PINECONE_PINECONE_COPY", raising=False)
    monkeypatch.setattr(
        "services.copy_pinecone_pinecone.pinecone_namespace_exists",
        lambda _cfg, _ns: True,
    )
    monkeypatch.setattr(
        "services.copy_pinecone_pinecone.pinecone_vector_count",
        lambda _cfg, _ns: 12,
    )
    result = copy_pinecone_to_pinecone(
        source_cfg=_pinecone_cfg("src"),
        source_table="src",
        dest_cfg=_pinecone_cfg("dst"),
        dest_table="dst",
        pairs=[("content", "content")],
        pinecone_ddls=["text"],
        replace_destination=False,
    )
    assert result.source_snapshot.get("copy_split") == "skip"


def test_pinecone_occupied_mismatch_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_PINECONE_PINECONE_COPY", raising=False)
    monkeypatch.setattr(
        "services.copy_pinecone_pinecone.pinecone_namespace_exists",
        lambda _cfg, _ns: True,
    )

    def _count(_cfg, ns):
        return 2 if ns == "dst" else 40

    monkeypatch.setattr(
        "services.copy_pinecone_pinecone.pinecone_vector_count",
        _count,
    )
    with pytest.raises(FastPathUnavailable, match="occupied Pinecone dest"):
        copy_pinecone_to_pinecone(
            source_cfg=_pinecone_cfg("src"),
            source_table="src",
            dest_cfg=_pinecone_cfg("dst"),
            dest_table="dst",
            pairs=[("content", "content")],
            pinecone_ddls=["text"],
            replace_destination=False,
        )
