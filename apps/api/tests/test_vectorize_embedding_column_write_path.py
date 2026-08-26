"""vectorize embedding_column uses coerce_embedding, not float(x).

A supplied Auto 1.234 / 2**53+1 vector must quarantine, never silent re-embed
or IEEE-collapse into the vector store.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.vectorization import vectorize_records  # noqa: E402


def test_plain_ieee_embedding_column_still_binds():
    rows = vectorize_records(
        [{"id": "1", "content": "hello world document", "embedding": [0.1, 0.2, 0.3]}],
        content_column="content",
        embedding_column="embedding",
        model="hash/32",
    )
    assert len(rows) == 1
    assert rows[0]["embedding"] == [0.1, 0.2, 0.3]
    assert not rows[0].get("_df_embed_error")


def test_locale_money_embedding_column_binds():
    rows = vectorize_records(
        [{"id": "1", "content": "hello world document", "embedding": ["$1.50", "2"]}],
        content_column="content",
        embedding_column="embedding",
        model="hash/32",
    )
    assert rows[0]["embedding"] == [1.5, 2.0]


def test_auto_grouping_refuses_silent_reembed():
    rows = vectorize_records(
        [{"id": "1", "content": "hello world document", "embedding": ["1.234", "2"]}],
        content_column="content",
        embedding_column="embedding",
        model="hash/32",
    )
    assert rows[0]["embedding"] is None
    assert "refuse silent re-embed" in str(rows[0].get("_df_embed_error") or "")
    assert "refuse invent" in str(rows[0].get("_df_embed_error") or "")


def test_ieee_lossy_mantissa_refuses_silent_reembed():
    rows = vectorize_records(
        [{"id": "1", "content": "hello world document", "embedding": [9007199254740993, 1.0]}],
        content_column="content",
        embedding_column="embedding",
        model="hash/32",
    )
    assert rows[0]["embedding"] is None
    assert "refuse silent re-embed" in str(rows[0].get("_df_embed_error") or "")
