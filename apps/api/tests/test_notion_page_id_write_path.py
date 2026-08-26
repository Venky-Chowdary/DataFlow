"""Notion page identity uses saas_record_id, not ``if val``.

``if val`` treated extract SQL_NULL_SENTINEL as a page id and would
PATCH /v1/pages/__DF_SQL_NULL__. Integer 0 stays a present token.
Hyphenated 32-hex UUIDs still normalize.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.notion_writer import (  # noqa: E402
    _notion_page_identity,
    _page_id,
)
from services.value_serializer import (  # noqa: E402
    DF_MISSING_SENTINEL,
    SQL_NULL_SENTINEL,
    Missing,
)


def test_notion_page_identity_refuses_reader_null_and_blank():
    for wire in (None, "", "  ", SQL_NULL_SENTINEL, "__df_ddb_null__", Missing, DF_MISSING_SENTINEL):
        assert _notion_page_identity(wire) is None, wire


def test_notion_page_identity_keeps_zero_and_hyphenates_uuid():
    assert _notion_page_identity(0) == "0"
    assert _notion_page_identity("page-abc") == "page-abc"
    raw = "0123456789abcdef0123456789abcdef"
    assert _notion_page_identity(raw) == _page_id(raw)
    assert _notion_page_identity(raw) == "01234567-89ab-cdef-0123-456789abcdef"
    assert SQL_NULL_SENTINEL not in (_notion_page_identity(SQL_NULL_SENTINEL) or "")
