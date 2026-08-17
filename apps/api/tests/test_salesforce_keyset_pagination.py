"""Salesforce large-object paging: SOQL OFFSET is capped, keyset must seek.

Salesforce rejects ``OFFSET`` above 2000 rows, so the offset pager could never
move an SObject past the first 2000 records — the transfer either errored with
an opaque API 400 or stopped early. These cases pin the seek contract:

* a cursor page emits ``WHERE Id > 'last'`` + ``ORDER BY Id`` and no ``OFFSET``;
* the batch bookmarks the page boundary in ``meta['next_cursor']``;
* SOQL literals are escaped (quote injection through the bookmark is refused);
* offset paging past the cap fails with an actionable message, never silently.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from connectors import salesforce
from services.keyset_pagination import KEYSET_CAPABLE_SOURCES

DESCRIBE = [
    {"name": "Id", "type": "id"},
    {"name": "Name", "type": "string"},
]


def _response(records: list[dict[str, Any]], *, total: int) -> MagicMock:
    resp = MagicMock(status_code=200)
    resp.json.return_value = {
        "totalSize": total,
        "done": True,
        "records": [{"attributes": {}, **r} for r in records],
    }
    resp.raise_for_status.return_value = None
    return resp


def _run(**kwargs: Any) -> tuple[Any, list[str]]:
    queries: list[str] = []

    def _request(*, method: str, url: str, token: str = "", params=None, timeout: int = 60):
        if params and "q" in params:
            queries.append(params["q"])
        return _response(
            [{"Id": "001A", "Name": "a"}, {"Id": "001B", "Name": "b"}], total=2
        )

    with patch.object(salesforce, "_access", return_value=("tok", "https://x.my.salesforce.com")):
        with patch.object(salesforce, "describe_sobject", return_value=DESCRIBE):
            with patch.object(salesforce, "request", side_effect=_request):
                batch = salesforce.read_object(cfg={}, object="Account", **kwargs)
    return batch, queries


def test_salesforce_is_keyset_capable():
    assert "salesforce" in KEYSET_CAPABLE_SOURCES


def test_first_keyset_page_orders_by_cursor_without_offset():
    batch, queries = _run(limit=200, offset=0, cursor_column="Id")
    assert queries, "expected a SOQL query"
    soql = queries[0]
    assert "ORDER BY Id" in soql
    assert "OFFSET" not in soql
    assert "WHERE" not in soql  # first page has no bookmark
    assert batch.meta["next_cursor"] == "001B"
    assert batch.meta["pagination_mode"] == "keyset"


def test_resume_page_seeks_past_the_bookmark():
    _, queries = _run(limit=200, offset=5000, cursor_column="Id", cursor_after="001A")
    soql = queries[0]
    assert "WHERE Id > '001A'" in soql
    # A resumed keyset page must never fall back to the capped OFFSET pager.
    assert "OFFSET" not in soql


def test_cursor_bookmark_literal_is_escaped():
    _, queries = _run(limit=10, cursor_column="Id", cursor_after="a' OR Name != '")
    assert "WHERE Id > 'a\\' OR Name != \\''" in queries[0]


def test_cursor_column_identifier_is_validated():
    with pytest.raises(ValueError):
        _run(limit=10, cursor_column="Id; DROP")


def test_offset_beyond_soql_cap_fails_closed_with_guidance():
    with pytest.raises(RuntimeError) as exc:
        _run(limit=200, offset=salesforce.SOQL_MAX_OFFSET + 1)
    message = str(exc.value)
    assert "2000" in message
    assert "keyset" in message.lower()


def test_offset_within_cap_still_uses_offset_paging():
    _, queries = _run(limit=200, offset=1000)
    assert "OFFSET 1000" in queries[0]
