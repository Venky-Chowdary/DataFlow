"""SaaS invent refuse + dense MERGE null-PK gate."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_assert_dense_upsert_keys_present_refuses_null():
    from connectors.writer_common import assert_dense_upsert_keys_present

    assert_dense_upsert_keys_present([("1", "a")], ["id"], ["id", "note"])
    with pytest.raises(ValueError, match="null/empty conflict"):
        assert_dense_upsert_keys_present([(None, "a")], ["id"], ["id", "note"])
    with pytest.raises(ValueError, match="null/empty conflict"):
        assert_dense_upsert_keys_present([{"id": "", "note": "x"}], ["id"])


def test_airtable_update_never_falls_through_to_post():
    from connectors.airtable_writer import _batch_payload

    url, method, payload, sources = _batch_payload(
        [{"name": "no-id"}],
        table_name="T",
        base_id="appX",
        update=True,
        merge_field=None,
    )
    assert method == "PATCH"
    assert payload["records"] == []
    # No record was sent, so nothing maps back to a source row.
    assert sources == []


def test_airtable_insert_still_posts():
    from connectors.airtable_writer import _batch_payload

    url, method, payload, sources = _batch_payload(
        [{"name": "n"}],
        table_name="T",
        base_id="appX",
        update=False,
        merge_field=None,
    )
    assert method == "POST"
    assert len(payload["records"]) == 1
    assert sources == [0]
