"""Airtable merge / record id use _is_nullish_conflict_key.

PATCH treated a truthy extract token as a record id, and upsert merge
only refused Python None / strip-empty. After extract emits
SQL_NULL_SENTINEL, performUpsert would create on the wire spelling.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.airtable_writer import (  # noqa: E402
    _airtable_record_id,
    _batch_payload,
    _drop_rows_missing_merge_field,
    _present_fields,
)
from services.value_serializer import SQL_NULL_SENTINEL  # noqa: E402


def test_airtable_record_id_refuses_reader_null():
    for wire in (None, SQL_NULL_SENTINEL, "", "  "):
        assert _airtable_record_id({"id": wire}) is None
    assert _airtable_record_id({"id": "recABC"}) == "recABC"
    assert _airtable_record_id({"Id": "recXYZ"}) == "recXYZ"


def test_patch_does_not_send_reader_null_as_record_id():
    rows = [{"id": SQL_NULL_SENTINEL, "name": "x"}, {"id": "recB", "name": "y"}]
    _url, method, payload, sources = _batch_payload(
        rows, table_name="T", base_id="appX", update=True, merge_field=None
    )
    assert method == "PATCH"
    assert [r["id"] for r in payload["records"]] == ["recB"]
    assert sources == [1]
    assert SQL_NULL_SENTINEL not in str(payload)


def test_upsert_drops_reader_null_merge_field():
    rejected: list[dict] = []
    batch = [{"email": SQL_NULL_SENTINEL}, {"email": "a@x.com"}, {"email": ""}]
    kept, sources, dropped = _drop_rows_missing_merge_field(
        batch,
        batch,
        merge_field="email",
        rejected=rejected,
        batch_offset=0,
        table="T",
        policy="quarantine",
        target_cols=["email"],
    )
    assert dropped == 2
    assert kept == [{"email": "a@x.com"}]
    assert sources == [1]


def test_present_fields_omit_reader_null():
    got = _present_fields(
        {"name": "kept", "note": SQL_NULL_SENTINEL, "blank": ""},
        ["name", "note", "blank"],
    )
    assert got == {"name": "kept"}
