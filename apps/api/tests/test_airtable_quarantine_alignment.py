"""A failed Airtable batch must quarantine the rows it actually sent.

Two filters shrink a batch before it goes out: PATCH keeps only rows carrying a
record id, and upsert drops rows whose merge field is empty. Neither shrinks the
original source list, so pairing ``payload["records"]`` with ``batch`` by
position attributed the failure to whichever row happened to sit at that index.

That is a data-integrity bug, not a reporting one: the DLQ named records that
never left, and replay would have re-sent their values against an error they did
not cause.
"""

from __future__ import annotations

from connectors.airtable_writer import _batch_payload


def _payload(rows, *, update, merge_field=None):
    return _batch_payload(
        rows, table_name="T", base_id="appX", update=update, merge_field=merge_field
    )


def test_patch_reports_the_source_row_of_every_record_sent():
    """Only rows 1 and 3 carry an id, so those are the rows sent."""
    rows = [
        {"name": "no-id-a"},
        {"id": "recB", "name": "has-id-b"},
        {"name": "no-id-c"},
        {"id": "recD", "name": "has-id-d"},
    ]
    _url, method, payload, sources = _payload(rows, update=True)

    assert method == "PATCH"
    assert [r["id"] for r in payload["records"]] == ["recB", "recD"]
    # Positionally these are records 0 and 1; they came from rows 1 and 3.
    assert sources == [1, 3]
    for position, source_index in enumerate(sources):
        sent = payload["records"][position]
        assert sent["fields"]["name"] == rows[source_index]["name"]


def test_upsert_and_insert_send_every_row_in_order():
    rows = [{"email": "a@x.com"}, {"email": "b@x.com"}]

    _u, method, payload, sources = _payload(rows, update=True, merge_field="email")
    assert method == "PATCH"
    assert sources == [0, 1]
    assert len(payload["records"]) == len(rows)

    _u2, method2, payload2, sources2 = _payload(rows, update=False)
    assert method2 == "POST"
    assert sources2 == [0, 1]
    assert len(payload2["records"]) == len(rows)


def test_no_record_sent_maps_to_no_source_row():
    """A batch where nothing qualifies must not claim any row failed."""
    _url, _method, payload, sources = _payload([{"name": "no-id"}], update=True)
    assert payload["records"] == []
    assert sources == []


def test_source_indices_always_address_the_batch():
    """Whatever the filter, every index must be usable against the input list."""
    rows = [{"id": "r1"}, {"name": "no-id"}, {"id": "r3"}, {"Id": "r4"}]
    for update, merge in ((True, None), (True, "name"), (False, None)):
        _url, _method, payload, sources = _payload(rows, update=update, merge_field=merge)
        assert len(sources) == len(payload["records"])
        assert all(0 <= s < len(rows) for s in sources), (update, merge, sources)
        assert sources == sorted(sources)
