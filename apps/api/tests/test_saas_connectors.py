"""Unit tests for SaaS source and reverse-ETL connectors using mocked HTTP responses."""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest
responses = pytest.importorskip("responses", reason="requires the optional HTTP mocking test dependency")

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import connectors.airtable_writer as airtable_writer  # noqa: E402
import connectors.hubspot as hubspot  # noqa: E402
import connectors.salesforce as salesforce  # noqa: E402
import connectors.shopify_writer as shopify_writer  # noqa: E402
import connectors.stripe as stripe  # noqa: E402


@responses.activate
def test_salesforce_probe_success():
    responses.add(
        responses.GET,
        re.compile(r"https://login\.salesforce\.com/services/data/v58\.0/limits"),
        json={"DailyApiRequests": {"Max": 1000, "Remaining": 999}},
        status=200,
    )
    ok, msg = salesforce.test_salesforce(api_key="fake-token")
    assert ok is True
    assert "reachable" in msg.lower()


@responses.activate
def test_salesforce_probe_auth_failure():
    responses.add(
        responses.GET,
        re.compile(r"https://login\.salesforce\.com/services/data/v58\.0/limits"),
        json=[{"message": "Session expired", "errorCode": "INVALID_SESSION_ID"}],
        status=401,
    )
    ok, msg = salesforce.test_salesforce(api_key="bad-token")
    assert ok is False
    assert "authentication" in msg.lower()


@responses.activate
def test_salesforce_read_object():
    responses.add(
        responses.GET,
        re.compile(r"https://login\.salesforce\.com/services/data/v58\.0/sobjects/Account/describe"),
        json={
            "fields": [
                {"name": "Id", "type": "id"},
                {"name": "Name", "type": "string"},
                {"name": "Industry", "type": "string"},
            ]
        },
        status=200,
    )
    responses.add(
        responses.GET,
        re.compile(r"https://login\.salesforce\.com/services/data/v58\.0/query"),
        json={
            "totalSize": 2,
            "records": [
                {"Id": "001", "Name": "Acme", "Industry": "Tech"},
                {"Id": "002", "Name": "Globex", "Industry": "Manufacturing"},
            ],
            "done": True,
        },
        status=200,
    )
    batch = salesforce.read_object(cfg={"api_key": "fake-token"}, limit=500)
    assert batch.headers == ["Id", "Name", "Industry"]
    assert len(batch.rows) == 2
    assert batch.total_rows == 2


@responses.activate
def test_salesforce_describe_failure_fail_closed():
    responses.add(
        responses.GET,
        re.compile(r"https://login\.salesforce\.com/services/data/v58\.0/sobjects/Account/describe"),
        json={"message": "INSUFFICIENT_ACCESS"},
        status=403,
    )
    with pytest.raises(RuntimeError, match="Describe is required"):
        salesforce.read_object(cfg={"api_key": "fake-token"}, limit=10)


@responses.activate
def test_hubspot_describe_properties_paginates():
    responses.add(
        responses.GET,
        re.compile(r"https://api\.hubapi\.com/crm/v3/properties/contacts"),
        json={
            "results": [{"name": "email", "type": "string"}],
            "paging": {"next": {"after": "cursor-2"}},
        },
        status=200,
    )
    responses.add(
        responses.GET,
        re.compile(r"https://api\.hubapi\.com/crm/v3/properties/contacts"),
        json={
            "results": [{"name": "custom_late_prop", "type": "string"}],
        },
        status=200,
    )
    props = hubspot.describe_properties({"api_key": "fake-token"}, "contacts")
    names = {p["name"] for p in props}
    assert "email" in names
    assert "custom_late_prop" in names
    assert len(responses.calls) == 2
    assert "after=cursor-2" in responses.calls[1].request.url



@responses.activate
def test_hubspot_probe_success():
    responses.add(
        responses.GET,
        re.compile(r"https://api\.hubapi\.com/crm/v3/objects/contacts"),
        json={"results": [{"id": "1", "properties": {"email": "a@b.com"}}]},
        status=200,
    )
    ok, msg = hubspot.test_hubspot(api_key="fake-token")
    assert ok is True
    assert "reachable" in msg.lower()


@responses.activate
def test_hubspot_read_object():
    responses.add(
        responses.GET,
        re.compile(r"https://api\.hubapi\.com/crm/v3/properties/contacts"),
        json={
            "results": [
                {"name": "email", "type": "string"},
                {"name": "firstname", "type": "string"},
            ]
        },
        status=200,
    )
    responses.add(
        responses.GET,
        re.compile(r"https://api\.hubapi\.com/crm/v3/objects/contacts"),
        json={
            "results": [
                {
                    "id": "101",
                    "properties": {"email": "alice@example.com", "firstname": "Alice"},
                },
                {
                    "id": "102",
                    "properties": {"email": "bob@example.com", "firstname": "Bob"},
                },
            ],
        },
        status=200,
    )
    batch = hubspot.read_object(cfg={"api_key": "fake-token"}, limit=100)
    assert "id" in batch.headers
    assert "email" in batch.headers
    assert len(batch.rows) == 2
    assert batch.total_rows is None


@responses.activate
def test_salesforce_soql_orders_by_identity():
    responses.add(
        responses.GET,
        re.compile(r"https://login\.salesforce\.com/services/data/v58\.0/sobjects/Account/describe"),
        json={
            "fields": [{"name": "Id", "type": "id"}, {"name": "Name", "type": "string"}]
        },
        status=200,
    )
    responses.add(
        responses.GET,
        re.compile(r"https://login\.salesforce\.com/services/data/v58\.0/query"),
        json={"totalSize": 1, "records": [{"Id": "001", "Name": "Acme"}], "done": True},
        status=200,
    )
    salesforce.read_object(cfg={"api_key": "fake-token"}, limit=10)
    q = responses.calls[1].request.params.get("q") or ""
    assert "ORDER BY Id" in q


@responses.activate
def test_hubspot_describe_failure_blocks_list_read():
    responses.add(
        responses.GET,
        re.compile(r"https://api\.hubapi\.com/crm/v3/properties/contacts"),
        json={"message": "internal error"},
        status=500,
    )
    with pytest.raises(RuntimeError, match="Describe is required"):
        hubspot.read_object(cfg={"api_key": "fake-token"}, limit=10)
    assert all("objects/contacts" not in c.request.url for c in responses.calls)


@responses.activate
def test_hubspot_repeated_after_cursor_fail_closed():
    responses.add(
        responses.GET,
        re.compile(r"https://api\.hubapi\.com/crm/v3/properties/contacts"),
        json={"results": [{"name": "email", "type": "string"}]},
        status=200,
    )
    responses.add(
        responses.GET,
        re.compile(r"https://api\.hubapi\.com/crm/v3/objects/contacts"),
        json={
            "results": [{"id": "1", "properties": {"email": "a@b.com"}}],
            "paging": {"next": {"after": "same"}},
        },
        status=200,
    )
    responses.add(
        responses.GET,
        re.compile(r"https://api\.hubapi\.com/crm/v3/objects/contacts"),
        json={
            "results": [{"id": "2", "properties": {"email": "c@d.com"}}],
            "paging": {"next": {"after": "same"}},
        },
        status=200,
    )
    with pytest.raises(RuntimeError, match="repeated an after cursor"):
        hubspot.read_object(cfg={"api_key": "fake-token"}, limit=100)


@responses.activate
def test_stripe_probe_success():
    responses.add(
        responses.GET,
        re.compile(r"https://api\.stripe\.com/v1/account"),
        json={"id": "acct_123", "email": "ops@example.com"},
        status=200,
    )
    ok, msg = stripe.test_stripe(api_key="sk_test_123")
    assert ok is True
    assert "reachable" in msg.lower()


@responses.activate
def test_stripe_read_customers():
    responses.add(
        responses.GET,
        re.compile(r"https://api\.stripe\.com/v1/customers"),
        json={
            "data": [
                {"id": "cus_1", "email": "a@example.com", "name": "Alice"},
                {"id": "cus_2", "email": "b@example.com", "name": "Bob"},
            ],
            "has_more": False,
        },
        status=200,
    )
    batch = stripe.read_object(cfg={"api_key": "sk_test_123"}, limit=10)
    assert "id" in batch.headers
    assert len(batch.rows) == 2


@responses.activate
def test_stripe_writer_creates_customers():
    import connectors.stripe_writer as stripe_writer

    responses.add(
        responses.POST,
        re.compile(r"https://api\.stripe\.com/v1/customers"),
        json={"id": "cus_1"},
        status=200,
    )
    responses.add(
        responses.POST,
        re.compile(r"https://api\.stripe\.com/v1/customers"),
        json={"id": "cus_2"},
        status=200,
    )
    r = stripe_writer.write_mapped_rows(
        api_key="sk_test_123",
        table_name="customers",
        headers=["name", "email"],
        data_rows=[["Alice", "alice@example.com"], ["Bob", "bob@example.com"]],
        mappings=[
            {"source": "name", "target": "name", "transform": "direct"},
            {"source": "email", "target": "email", "transform": "direct"},
        ],
        write_mode="insert",
    )
    assert r.ok
    assert r.rows_written == 2
    assert r.table_name == "customers"


@responses.activate
def test_stripe_writer_updates_by_id():
    import connectors.stripe_writer as stripe_writer

    responses.add(
        responses.POST,
        re.compile(r"https://api\.stripe\.com/v1/customers/cus_existing"),
        json={"id": "cus_existing"},
        status=200,
    )
    r = stripe_writer.write_mapped_rows(
        api_key="sk_test_123",
        table_name="customers",
        headers=["id", "name"],
        data_rows=[["cus_existing", "New Name"]],
        mappings=[
            {"source": "id", "target": "id", "transform": "direct"},
            {"source": "name", "target": "name", "transform": "direct"},
        ],
        write_mode="upsert",
        conflict_columns=["id"],
    )
    assert r.ok
    assert r.rows_written == 1
    assert "cus_existing" in responses.calls[0].request.url


@responses.activate
def test_stripe_writer_quarantines_bad_rows():
    import connectors.stripe_writer as stripe_writer

    responses.add(
        responses.POST,
        re.compile(r"https://api\.stripe\.com/v1/customers"),
        json={"error": {"message": "invalid"}},
        status=400,
    )
    responses.add(
        responses.POST,
        re.compile(r"https://api\.stripe\.com/v1/customers"),
        json={"id": "cus_ok"},
        status=200,
    )
    r = stripe_writer.write_mapped_rows(
        api_key="sk_test_123",
        table_name="customers",
        headers=["name"],
        data_rows=[["Bad"], ["Good"]],
        mappings=[{"source": "name", "target": "name", "transform": "direct"}],
        write_mode="insert",
        error_policy="quarantine",
    )
    assert r.ok
    assert r.rows_written == 1
    assert r.rejected_rows == 1


@responses.activate
def test_stripe_upsert_without_conflict_refuses_create_invent():
    import connectors.stripe_writer as stripe_writer

    r = stripe_writer.write_mapped_rows(
        api_key="sk_test_123",
        table_name="customers",
        headers=["name"],
        data_rows=[["Alice"]],
        mappings=[{"source": "name", "target": "name", "transform": "direct"}],
        write_mode="upsert",
        conflict_columns=[],
        error_policy="fail",
    )
    assert r.ok is False
    assert "refuse" in (r.error or "").lower()
    assert len(responses.calls) == 0


@responses.activate
def test_stripe_writer_auth_fails_closed():
    import connectors.stripe_writer as stripe_writer

    responses.add(
        responses.POST,
        re.compile(r"https://api\.stripe\.com/v1/customers"),
        json={"error": {"message": "unauth"}},
        status=401,
    )
    r = stripe_writer.write_mapped_rows(
        api_key="sk_test_123",
        table_name="customers",
        headers=["name"],
        data_rows=[["Alice"]],
        mappings=[{"source": "name", "target": "name", "transform": "direct"}],
        write_mode="insert",
        error_policy="quarantine",
    )
    assert r.ok is False
    assert "authentication" in (r.error or "").lower()


@responses.activate
def test_airtable_writer_creates_records():
    responses.add(
        responses.POST,
        re.compile(r"https://api\.airtable\.com/v0/appXXX/Contacts"),
        json={"records": [{"id": "rec1"}]},
        status=200,
    )
    r = airtable_writer.write_mapped_rows(
        api_key="patXXX",
        database="appXXX",
        table_name="Contacts",
        headers=["name"],
        data_rows=[["Alice"]],
        mappings=[{"source": "name", "target": "name", "transform": "direct"}],
        write_mode="upsert",
    )
    assert r.ok
    assert r.rows_written == 1
    assert r.table_name == "Contacts"


@responses.activate
def test_airtable_writer_upserts_by_conflict_column():
    responses.add(
        responses.PATCH,
        re.compile(r"https://api\.airtable\.com/v0/appXXX/Contacts"),
        json={"records": [{"id": "rec2"}]},
        status=200,
    )
    r = airtable_writer.write_mapped_rows(
        api_key="patXXX",
        database="appXXX",
        table_name="Contacts",
        headers=["email", "name"],
        data_rows=[["a@example.com", "Alice"]],
        mappings=[
            {"source": "email", "target": "email", "transform": "direct"},
            {"source": "name", "target": "name", "transform": "direct"},
        ],
        write_mode="upsert",
        conflict_columns=["email"],
    )
    assert r.ok
    assert r.rows_written == 1


@responses.activate
def test_airtable_writer_updates_by_record_id():
    responses.add(
        responses.PATCH,
        re.compile(r"https://api\.airtable\.com/v0/appXXX/Contacts"),
        json={"records": [{"id": "rec_existing"}]},
        status=200,
    )
    r = airtable_writer.write_mapped_rows(
        api_key="patXXX",
        database="appXXX",
        table_name="Contacts",
        headers=["id", "name"],
        data_rows=[["rec_existing", "New Name"]],
        mappings=[
            {"source": "id", "target": "id", "transform": "direct"},
            {"source": "name", "target": "name", "transform": "direct"},
        ],
        write_mode="upsert",
    )
    assert r.ok
    assert r.rows_written == 1


@responses.activate
def test_airtable_writer_auth_fails_closed():
    responses.add(
        responses.POST,
        re.compile(r"https://api\.airtable\.com/v0/appXXX/Contacts"),
        json={"error": "Unauthorized"},
        status=401,
    )
    r = airtable_writer.write_mapped_rows(
        api_key="patXXX",
        database="appXXX",
        table_name="Contacts",
        headers=["name"],
        data_rows=[["Alice"]],
        mappings=[{"source": "name", "target": "name", "transform": "direct"}],
        write_mode="upsert",
        error_policy="quarantine",
    )
    assert r.ok is False
    assert "authentication" in (r.error or "").lower()


@responses.activate
def test_shopify_writer_creates_customer():
    responses.add(
        responses.POST,
        re.compile(r"https://myshop\.myshopify\.com/admin/api/2024-04/customers\.json"),
        json={"customer": {"id": "123"}},
        status=200,
    )
    r = shopify_writer.write_mapped_rows(
        host="myshop.myshopify.com",
        api_key="shpat_xxx",
        table_name="customers",
        headers=["first_name", "email"],
        data_rows=[["Alice", "alice@example.com"]],
        mappings=[
            {"source": "first_name", "target": "first_name", "transform": "direct"},
            {"source": "email", "target": "email", "transform": "direct"},
        ],
        write_mode="upsert",
    )
    assert r.ok
    assert r.rows_written == 1
    assert r.table_name == "customers"


@responses.activate
def test_shopify_writer_updates_by_id():
    responses.add(
        responses.PUT,
        re.compile(r"https://myshop\.myshopify\.com/admin/api/2024-04/customers/456\.json"),
        json={"customer": {"id": "456"}},
        status=200,
    )
    r = shopify_writer.write_mapped_rows(
        host="myshop.myshopify.com",
        api_key="shpat_xxx",
        table_name="customers",
        headers=["id", "first_name"],
        data_rows=[["456", "New"]],
        mappings=[
            {"source": "id", "target": "id", "transform": "direct"},
            {"source": "first_name", "target": "first_name", "transform": "direct"},
        ],
        write_mode="upsert",
        conflict_columns=["id"],
    )
    assert r.ok
    assert r.rows_written == 1


@responses.activate
def test_shopify_writer_missing_shop_fails():
    r = shopify_writer.write_mapped_rows(
        api_key="shpat_xxx",
        table_name="customers",
        headers=["first_name"],
        data_rows=[["Alice"]],
        mappings=[{"source": "first_name", "target": "first_name", "transform": "direct"}],
        write_mode="upsert",
    )
    assert r.ok is False
    assert "shop" in (r.error or "").lower()
