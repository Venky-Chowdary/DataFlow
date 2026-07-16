"""Unit tests for source-only SaaS connectors using mocked HTTP responses."""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest
import responses

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import connectors.hubspot as hubspot  # noqa: E402
import connectors.salesforce as salesforce  # noqa: E402
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
