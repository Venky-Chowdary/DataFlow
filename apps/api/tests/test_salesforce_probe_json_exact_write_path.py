"""Salesforce uniqueness probe uses load_http_json, not Response.json().

stdlib Response.json() collapsed a long fraction in an External Id before
Validate showed the duplicate key. Findings now use the cell_to_string
wire (long fraction stays digits; 1.5 is "1.5"). Id-only identity still
skips the query.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.source_duplicate_probe import _salesforce_duplicates  # noqa: E402

LONG = "1.234567890123456789"
IEEE_LOSSY = 9007199254740993


class _Resp:
    def __init__(self, payload: str) -> None:
        self.text = payload
        self.content = payload.encode("utf-8")

    def raise_for_status(self) -> None:
        return None

    def json(self) -> Any:
        return json.loads(self.text)


def _patch_sf(monkeypatch: pytest.MonkeyPatch, payload: str) -> None:
    monkeypatch.setattr(
        "connectors.salesforce._access", lambda cfg: ("tok", "https://example.salesforce.com")
    )
    monkeypatch.setattr("connectors.salesforce._validate_api_name", lambda name, _label: name)
    monkeypatch.setattr(
        "connectors.saas_common.request",
        lambda **_kw: _Resp(payload),
    )


def test_external_id_long_fraction_stays_decimal(monkeypatch: pytest.MonkeyPatch):
    _patch_sf(
        monkeypatch,
        f'{{"records":[{{"External_Id__c": {LONG}, "dupes": 2}}]}}',
    )
    findings, status, _msg = _salesforce_duplicates(
        {}, "Account", ["External_Id__c"]
    )
    assert status == "ran"
    assert findings[0]["value"] == LONG
    stock = json.loads(f'{{"External_Id__c": {LONG}}}')["External_Id__c"]
    assert findings[0]["value"] != str(stock)
    assert findings[0]["count"] == 2


def test_ieee_exact_fraction_stays_float(monkeypatch: pytest.MonkeyPatch):
    _patch_sf(
        monkeypatch,
        '{"records":[{"External_Id__c": 1.5, "dupes": 3}]}',
    )
    findings, status, _msg = _salesforce_duplicates(
        {}, "Account", ["External_Id__c"]
    )
    assert status == "ran"
    assert findings[0]["value"] == "1.5"


def test_int_past_ieee_mantissa_stays_int(monkeypatch: pytest.MonkeyPatch):
    _patch_sf(
        monkeypatch,
        f'{{"records":[{{"External_Id__c": {IEEE_LOSSY}, "dupes": 2}}]}}',
    )
    findings, status, _msg = _salesforce_duplicates(
        {}, "Account", ["External_Id__c"]
    )
    assert status == "ran"
    assert findings[0]["value"] == str(IEEE_LOSSY)


def test_platform_id_skips_query(monkeypatch: pytest.MonkeyPatch):
    def _boom(**_kw: object) -> None:
        raise AssertionError("Id uniqueness must not query")

    monkeypatch.setattr("connectors.saas_common.request", _boom)
    findings, status, msg = _salesforce_duplicates({}, "Account", ["Id"])
    assert status == "ran"
    assert findings == []
    assert "Id" in msg
