"""A reverse-ETL body carries typed cells, so the SaaS request serializes them.

``requests`` encodes only JSON-native types. A mapped DECIMAL cell binds as
``Decimal`` and a temporal cell as ``datetime``, so a batch holding one died
with ``Object of type Decimal is not JSON serializable`` — the whole chunk,
not the cell — on every SaaS writer (Salesforce, HubSpot, Airtable, Zendesk,
Stripe, Shopify, Notion all post through ``saas_common.request``).
"""

from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
from uuid import UUID

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors import saas_common  # noqa: E402
from services.value_serializer import (  # noqa: E402
    DF_MISSING_SENTINEL,
    Missing,
)


class _Resp:
    status_code = 200

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return {}


def _sent_body(data: dict | None) -> dict | None:
    captured: dict = {}

    def _request(**kwargs):
        captured.update(kwargs)
        return _Resp()

    with patch.object(saas_common.requests, "request", _request):
        saas_common.request(
            method="POST", url="https://example.invalid/x", token="t", data=data
        )
    return captured.get("json")


def test_decimal_cell_serializes_exactly_not_as_float():
    body = _sent_body({"records": [{"AnnualRevenue": Decimal("500.25")}]})
    assert body == {"records": [{"AnnualRevenue": "500.25"}]}
    # Binary float would answer 0.1000000000000000055511151231257827.
    assert _sent_body({"v": Decimal("0.1")}) == {"v": "0.1"}
    # Scale is part of the value: 2 decimal places stay 2 decimal places.
    assert _sent_body({"v": Decimal("10.50")}) == {"v": "10.50"}
    assert _sent_body({"v": Decimal("-0.0000000001")}) == {"v": "-0.0000000001"}
    assert _sent_body({"v": Decimal("12345678901234567890.1234567890")}) == {
        "v": "12345678901234567890.1234567890"
    }


def test_temporal_uuid_and_bytes_cells_serialize():
    body = _sent_body(
        {
            "ts": datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
            "d": date(2024, 12, 31),
            "u": UUID("12345678-1234-5678-1234-567812345678"),
            "b": b"\x00\x01",
        }
    )
    assert body == {
        "ts": "2024-12-31T23:59:59+00:00",
        "d": "2024-12-31",
        "u": "12345678-1234-5678-1234-567812345678",
        "b": "AAE=",
    }


def test_json_native_cells_are_untouched():
    payload = {
        "allOrNone": False,
        "records": [
            {"attributes": {"type": "Account"}, "Name": "Initech", "Qty": 0},
            {"Name": "Umbrella", "Active": True, "Ratio": 1.5, "Note": None},
        ],
    }
    assert _sent_body(payload) == payload
    assert _sent_body(None) is None


@pytest.mark.parametrize(
    "cell",
    [
        Missing,
        DF_MISSING_SENTINEL,
        float("nan"),
        float("inf"),
        Decimal("NaN"),
    ],
)
def test_absent_and_non_finite_cells_are_refused_not_wired(cell):
    """Fail closed: a CRM record must not receive NaN or the missing sentinel."""
    with pytest.raises(ValueError):
        _sent_body({"records": [{"Amount": cell}]})
