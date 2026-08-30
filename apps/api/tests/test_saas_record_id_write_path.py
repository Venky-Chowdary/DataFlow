"""SaaS upsert ids use present_cell_text, not ``if val`` / ``str(val)``.

``if val`` dropped integer ``0``. Reader-null sentinels became URL ids.
``str(True)`` invented ``True`` so dest ``true`` missed upsert identity.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.saas_common import saas_record_id  # noqa: E402
from connectors.stripe_writer import _row_id  # noqa: E402
from services.value_serializer import (  # noqa: E402
    DF_MISSING_SENTINEL,
    SQL_NULL_SENTINEL,
    Missing,
)

_ABSENT = (None, "", "   ", SQL_NULL_SENTINEL, "__df_ddb_null__", Missing, DF_MISSING_SENTINEL)


def test_saas_record_id_absent_is_none():
    for wire in _ABSENT:
        assert saas_record_id(wire) is None, wire


def test_saas_record_id_zero_and_true():
    assert saas_record_id(0) == "0"
    assert saas_record_id(False) == "false"
    assert saas_record_id(True) == "true"
    assert saas_record_id("true") == "true"
    assert saas_record_id(True) != "True"
    assert saas_record_id("cus_123") == "cus_123"


def test_stripe_row_id_refuses_sentinel_and_keeps_zero():
    assert _row_id({"id": SQL_NULL_SENTINEL}, ["id"]) is None
    assert _row_id({"id": None}, ["id"]) is None
    assert _row_id({"id": 0}, ["id"]) == "0"
    assert _row_id({"id": True}, ["id"]) == "true"
    assert _row_id({"id": "cus_abc"}, ["id"]) == "cus_abc"
    assert _row_id({"email": "a@b.test"}, ["email"]) is None
