"""Temporal / CDC identity / Notion / JSON export / LSN invent refuse."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_generic_sql_datetime_keeps_naive_wall_clock():
    from connectors.generic_sql import _to_sa_value
    from services.type_system import LOGICAL_DATETIME

    naive = datetime(2024, 6, 1, 12, 0, 0)
    out = _to_sa_value(naive, LOGICAL_DATETIME)
    assert out.tzinfo is None
    assert out == naive


def test_generic_sql_timestamptz_refuses_naive():
    from connectors.generic_sql import _to_sa_value

    with pytest.raises(ValueError, match="naive|offset"):
        _to_sa_value(datetime(2024, 6, 1, 12, 0, 0), "TIMESTAMPTZ")


def test_iceberg_timestamptz_refuses_naive():
    import pyarrow as pa
    from connectors.iceberg_writer import _coerce_arrow_cell

    tz_type = pa.timestamp("us", tz="UTC")
    with pytest.raises(ValueError, match="naive|TIMESTAMPTZ"):
        _coerce_arrow_cell(datetime(2024, 6, 1, 12, 0, 0), tz_type, pa)


def test_cdc_primary_key_refuses_id_invent():
    from services.cdc_identity import require_cdc_primary_key

    assert require_cdc_primary_key("order_id", table="orders") == "order_id"
    assert require_cdc_primary_key(["a", "b"], table="orders") == ["a", "b"]
    assert require_cdc_primary_key("a,b", table="orders") == ["a", "b"]
    with pytest.raises(ValueError, match="explicit primary_key"):
        require_cdc_primary_key("", table="orders")
    with pytest.raises(ValueError, match="explicit primary_key"):
        require_cdc_primary_key(None, table="orders")
    with pytest.raises(ValueError, match="explicit primary_key"):
        require_cdc_primary_key([], table="orders")


def test_to_json_value_does_not_invent_int_for_string_type():
    from connectors.writer_common import to_json_value

    assert to_json_value("42", "amt", {"amt": "string"}) == "42"
    assert to_json_value("42", "amt", {}) == "42"
    assert to_json_value("42", "amt", {"amt": "integer"}) == 42


def test_notion_checkbox_refuses_maybe():
    from connectors.notion_writer import _as_property_value

    with pytest.raises(ValueError, match="checkbox|boolean"):
        _as_property_value("maybe", "checkbox", "done", [], 1)
    assert _as_property_value("false", "checkbox", "done", [], 1) == {"checkbox": False}


def test_filter_stale_lsn_skips_none_incoming_when_prior_exists():
    from connectors.writer_common import lsn_is_newer

    assert lsn_is_newer(None, "0/1") is False
    assert lsn_is_newer("0/2", "0/1") is True
    assert lsn_is_newer("0/1", None) is True
