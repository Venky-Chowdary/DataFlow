"""The exactly-once CDC apply runs the Map transform on every cell (D177 chain).

``delivery_guarantee=auto`` selects the dest-owned exactly-once apply for a
MySQL -> PostgreSQL ``cdc_position`` route. It bound the raw source cell, so a
declared ``assume_timezone:`` zone never reached a ``TIMESTAMPTZ`` column: the
bind refused the zoneless wall-clock and the job ended failed with 0 rows even
with ``source_timezone`` set.
"""

from __future__ import annotations

import pytest

from connectors.cdc_eos_sa import _apply_mapped_transforms
from services.value_serializer import DF_MISSING_SENTINEL, SQL_NULL_SENTINEL

ZONE = "assume_timezone:America/New_York"


def _m(source: str, transform: str = "", target_type: str = "") -> dict:
    return {"source": source, "target": source, "transform": transform, "target_type": target_type}


def test_eos_apply_attaches_the_declared_zone():
    out = _apply_mapped_transforms(
        {"ts": "2024-01-02 03:04:05", "n": "7", "gone": None, "keep": DF_MISSING_SENTINEL},
        [_m("ts", ZONE, "TIMESTAMPTZ"), _m("n", "integer", "BIGINT"), _m("gone", "integer"),
         _m("keep", "integer")],
        {"ts": "TIMESTAMP", "n": "INT"},
    )
    assert str(out["ts"]).startswith("2024-01-02T03:04:05-05:00"), out
    assert out["n"] == 7
    assert out["gone"] is None
    assert out["keep"] == DF_MISSING_SENTINEL  # sparse-CDC "not sent" is not converted


def test_eos_apply_decodes_the_sql_null_sentinel_for_a_transformed_cell():
    out = _apply_mapped_transforms(
        {"ts": SQL_NULL_SENTINEL}, [_m("ts", ZONE, "TIMESTAMPTZ")], {"ts": "TIMESTAMP"}
    )
    assert out["ts"] is None


def test_eos_apply_fails_the_batch_on_a_cell_it_cannot_convert():
    with pytest.raises(ValueError, match="ts"):
        _apply_mapped_transforms(
            {"ts": "not a date"}, [_m("ts", ZONE, "TIMESTAMPTZ")], {"ts": "TIMESTAMP"}
        )
