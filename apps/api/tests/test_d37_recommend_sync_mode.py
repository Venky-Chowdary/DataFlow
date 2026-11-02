"""D37 regression tests — recommend_sync_mode must not route history
workloads to a mode that keeps only one row per key.

P3.6: needs_history=true + cursor + primary_key recommended
incremental_deduped (upsert — one row per key, history destroyed).
It must recommend scd2. Decision-table coverage of the matrix.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.ai.copilot.tools import DataPilotTools


def _recommend(**kwargs):
    res = DataPilotTools().execute("recommend_sync_mode", kwargs)
    assert res.success, res.error
    return res.output


@pytest.mark.parametrize(
    "workload",
    [
        "",
        "keep an audit trail of every change",
        "history of changes over time",
    ],
)
def test_history_workload_recommends_scd2(workload):
    out = _recommend(
        workload=workload,
        has_cursor=True,
        has_primary_key=True,
        needs_history=True,
    )
    assert out["recommended_mode"] == "scd2"
    assert "version" in out["reason"].lower() or "history" in out["reason"].lower()
    assert out["requires"]["primary_key"] is True


def test_history_never_recommends_one_row_per_key_modes():
    """needs_history=true can never land on a mode that collapses history."""
    for has_cursor in (False, True):
        out = _recommend(
            workload="track changes",
            has_cursor=has_cursor,
            has_primary_key=True,
            needs_history=True,
        )
        assert out["recommended_mode"] != "incremental_append"
    out = _recommend(
        workload="track changes",
        has_cursor=True,
        has_primary_key=True,
        needs_history=True,
    )
    assert out["recommended_mode"] == "scd2"


def test_scd2_label_and_requires():
    out = _recommend(
        workload="slowly changing customer attributes",
        has_cursor=True,
        has_primary_key=True,
        needs_history=True,
    )
    assert out["recommended_mode"] == "scd2"
    assert out["label"]
    assert out["requires"]["primary_key"] is True
    # SCD2 does full-snapshot merges; it does not advance a cursor.
    assert out["requires"]["cursor"] is False
    assert out["requires"]["cdc_log_access"] is False


def test_upsert_wording_without_history_stays_deduped():
    out = _recommend(
        workload="upsert orders into warehouse",
        has_cursor=True,
        has_primary_key=True,
        needs_history=False,
    )
    assert out["recommended_mode"] == "incremental_deduped"


def test_cdc_and_delete_branches_unchanged():
    out = _recommend(
        workload="orders, updates and deletes, near-real-time",
        has_cursor=True,
        has_primary_key=True,
        needs_history=True,
    )
    assert out["recommended_mode"] == "cdc_incremental"
    out = _recommend(
        workload="rows get deleted from source",
        has_cursor=False,
        has_primary_key=True,
        needs_history=False,
    )
    assert out["recommended_mode"] == "full_refresh_mirror"
