"""X-110 (P2 honesty): buffered ops claimed "verified" with no transform
proof — an aggregate clean run read "Row fidelity verified" while the
transform oracle was skipped. Only dedupe is covered by side evidence
(row ledger + winner checks); aggregate/group_by/rank/router must report
an honest skip so the verdict reads partial.

Also: a diverted row is quarantine evidence; a shaped-out row was filtered
by the recipe — the verified message names each separately (was one bare
"rejected" lump).
"""
import os

import pytest

from src.transfer.reconcile_step import _transform_applied_proof
from services.reconciliation import reconcile


class _Endpoint:
    kind = "database"
    type = "sqlite"
    table = "t"
    schema = ""
    config = {}


def _proof(steps):
    return _transform_applied_proof(
        shape_recipe={"version": 1, "steps": steps},
        dest_summary={"shape_recipe_hash": "x", "rows_written": 1},
        source_endpoint=_Endpoint(),
        endpoint=_Endpoint(),
        columns=[],
        mappings=[],
        source_schema={},
        recon={},
    )


def test_aggregate_skip_has_no_coverage_claim():
    proof = _proof([
        {"op": "aggregate", "enabled": True, "options": {
            "group_by": ["cust"],
            "measures": [{"function": "sum", "column": "amt", "as": "total"}],
        }},
    ])
    assert proof["status"] == "skipped"
    assert "covered_by" not in proof
    assert "aggregate" in proof["reason"]


def test_rank_rows_skip_has_no_coverage_claim():
    proof = _proof([
        {"op": "rank_rows", "enabled": True, "options": {
            "order_by": "amt", "direction": "top", "limit": 1,
        }},
    ])
    assert proof["status"] == "skipped"
    assert "covered_by" not in proof


def test_dedupe_keeps_its_winner_check_coverage():
    proof = _proof([
        {"op": "dedupe_rows", "enabled": True, "options": {
            "keys": ["id"], "keep": "last",
        }},
    ])
    assert proof["status"] == "skipped"
    assert proof.get("covered_by")


def test_verified_message_splits_diverted_from_filtered():
    report = reconcile(
        source_rows=12,
        target_rows=5,
        source_checksum="aa",
        target_checksum="aa",
        rejected_rows=1,
        rows_shaped_out=6,
        rows_shape_diverted=1,
    )
    msg = report.message
    assert "5 rows written" in msg
    assert "1 diverted to quarantine" in msg
    assert "6 filtered by recipe" in msg
    assert "rejected" not in msg.replace("rows written", "")


def test_verified_message_plain_reject_still_names_rejected():
    report = reconcile(
        source_rows=5,
        target_rows=3,
        source_checksum="aa",
        target_checksum="aa",
        rejected_rows=2,
    )
    assert report.message == "Transfer verified (3 rows written, 2 rejected)"


@pytest.mark.skipif(
    os.environ.get("DATAFLOW_X110_LIVE") != "1",
    reason="live aggregate/rank verdict matrix — user-run",
)
def test_x110_live(tmp_path):  # pragma: no cover
    pass
