"""G11 re-review L1: collisions are judged over every final Map row.

The adoption collision check compared adopted rows only with each other:

* BigQuery ``Name`` + ``name`` with an existing ``name`` column: ``Name``
  adopts ``name`` and ``name`` is that column already. Validate passed and
  Execute refused (an inconsistent pair);
* MySQL: a 65-character target adopted onto the earlier run's 63-character
  column plus a row targeting that 63-character name literally. Both passed;
  ``resolve_target_columns`` then de-duplicated the column and one source
  silently won.

Now the final rows (adopted and literal) are checked together, by the same
function at Validate and at Execute.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

CUT = "c" * 63
CASES = [
    ("bigquery", ["Name", "name"], {"id": "INT64", "name": "STRING"}),
    ("mysql", ["c" * 65, CUT], {"id": "BIGINT", CUT: "BIGINT"}),
]


def _maps(targets):
    return [{"source": f"s{i}", "target": t, "confidence": 1.0} for i, t in enumerate(targets)]


@pytest.mark.parametrize(("dest", "targets", "schema"), CASES)
def test_validate_refuses_two_rows_on_one_existing_column(dest, targets, schema):
    from services.preflight_transfer_policy import run_transfer_policy_gates

    gates = {g["id"]: g for g in run_transfer_policy_gates(
        sync_mode="full_refresh_append", dest_type=dest, source_type="sqlite",
        source_kind="database", mappings=_maps(targets), destination_table="t",
        destination_columns=list(schema),
    )}
    gate = gates.get("g23c_target_names") or {}
    assert gate.get("status") == "block", sorted(gates)
    assert "target_name_collision" in [i["code"] for i in gate["details"]["issues"]]


@pytest.mark.parametrize(("dest", "targets", "schema"), CASES)
def test_execute_refuses_the_same(dest, targets, schema):
    from services.source_filter_gate import request_refusal
    from src.transfer.models import EndpointConfig, TransferRequest

    request = TransferRequest(
        mappings=_maps(targets),
        source=EndpointConfig(kind="database", format="sqlite", database="/x.db", table="s"),
        destination=EndpointConfig(kind="database", format=dest, database="d", table="t"),
        sync_mode="full_refresh_append",
    )
    info = {"connected": True, "table_exists": True, "schema": dict(schema)}
    with patch("src.transfer.endpoint_intelligence.introspect_endpoint", return_value=info):
        refused = request_refusal(request)
    assert "would all write the existing column" in refused or "case" in refused, refused


def test_one_adopted_row_alone_is_fine():
    from services.preflight_transfer_policy import run_transfer_policy_gates

    gates = {g["id"] for g in run_transfer_policy_gates(
        sync_mode="full_refresh_append", dest_type="mysql", source_type="sqlite",
        source_kind="database", mappings=_maps(["id", "c" * 65]), destination_table="t",
        destination_columns=["id", CUT],
    )}
    assert "g23c_target_names" not in gates and "g23d_existing_target_columns" in gates
