"""G11 re-review L3: an overwrite that recreates the table uses the full names.

BigQuery and Snowflake overwrites drop and recreate the table. A single-stream
run adopted the earlier run's legacy column (``customername``) before the drop,
while a multi-stream run dropped first and then used the full name
(``CustomerName``): the same Map produced different tables by route shape.
Now both use the full name on such an overwrite (the old column goes with the
dropped table), and Validate's g23d note says so. Engines whose overwrite
empties the table in place (PostgreSQL, MySQL, ...) keep adopting.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

MAPS = [{"source": "a", "target": "CustomerID"}, {"source": "b", "target": "CustomerName"}]
INFO = {"connected": True, "table_exists": True,
        "schema": {"customerid": "INT64", "customername": "STRING"}}


def _request(sync_mode, dest="bigquery", streams=None):
    from src.transfer.models import EndpointConfig, TransferRequest

    return TransferRequest(
        mappings=[dict(m) for m in MAPS],
        source=EndpointConfig(kind="database", format="sqlite", database="/x.db", table="t"),
        destination=EndpointConfig(kind="database", format=dest, database="d", table="t"),
        sync_mode=sync_mode, stream_contracts=streams or [],
    )


@pytest.mark.parametrize("dest", ["bigquery", "snowflake"])
def test_single_and_multi_stream_overwrite_agree_on_full_names(dest):
    from services.source_filter_gate import request_refusal
    from services.target_name_legacy import prepare_stream_targets

    single = _request("full_refresh_overwrite", dest)
    with patch("src.transfer.endpoint_intelligence.introspect_endpoint", return_value=INFO):
        assert request_refusal(single) == ""
        stream_rows, _ack = prepare_stream_targets(
            single.destination, MAPS, "full_refresh_overwrite", stream_name="t2")
    assert [m["target"] for m in single.mappings] == ["CustomerID", "CustomerName"]
    assert [m["target"] for m in stream_rows] == ["CustomerID", "CustomerName"]


def test_append_into_the_existing_table_still_adopts():
    from services.source_filter_gate import request_refusal

    request = _request("full_refresh_append")
    with patch("src.transfer.endpoint_intelligence.introspect_endpoint", return_value=INFO):
        assert request_refusal(request) == ""
    assert [m["target"] for m in request.mappings] == ["customerid", "customername"]


def test_validate_note_says_the_overwrite_replaces_the_old_column():
    from services.preflight_transfer_policy import run_transfer_policy_gates

    gates = {g["id"]: g for g in run_transfer_policy_gates(
        sync_mode="full_refresh_overwrite", dest_type="bigquery", source_type="sqlite",
        source_kind="database", mappings=[dict(m) for m in MAPS], destination_table="t",
        destination_columns=["customerid", "customername"],
    )}
    note = gates["g23d_existing_target_columns"]["message"]
    assert "recreates the table" in note and "'CustomerName'" in note, note


def test_validate_and_execute_hash_the_same_rows_on_overwrite():
    from services.target_name_legacy import adopt_rows_for_run

    rows = adopt_rows_for_run("bigquery", MAPS, list(INFO["schema"]), "full_refresh_overwrite")
    assert [m["target"] for m in rows] == ["CustomerID", "CustomerName"]
