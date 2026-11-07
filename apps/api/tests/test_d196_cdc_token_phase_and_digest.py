"""D196 unit pins beside the live repro (tests/test_d196_sqlserver_change_tracking_cdc_live.py).

The exactly-once decision and the CDC value proof must read SQL Server's
JSON-string tokens and GUID spellings the same way on both sides, without
loosening what a real conflict or a real difference does.
"""

from __future__ import annotations

import json

import pytest

from services import cdc_value_digest as cvd
from services.cdc_exactly_once import (
    ExactlyOnceRouteError,
    decide_eos_apply,
    extract_cdc_phase,
    extract_snapshot_last_pk,
)


def _ct(phase: str, last_pk: str = "") -> str:
    token = {"kind": "mssql-ct", "table": "t", "version": 16, "phase": phase, "offset": 4}
    if last_pk:
        token["last_pk"] = last_pk
    return json.dumps(token, separators=(",", ":"))


def test_a_json_string_token_keeps_its_snapshot_phase():
    assert extract_cdc_phase(_ct("snapshot", "4")) == "snapshot"
    assert extract_cdc_phase(_ct("streaming")) == "streaming"
    assert extract_snapshot_last_pk(_ct("snapshot", "1096")) == "1096"


def test_handoff_at_the_snapshot_version_advances_the_phase():
    action, _ = decide_eos_apply(
        incoming_lsn="00000000000000000016", dest_lsn="00000000000000000016",
        incoming_phase="streaming", dest_phase="snapshot",
    )
    assert action == "handoff_phase"


def test_next_snapshot_chunk_at_the_same_version_applies():
    action, _ = decide_eos_apply(
        incoming_lsn="00000000000000000016", dest_lsn="00000000000000000016",
        incoming_phase="snapshot", dest_phase="snapshot",
        incoming_checksum="b", dest_checksum="a",
        incoming_last_pk="2096", dest_last_pk="1096",
    )
    assert action == "apply"


@pytest.mark.parametrize("last_pk", ["1096", "500", ""])
def test_a_redelivered_chunk_with_another_payload_is_still_refused(last_pk):
    with pytest.raises(ExactlyOnceRouteError) as err:
        decide_eos_apply(
            incoming_lsn="00000000000000000016", dest_lsn="00000000000000000016",
            incoming_phase="snapshot", dest_phase="snapshot",
            incoming_checksum="b", dest_checksum="a",
            incoming_last_pk=last_pk, dest_last_pk="1096",
        )
    # The message says what happened: nothing applied, nothing quarantined.
    assert "not applied" in str(err.value) and "not written to quarantine" in str(err.value)


def test_streaming_redelivery_with_another_payload_is_still_refused():
    with pytest.raises(ExactlyOnceRouteError):
        decide_eos_apply(
            incoming_lsn="00000000000000000016", dest_lsn="00000000000000000016",
            incoming_phase="streaming", dest_phase="streaming",
            incoming_checksum="b", dest_checksum="a",
        )


def _m(source: str, transform: str) -> dict:
    return {"source": source, "target": source, "transform": transform}


def test_guid_case_is_one_value_and_another_guid_is_not():
    maps = [_m("id", "integer"), _m("g", "uuid")]
    assert cvd._identity_pairs(maps) == [("id", "id"), ("g", "g")]
    carries = cvd._declared_carries(maps)
    src = cvd._apply_carries([{"id": "1", "g": "6F9619FF-8B86-D011-B42D-00C04FC964FF"}], carries)
    same = [{"id": 1, "g": "6f9619ff-8b86-d011-b42d-00c04fc964ff"}]
    other = [{"id": 1, "g": "6f9619ff-8b86-d011-b42d-00c04fc964fe"}]
    types = {"id": "BIGINT", "g": "CHAR(36)"}
    assert cvd.compare_row_sets(src, same, ["id", "g"], engine="mysql", dest_types=types).matched
    assert not cvd.compare_row_sets(src, other, ["id", "g"], engine="mysql", dest_types=types).matched


def test_literal_sentinel_text_is_not_null_in_the_digest():
    from services.reconciliation import canonical_checksum

    def fp(v):
        return canonical_checksum([{"n": v}], ["n"], dest_db_type="postgresql", dest_types={"n": "TEXT"})

    assert fp("__DF_SQL_NULL__") == fp(None)  # the wire spelling of NULL
    assert fp("__DF_ESC____DF_SQL_NULL__") != fp(None)  # source text that spells it
