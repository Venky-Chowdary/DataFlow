"""Exactly-once ordering regressions for within-LSN CDC positions."""

from __future__ import annotations

import json

import pytest

from connectors.lsn_guards import (
    compare_lsn,
    extract_cdc_commit_position,
    lsn_family,
    lsn_sort_key,
)
from connectors.oracle_logminer import (
    OracleLogMinerCdc,
    decode_logminer_token,
    encode_logminer_token,
)
from connectors.sqlserver_cdc_native import (
    SqlServerNativeCdc,
    encode_mssql_cdc_token,
)
from services.cdc_exactly_once import (
    DestWmView,
    ExactlyOnceRouteError,
    InMemoryEosStore,
    batch_lsn,
    clamp_job_resume_to_dest,
    decide_eos_apply,
    decide_from_view,
    encode_resume_blob,
    plan_open_session,
)
from services.cdc_snapshot_mode import snapshot_dump_open


def test_oracle_eos_applies_later_rs_id_at_same_scn() -> None:
    first = encode_logminer_token(
        100,
        table="T",
        rs_id=" 0x00000b.00012d4e.0010 ",
        ssn=0,
    )
    later = encode_logminer_token(
        100,
        table="T",
        rs_id=" 0x00000b.00012d4f.0010 ",
        ssn=0,
    )

    assert (
        decide_eos_apply(
            incoming_lsn=batch_lsn(later),
            dest_lsn=batch_lsn(first),
        )[0]
        == "apply"
    )


def test_sqlserver_eos_applies_later_seqval_at_same_lsn() -> None:
    first = encode_mssql_cdc_token(
        "0000002e000001d80030",
        table="T",
        seqval="0000002e000001d80002",
    )
    later = encode_mssql_cdc_token(
        "0000002e000001d80030",
        table="T",
        seqval="0000002e000001d80005",
    )

    assert (
        decide_eos_apply(
            incoming_lsn=batch_lsn(later),
            dest_lsn=batch_lsn(first),
        )[0]
        == "apply"
    )


def test_sqlserver_digit_only_lsn_family_matches_next_hex_lsn() -> None:
    first = encode_mssql_cdc_token("00000025000004500003", table="T")
    later = encode_mssql_cdc_token("0000002a000000100003", table="T")
    first_lsn = batch_lsn(first)
    later_lsn = batch_lsn(later)
    first_family = lsn_family(first_lsn)
    later_family = lsn_family(later_lsn)
    decision = None
    decision_error = None
    try:
        decision = decide_eos_apply(
            incoming_lsn=later_lsn,
            dest_lsn=first_lsn,
        )
    except ExactlyOnceRouteError as exc:
        decision_error = f"{type(exc).__name__}: {exc}"

    assert first_family == later_family, (
        f"families differ: first={first_family}, later={later_family}; "
        f"EOS decision={decision or decision_error}"
    )
    assert decision is not None and decision[0] == "apply"


def test_capture_reader_positions_require_a_completed_window_and_match_head_family():
    oracle = object.__new__(OracleLogMinerCdc)
    sqlserver = object.__new__(SqlServerNativeCdc)

    assert oracle.capture_reader_position() is None
    assert sqlserver.capture_reader_position() is None

    oracle._last_completed_window_scn = 123456
    sqlserver._last_completed_poll_to_lsn = "0x0000002e000001d80030"

    oracle_position = oracle.capture_reader_position()
    sqlserver_position = sqlserver.capture_reader_position()
    assert oracle_position == "scn:123456.c"
    assert sqlserver_position == "0000002e000001d80030.c"
    assert lsn_family(oracle_position) == lsn_family("scn:123456.c")
    assert lsn_family(sqlserver_position) == lsn_family(
        "0000002e000001d80040.c"
    )


def test_composite_position_text_and_ordering() -> None:
    oracle_legacy = encode_logminer_token(100, table="T", phase="snapshot")
    oracle_r1 = encode_logminer_token(
        100, table="T", rs_id=" 0x1.1.1 ", ssn=3
    )
    oracle_r2 = encode_logminer_token(
        100, table="T", rs_id="0x1.1.2", ssn=3
    )
    oracle_complete = encode_logminer_token(100, table="T", scn_complete=True)
    oracle_incomplete = encode_logminer_token(100, table="T")
    assert batch_lsn(oracle_legacy) == "scn:99"
    assert batch_lsn(oracle_r1) == "scn:100.p0x1.1.1.0000000003"
    assert batch_lsn(oracle_complete) == "scn:100.c"
    assert batch_lsn(oracle_incomplete) == "scn:100"
    assert compare_lsn(batch_lsn(oracle_legacy), batch_lsn(oracle_r1)) < 0
    assert compare_lsn(batch_lsn(oracle_r1), batch_lsn(oracle_complete)) < 0
    assert compare_lsn(batch_lsn(oracle_incomplete), batch_lsn(oracle_r1)) < 0
    assert compare_lsn(batch_lsn(oracle_r1), batch_lsn(oracle_r2)) < 0
    assert compare_lsn("scn:100.c", "scn:101") < 0
    assert len(lsn_sort_key(batch_lsn(oracle_r1))) == 4
    assert isinstance(lsn_sort_key(batch_lsn(oracle_r1))[3], str)
    assert compare_lsn(
        batch_lsn(encode_logminer_token(100, table="T", rs_id="0x17.b4a2.10", ssn=1)),
        batch_lsn(
            encode_logminer_token(
                100, table="T", rs_id="0x000017.0000b4a2.0010", ssn=1
            )
        ),
    ) == 0
    assert compare_lsn(
        batch_lsn(encode_logminer_token(100, table="T", rs_id="0x9.ffff.ffff", ssn=9)),
        batch_lsn(encode_logminer_token(100, table="T", rs_id="0x10.0.0", ssn=0)),
    ) < 0
    assert compare_lsn(
        batch_lsn(encode_logminer_token(100, table="T", rs_id="0x17.ffff.ffff", ssn=9)),
        batch_lsn(encode_logminer_token(100, table="T", rs_id="0x18.0.0", ssn=0)),
    ) < 0
    sqlserver_lsn = "0000002E000001D80030"
    sqlserver_seq1 = "0000002E000001D80002"
    sqlserver_seq2 = "0000002E000001D80005"
    sqlserver_partial = encode_mssql_cdc_token(
        sqlserver_lsn, table="T", seqval=sqlserver_seq1
    )
    sqlserver_later = encode_mssql_cdc_token(
        sqlserver_lsn, table="T", seqval=sqlserver_seq2
    )
    sqlserver_complete = encode_mssql_cdc_token(sqlserver_lsn, table="T")
    assert (
        batch_lsn(sqlserver_partial)
        == "0000002e000001d80030.p0000002e000001d80002"
    )
    assert batch_lsn(sqlserver_complete) == "0000002e000001d80030.c"
    assert compare_lsn(sqlserver_lsn.lower(), batch_lsn(sqlserver_partial)) < 0
    assert compare_lsn(batch_lsn(sqlserver_partial), batch_lsn(sqlserver_complete)) < 0
    assert compare_lsn(batch_lsn(sqlserver_partial), batch_lsn(sqlserver_later)) < 0
    assert compare_lsn(
        batch_lsn(encode_mssql_cdc_token(sqlserver_lsn, table="T", seqval="5")),
        batch_lsn(encode_mssql_cdc_token(sqlserver_lsn, table="T", seqval="10")),
    ) < 0
    assert compare_lsn(
        batch_lsn(encode_mssql_cdc_token(sqlserver_lsn, table="T", seqval="5")),
        batch_lsn(encode_mssql_cdc_token(sqlserver_lsn, table="T", seqval="0005")),
    ) == 0
    assert compare_lsn(
        "0000002e000001d80030.c",
        "0000002e000001d80031.p0000002e000001d80002",
    ) < 0
    assert len(lsn_sort_key(batch_lsn(sqlserver_partial))) == 4
    assert isinstance(lsn_sort_key(batch_lsn(sqlserver_partial))[3], str)


def test_oracle_snapshot_position_precedes_streaming_handoff_and_events() -> None:
    handoff_scn = 500
    snapshot = encode_logminer_token(
        handoff_scn, table="T", phase="snapshot"
    )
    streaming_handoff = encode_logminer_token(
        handoff_scn - 1,
        table="T",
        phase="streaming",
        scn_complete=True,
    )
    first_event = encode_logminer_token(
        handoff_scn,
        table="T",
        phase="streaming",
        rs_id="0x1.1.1",
        ssn=1,
    )

    assert compare_lsn(batch_lsn(snapshot), batch_lsn(streaming_handoff)) < 0
    assert compare_lsn(batch_lsn(snapshot), batch_lsn(first_event)) < 0


def test_oracle_snapshot_handoff_eos_resume_keeps_streaming_phase() -> None:
    handoff_scn = 500
    stream_key = "oracle|db|T"
    snapshot = encode_logminer_token(
        handoff_scn, table="T", phase="snapshot"
    )
    streaming_handoff = encode_logminer_token(
        handoff_scn - 1,
        table="T",
        phase="streaming",
        scn_complete=True,
    )
    first_event = encode_logminer_token(
        handoff_scn,
        table="T",
        phase="streaming",
        rs_id="0x1.1.1",
        ssn=1,
    )
    store = InMemoryEosStore()
    snapshot_result = store.commit_atomic(
        stream_key=stream_key,
        incoming_lsn=batch_lsn(snapshot),
        batch_id="snapshot",
        apply_fn=lambda: store.upsert_row("1", {"id": "1", "v": "snapshot"}),
    )
    assert not snapshot_result.already_committed
    destination_resume = encode_resume_blob(snapshot)

    handoff_result = store.commit_atomic(
        stream_key=stream_key,
        incoming_lsn=batch_lsn(streaming_handoff),
        batch_id="streaming-handoff",
        apply_fn=lambda: None,
    )
    if not handoff_result.already_committed:
        destination_resume = encode_resume_blob(streaming_handoff)
    assert not handoff_result.already_committed
    destination = DestWmView(
        committed_lsn=store.watermarks[stream_key].committed_lsn,
        resume_blob=destination_resume,
    )
    opened = plan_open_session(
        dest=destination,
        incoming_fence=0,
        job_resume=snapshot,
    )
    assert decode_logminer_token(opened.resume)["phase"] == "streaming"
    assert not snapshot_dump_open(opened.resume)

    event_result = store.commit_atomic(
        stream_key=stream_key,
        incoming_lsn=batch_lsn(first_event),
        batch_id="insert",
        apply_fn=lambda: store.upsert_row("2", {"id": "2", "v": "insert"}),
    )
    assert not event_result.already_committed
    destination = DestWmView(
        committed_lsn=store.watermarks[stream_key].committed_lsn,
        resume_blob=encode_resume_blob(first_event),
    )
    resumed = plan_open_session(
        dest=destination,
        incoming_fence=0,
        job_resume=snapshot,
    )
    assert decode_logminer_token(resumed.resume)["phase"] == "streaming"
    assert not snapshot_dump_open(resumed.resume)
    assert set(store.rows) == {"1", "2"}


def test_legacy_oracle_snapshot_watermark_skips_snapshot_but_applies_streaming_event() -> None:
    handoff_scn = 500
    stream_key = "oracle|db|T"
    store = InMemoryEosStore()
    legacy = store.commit_atomic(
        stream_key=stream_key,
        incoming_lsn=f"scn:{handoff_scn}",
        batch_id="legacy-snapshot",
        apply_fn=lambda: None,
    )
    assert not legacy.already_committed

    snapshot = encode_logminer_token(
        handoff_scn, table="T", phase="snapshot"
    )
    redelivery = store.commit_atomic(
        stream_key=stream_key,
        incoming_lsn=batch_lsn(snapshot),
        batch_id="snapshot-redelivery",
        apply_fn=lambda: store.upsert_row("1", {"id": "1", "v": "snapshot"}),
    )
    assert redelivery.already_committed

    event = encode_logminer_token(
        handoff_scn,
        table="T",
        phase="streaming",
        rs_id="0x1.1.1",
        ssn=1,
    )
    streamed = store.commit_atomic(
        stream_key=stream_key,
        incoming_lsn=batch_lsn(event),
        batch_id="streaming-insert",
        apply_fn=lambda: store.upsert_row("2", {"id": "2", "v": "insert"}),
    )
    assert not streamed.already_committed
    assert set(store.rows) == {"2"}


def test_sqlserver_snapshot_position_precedes_streaming_handoff() -> None:
    handoff_lsn = "0000002e000001d80030"
    snapshot = encode_mssql_cdc_token(
        handoff_lsn, table="T", phase="snapshot"
    )
    streaming_handoff = encode_mssql_cdc_token(
        handoff_lsn, table="T", phase="streaming"
    )

    assert compare_lsn(batch_lsn(snapshot), batch_lsn(streaming_handoff)) < 0


@pytest.mark.parametrize("txn_buffer", [False, True], ids=["buffer-off", "buffer-on"])
def test_legacy_oracle_empty_cursor_keeps_published_complete_eos_position(
    txn_buffer: bool,
) -> None:
    legacy = {
        "kind": "oracle-logminer",
        "table": "T",
        "scn": 100,
        "phase": "streaming",
        "txn_buffer": txn_buffer,
    }
    if txn_buffer:
        legacy.update(
            {
                "low_scn": 90,
                "poll_scn": 100,
                "commit_scn": 100,
                "commit_xid": "1.2.3",
            }
        )

    destination_lsn = "scn:100.c"
    assert extract_cdc_commit_position(legacy) == destination_lsn
    assert lsn_sort_key(extract_cdc_commit_position(legacy)) == lsn_sort_key(
        destination_lsn
    )
    resumed, proof = clamp_job_resume_to_dest(
        legacy,
        destination_lsn,
        encode_resume_blob(legacy),
    )
    assert proof["reason"] == "dest_resume_blob_authoritative"
    assert not proof["clamped"]
    assert resumed == legacy
    if txn_buffer:
        assert decode_logminer_token(resumed)["low_scn"] == 90

    at_x = encode_logminer_token(100, table="T", rs_id="0x1.1.1", ssn=1)
    at_x_later = encode_logminer_token(100, table="T", rs_id="0x1.1.2", ssn=2)
    at_x_plus_one = encode_logminer_token(
        101, table="T", rs_id="0x1.1.1", ssn=1
    )
    for token in (at_x, at_x_later):
        position = extract_cdc_commit_position(token)
        assert position is not None
        assert decide_eos_apply(
            incoming_lsn=position,
            dest_lsn=destination_lsn,
        )[0] == "already_committed"
    next_position = extract_cdc_commit_position(at_x_plus_one)
    assert next_position is not None
    assert decide_eos_apply(
        incoming_lsn=next_position,
        dest_lsn=destination_lsn,
    )[0] == "apply"


def test_explicit_incomplete_oracle_token_keeps_same_scn_inclusive() -> None:
    token = encode_logminer_token(100, table="T", scn_complete=False)
    state = decode_logminer_token(token)
    assert state["scn_complete"] is False
    assert extract_cdc_commit_position(token) == "scn:100"
    assert lsn_sort_key(extract_cdc_commit_position(token)) == lsn_sort_key(
        "scn:100"
    )


def test_oracle_encoder_always_emits_scn_complete_boolean() -> None:
    for token in (
        encode_logminer_token(100, table="T"),
        encode_logminer_token(100, table="T", scn_complete=True),
        encode_logminer_token(100, table="T", phase="snapshot"),
    ):
        assert isinstance(json.loads(token)["scn_complete"], bool)


def test_live_oracle_rsid_samples_match_logminer_order() -> None:
    mined_rows = (
        [(2329301, " 0x000016.0000017b.00d8 ", ssn) for ssn in range(16)]
        + [(2329456, " 0x000017.000000b5.0168 ", ssn) for ssn in range(8)]
        + [(2329465, " 0x000018.00000007.0010 ", ssn) for ssn in range(8, 16)]
    )
    expected = sorted(mined_rows)
    composite_positions = [
        batch_lsn(
            encode_logminer_token(
                scn, table="T", rs_id=rs_id, ssn=ssn
            )
        )
        for scn, rs_id, ssn in mined_rows
    ]
    observed = [
        row
        for _, row in sorted(
            zip(
                mined_rows,
                composite_positions,
                strict=True,
            ),
            key=lambda pair: lsn_sort_key(pair[1]),
        )
    ]
    assert mined_rows == expected
    assert observed == [
        batch_lsn(encode_logminer_token(scn, table="T", rs_id=rs_id, ssn=ssn))
        for scn, rs_id, ssn in expected
    ]


@pytest.mark.parametrize(
    ("malformed", "valid"),
    [
        ("scn:100.pXYZ", "scn:100.p0x1.1.1.0000000001"),
        ("scn:100.p1.2.0000000001", "scn:100.p0x1.1.1.0000000001"),
        ("scn:100.pg.2.3.0000000001", "scn:100.p0x1.1.1.0000000001"),
        ("0000002e000001d80030.q12", "0000002e000001d80030.c"),
    ],
)
def test_malformed_composite_positions_fail_closed(
    malformed: str, valid: str
) -> None:
    assert lsn_family(malformed) == "opaque"
    with pytest.raises(ExactlyOnceRouteError):
        decide_eos_apply(incoming_lsn=malformed, dest_lsn=valid)


def test_legacy_oracle_watermark_is_refined_from_resume_blob() -> None:
    first = encode_logminer_token(
        100, table="T", rs_id="0x1.1.1", ssn=1
    )
    later = encode_logminer_token(
        100, table="T", rs_id="0x1.1.2", ssn=1
    )
    no_rs_id = encode_logminer_token(100, table="T", scn_complete=True)
    dest = DestWmView(
        committed_lsn="scn:100",
        resume_blob=encode_resume_blob(first),
    )
    assert decide_from_view(incoming_lsn=batch_lsn(later), dest=dest)[0] == "apply"
    assert (
        decide_from_view(incoming_lsn=batch_lsn(first), dest=dest)[0]
        == "already_committed"
    )
    assert decide_from_view(incoming_lsn=batch_lsn(no_rs_id), dest=dest)[0] == "apply"


def test_legacy_sqlserver_watermark_is_refined_from_resume_blob() -> None:
    lsn = "0000002e000001d80030"
    first = encode_mssql_cdc_token(lsn, table="T", seqval="0000002e000001d80002")
    later = encode_mssql_cdc_token(lsn, table="T", seqval="0000002e000001d80005")
    no_seqval = encode_mssql_cdc_token(lsn, table="T")
    dest = DestWmView(
        committed_lsn=lsn,
        resume_blob=encode_resume_blob(first),
    )
    assert decide_from_view(incoming_lsn=batch_lsn(later), dest=dest)[0] == "apply"
    assert (
        decide_from_view(incoming_lsn=batch_lsn(first), dest=dest)[0]
        == "already_committed"
    )
    assert (
        decide_from_view(incoming_lsn=batch_lsn(no_seqval), dest=dest)[0]
        == "apply"
    )


def test_legacy_plain_watermarks_apply_same_major_composite_positions() -> None:
    oracle_token = encode_logminer_token(
        100, table="T", rs_id="0x1.1.1", ssn=1
    )
    sqlserver_token = encode_mssql_cdc_token(
        "0000002e000001d80030",
        table="T",
        seqval="0000002e000001d80002",
    )
    assert (
        decide_from_view(
            incoming_lsn=batch_lsn(oracle_token),
            dest=DestWmView(committed_lsn="scn:100"),
        )[0]
        == "apply"
    )
    assert (
        decide_from_view(
            incoming_lsn=batch_lsn(sqlserver_token),
            dest=DestWmView(committed_lsn="0000002e000001d80030"),
        )[0]
        == "apply"
    )


def test_digit_only_legacy_sqlserver_watermark_fails_closed() -> None:
    incoming = encode_mssql_cdc_token(
        "0000002a000000100003",
        table="T",
        seqval="0000002a000000100005",
    )
    with pytest.raises(ExactlyOnceRouteError):
        decide_from_view(
            incoming_lsn=batch_lsn(incoming),
            dest=DestWmView(committed_lsn="00000025000004500003"),
        )


def test_clamp_prefers_matching_composite_destination_resume_blob() -> None:
    token = encode_logminer_token(
        100, table="T", rs_id="0x1.1.1", ssn=1
    )
    dest_lsn = batch_lsn(token)
    resumed, proof = clamp_job_resume_to_dest(
        token,
        dest_lsn,
        encode_resume_blob(token),
    )
    assert proof["reason"] == "dest_resume_blob_authoritative"
    assert proof["job_lsn"] == dest_lsn
    assert isinstance(resumed, dict)
    assert resumed != dest_lsn
    assert batch_lsn(resumed) == dest_lsn


def test_in_memory_eos_applies_split_same_scn_and_skips_redelivery() -> None:
    store = InMemoryEosStore()
    stream_key = "oracle|db|T"
    tokens = [
        encode_logminer_token(100, table="T", rs_id="0x1.1.1", ssn=1),
        encode_logminer_token(100, table="T", rs_id="0x1.1.2", ssn=1),
    ]
    applied: list[str] = []

    def commit(token: str, value: str, batch_id: str):
        position = batch_lsn(token)

        def apply() -> None:
            applied.append(value)
            store.upsert_row("1", {"id": "1", "value": value, "_df_lsn": position})

        return store.commit_atomic(
            stream_key=stream_key,
            incoming_lsn=position,
            batch_id=batch_id,
            apply_fn=apply,
        )

    first = commit(tokens[0], "first", "b1")
    second = commit(tokens[1], "second", "b2")
    redelivery = commit(tokens[1], "second", "b2")
    assert first.status == "applied"
    assert second.status == "applied"
    assert redelivery.status == "already_committed"
    assert applied == ["first", "second"]
    assert store.rows["1"]["value"] == "second"


def test_legacy_watermark_warning_is_logged_once_for_composite_position(caplog) -> None:
    dest = DestWmView(committed_lsn="scn:889")
    incoming = "scn:889.p0x1.1.1.0000000001"

    with caplog.at_level("WARNING", logger="services.cdc_exactly_once"):
        assert decide_from_view(incoming_lsn=incoming, dest=dest)[0] == "apply"
        assert decide_from_view(incoming_lsn=incoming, dest=dest)[0] == "apply"

    warnings = [
        record
        for record in caplog.records
        if "legacy dest watermark compared with composite incoming position"
        in record.message
    ]
    assert len(warnings) == 1
    assert "dest=scn:889" in warnings[0].message
    assert f"incoming={incoming}" in warnings[0].message
