"""Exactly-once ordering regressions for within-LSN CDC positions."""

from __future__ import annotations

import pytest

from connectors.lsn_guards import lsn_family
from connectors.oracle_logminer import encode_logminer_token
from connectors.sqlserver_cdc_native import encode_mssql_cdc_token
from services.cdc_exactly_once import (
    ExactlyOnceRouteError,
    batch_lsn,
    decide_eos_apply,
)


@pytest.mark.xfail(
    strict=True,
    reason="G-CDC M1 red: Oracle EOS watermark ignores RS_ID position within an SCN",
)
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


@pytest.mark.xfail(
    strict=True,
    reason="G-CDC M1 red: SQL Server EOS watermark ignores seqval within an LSN",
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


@pytest.mark.xfail(
    strict=True,
    reason="G-CDC M1 red: digit-only SQL Server LSN is misclassified against hex LSN",
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
