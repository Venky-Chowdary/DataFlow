"""CDC apply stamps _df_lsn from resume tokens for monotonic MERGE."""

from __future__ import annotations

from connectors.writer_common import DF_LSN_COL
from services.cdc_engine import ChangeBatch
from src.transfer.cdc_transfer import _stamp_cdc_lsn


def test_stamp_cdc_lsn_adds_meta_column_and_mapping():
    change = ChangeBatch(
        inserts=[{"id": "1", "amount": "10"}],
        updates=[{"id": "2", "amount": "20"}],
        resume_token="slot=df_x|phase=streaming|lsn=0/ABC",
    )
    headers, mappings, types = _stamp_cdc_lsn(
        change,
        ["id", "amount"],
        [{"source": "id", "target": "id"}, {"source": "amount", "target": "amount"}],
        {"id": "string", "amount": "string"},
    )
    assert DF_LSN_COL in headers
    assert change.inserts[0][DF_LSN_COL] == "0/ABC"
    assert change.updates[0][DF_LSN_COL] == "0/ABC"
    assert any(m.get("source") == DF_LSN_COL for m in mappings)
    assert types[DF_LSN_COL] == "string"


def test_stamp_cdc_lsn_noop_without_token():
    change = ChangeBatch(inserts=[{"id": "1"}])
    headers, mappings, types = _stamp_cdc_lsn(
        change, ["id"], [{"source": "id", "target": "id"}], {"id": "string"}
    )
    assert headers == ["id"]
    assert DF_LSN_COL not in change.inserts[0]
    assert types == {"id": "string"}
