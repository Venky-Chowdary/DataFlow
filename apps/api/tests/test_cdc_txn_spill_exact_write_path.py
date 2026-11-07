"""CDC transaction-buffer spill reread uses json_loads_exact.

stdlib json.loads collapsed 1.234567890123456789 in spilled after-images
before COMMIT applied them. IEEE-exact 1.5 stays float. Invalid lines
stay skipped — never invent a DML event.
"""

from __future__ import annotations

import json
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.cdc_transaction_buffer import (  # noqa: E402
    TransactionBuffer,
    load_cdc_txn_spill_event,
)

LONG = "1.234567890123456789"


def _spill_line(*, pk: str = "1") -> str:
    return (
        '{"op":"i","pk":"'
        + pk
        + '","row":{"amt": '
        + LONG
        + ', "n": 1.5, "id": '
        + pk
        + '},"lsn":"0/1"}'
    )


def test_load_cdc_txn_spill_keeps_long_fraction():
    line = _spill_line()
    rec = load_cdc_txn_spill_event(line)
    assert rec is not None
    assert rec["row"]["amt"] == Decimal(LONG)
    assert rec["row"]["amt"] != json.loads(line)["row"]["amt"]
    assert rec["row"]["n"] == 1.5
    assert rec["row"]["id"] == 1


def test_load_cdc_txn_spill_invalid_is_none():
    assert load_cdc_txn_spill_event("{not-json}") is None
    assert load_cdc_txn_spill_event("42") is None
    assert load_cdc_txn_spill_event("") is None
    assert load_cdc_txn_spill_event("[1, 2]") is None


def test_commit_materialize_keeps_long_fraction(tmp_path):
    path = tmp_path / "df_cdc_txn_exact.jsonl"
    path.write_text("{not-json}\n" + _spill_line() + "\n42\n", encoding="utf-8")
    buf = TransactionBuffer(max_events=100, spill_after=50, spill_dir=tmp_path)
    buf.begin("xid-exact")
    assert buf._open is not None
    buf._open.spill_path = str(path)
    buf._open.spilled_count = 1
    batch = buf.commit(resume_token={"lsn": "0/1"})
    assert batch is not None
    assert len(batch.inserts) == 1
    assert batch.inserts[0]["amt"] == Decimal(LONG)
    assert batch.inserts[0]["n"] == 1.5
    assert batch.inserts[0]["id"] == 1
