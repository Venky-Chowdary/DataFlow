"""Re-review 4, finding 6 (Low-Medium): an explained difference Gate-8 could not see does not
re-check a cell Gate-8 named.

Gate-8 named column ``name`` (Map transform ``upper``) different on key 7; the re-read now holds
expected == destination, so only the transform separates raw from destination and the cell
turned into explained ``transform_applied``. A CHAR cell that now differs only by padding became
explained ``whitespace``. Gate-8 compares the written image and strips CHAR padding, so neither
is the difference it saw: such a cell "cannot be re-checked" (contract 29/45).
"""

from __future__ import annotations

import pytest

from services import parity_cache
from services.parity_report import build_parity_report

MAPS = [{"source": "id", "target": "id"},
        {"source": "name", "target": "name", "transform": "upper", "target_type": "VARCHAR(20)"},
        {"source": "code", "target": "code", "target_type": "CHAR(10)"},
        {"source": "amount", "target": "amount", "target_type": "DOUBLE PRECISION", "source_type": "DOUBLE PRECISION"}]


def _job(mismatches: list[dict]) -> dict:
    return {"_id": "j", "status": "completed",
            "reconciliation": {"passed": False, "source_rows": 10, "target_rows": 10, "culprit_census_clean": True,
                               "sample_compare": {"rows_compared": 10, "mismatches": mismatches}},
            "destination_summary": {"table": "t", "primary_key_columns": ["id"], "sync_mode": "full_refresh_overwrite"},
            "transfer_request": {"mappings": MAPS, "source": {"kind": "database"}, "destination": {"kind": "database"}}}


@pytest.fixture(autouse=True)
def _clean_cache():
    parity_cache.clear()
    yield
    parity_cache.clear()


def _rows(rows: dict) -> callable:
    def reread(job, ev, keys):
        return {k: rows[k] for k in keys if k in rows}
    return reread


def test_transform_and_char_padding_do_not_recheck_a_named_cell(monkeypatch):
    from services import parity_report

    monkeypatch.setattr(parity_report, "reread_cells", _rows({
        "7": {"raw": {"id": "7", "name": "alice", "code": "x", "amount": "1"},
              "expected": {"id": "7", "name": "ALICE", "code": "x", "amount": "1"},
              "dest": {"id": "7", "name": "ALICE", "code": "x", "amount": "1"}, "held_out": False},
        "8": {"raw": {"id": "8", "name": "bob", "code": "abc", "amount": "1"},
              "expected": {"id": "8", "name": "BOB", "code": "abc", "amount": "1"},
              "dest": {"id": "8", "name": "BOB", "code": "abc       ", "amount": "1"}, "held_out": False},
    }))
    report = build_parity_report(_job([
        {"key": "7", "source": "name", "target": "name", "source_value": "ALICE", "target_value": "ALICX"},
        {"key": "8", "source": "code", "target": "code", "source_value": "abc", "target_value": "abd"},
    ]), reread=True)
    open_cells = {(k, d["column"]) for d in report["differences"] if not d["cause"]["explained"]
                  for k in d["keys_sample"]}
    assert ("7", "name") in open_cells and ("8", "code") in open_cells, report["differences"]
    assert any("cannot be re-checked" in d["cause"]["detail"] for d in report["differences"])


def test_an_explained_difference_gate8_itself_sees_still_rechecks(monkeypatch):
    from services import parity_report

    monkeypatch.setattr(parity_report, "reread_cells", _rows({
        "9": {"raw": {"id": "9", "name": "c", "code": "c", "amount": "0.1"},
              "expected": {"id": "9", "name": "C", "code": "c", "amount": "0.1"},
              "dest": {"id": "9", "name": "C", "code": "c", "amount": "0.1000000000000000055511151231257827"},
              "held_out": False},
    }))
    report = build_parity_report(_job([
        {"key": "9", "source": "amount", "target": "amount", "source_value": "0.1",
         "target_value": "0.1000000000000000055511151231257827"},
    ]), reread=True)
    amount = [d for d in report["differences"] if d["column"] == "amount"]
    assert amount and all(d["cause"]["explained"] for d in amount), amount
    assert not any("cannot be re-checked" in d["cause"]["detail"] for d in report["differences"])
