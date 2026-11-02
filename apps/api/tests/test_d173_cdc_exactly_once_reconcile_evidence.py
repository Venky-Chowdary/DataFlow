"""D173 — a SQL Server change-tracking CDC run applied every change and was
still reported "failed".

SQL Server CT batches carry a resume token, so ``_apply_change_batch`` takes
the exactly-once branch, which returned before the reconcile-sample stash.
Gate-8 then refused a streaming write that had no sample ("no reconcile_sample
was stashed"), and the failure text named an option ("sample stash enabled")
that exists nowhere in the product, plus a census line saying SQL Server "has
no census driver" because ``resolve_driver_type`` calls it ``sqlserver`` while
the census keys it ``mssql``.
"""

from __future__ import annotations

from typing import Any

import pytest

from services.cdc_engine import ChangeBatch
from src.transfer import cdc_transfer
from src.transfer.cdc_transfer import _apply_change_batch


def _batch() -> ChangeBatch:
    return ChangeBatch(
        inserts=[{"id": "4", "v": "d"}],
        updates=[{"id": "1", "v": "changed"}],
        deletes=["2", "3"],
        resume_token=7,
    )


def _call(monkeypatch, delivery: str = "exactly_once") -> dict[str, Any]:
    seen: dict[str, Any] = {}

    def fake_eos(**kwargs):
        seen["called"] = True
        return 2, "digest", {"cdc_delivery": "exactly-once"}, 2

    monkeypatch.setattr(
        "connectors.cdc_eos_sql.apply_change_batch_exactly_once", fake_eos
    )
    _rows, _checksum, summary, _deleted = _apply_change_batch(
        "sqlserver",
        type("E", (), {"format": "sqlserver", "table": "t", "extra": {}})(),
        {"database": "x"},
        "t",
        _batch(),
        [{"source": "id", "target": "id"}, {"source": "v", "target": "v"}],
        {"id": "integer", "v": "string"},
        ["id", "v"],
        "id",
        0,
        1,
        delivery_guarantee=delivery,
        cursor_key="k",
        stream_name="t",
        pk_source_cols=["id"],
    )
    assert seen.get("called"), "the exactly-once branch was not taken"
    return summary


def test_exactly_once_branch_stashes_reconcile_sample_and_deletes(monkeypatch):
    summary = _call(monkeypatch)
    assert summary["reconcile_sample"] == [{"id": "4", "v": "d"}, {"id": "1", "v": "changed"}]
    assert summary["reconcile_deletes"] == ["2", "3"]
    assert summary["reconcile_delete_count"] == 2


def test_stash_helper_is_bounded_and_skips_empty_batches():
    big = ChangeBatch(inserts=[{"id": str(i)} for i in range(80)], deletes=[str(i) for i in range(80)])
    out: dict[str, Any] = {}
    cdc_transfer._stash_reconcile_evidence(out, big)
    assert len(out["reconcile_sample"]) == 50
    assert len(out["reconcile_deletes"]) == 50
    assert out["reconcile_delete_count"] == 80
    empty: dict[str, Any] = {}
    cdc_transfer._stash_reconcile_evidence(empty, ChangeBatch())
    assert empty == {}


@pytest.mark.parametrize("fmt", ["mssql", "sqlserver", "azure_sql"])
def test_census_driver_accepts_the_sqlserver_alias(monkeypatch, fmt):
    from src.transfer import reconcile_step

    opened: list[dict] = []

    def fake_connect(cfg):
        opened.append(cfg)
        return object()

    monkeypatch.setattr("services.copy_sqlserver_sqlserver._ss_connect", fake_connect)
    got = reconcile_step._culprit_census_conn(fmt, {"host": "h"})
    assert got is not None, fmt
    assert got[0] == "mssql"
    assert opened == [{"host": "h"}]


def test_gate8_refusal_does_not_name_a_setting_the_operator_cannot_turn_on():
    import inspect

    from src.transfer import reconcile_step

    source = inspect.getsource(reconcile_step)
    assert "sample stash enabled" not in source
