"""Quarantined rows are removed from the ladder's source side by their key value.

A stream finding carries ``row`` as the 1-based source row number and the row's
values under ``source_values``. The ladder read a scalar ``row`` as the primary
key value, so it removed the rows whose *id* equalled those row numbers (6 and
8) instead of the quarantined ids 5 and 7. L2/L5 then reported the good rows 6
and 8 as missing from the source and vetoed a correct quarantining load (found
while reproducing D263's resumed reconcile).
"""

from __future__ import annotations

from src.transfer import reconcile_step as rs
from src.transfer.models import EndpointConfig
from services import verification_ladder as vl

_SOURCE_IDS = [1, 2, 3, 3, 4, 5, 6, 7, 8, 9]
_BAD = {5: "oops", 7: ""}


def _ep(table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database", format="postgresql", host="localhost", port=5432,
        database="dataflow", username="dataflow", password="dataflow",
        schema="public", table=table,
    )


def _finding(row_number: int, pk: int) -> dict:
    values = {"id": str(pk), "name": f"n{pk}", "qty": _BAD[pk]}
    return {
        "row": row_number,
        "column": "qty",
        "value": _BAD[pk],
        "reason": "Invalid integer",
        "values": dict(values),
        "source_values": dict(values),
    }


def test_ladder_excludes_quarantined_rows_by_key_not_row_number(monkeypatch) -> None:
    source = [{"id": str(i), "name": f"n{i}", "qty": _BAD.get(i, str(i * 3))} for i in _SOURCE_IDS]
    target = [{"id": str(i), "name": f"n{i}", "qty": str(i * 3)} for i in _SOURCE_IDS if i not in _BAD]
    reads = {"src_t": source, "dst_t": target}
    monkeypatch.setattr(vl, "read_postgres_rows", lambda *, table, **kw: list(reads[table]))
    captured: dict = {}
    monkeypatch.setattr(
        vl,
        "run_five_layer_verification",
        lambda **kw: captured.update(kw) or {"layers": {}, "passed": True},
    )
    report = {
        "source_rows": 10,
        "target_rows": 8,
        "records_transferred": 8,
        "rejected_rows": 2,
    }
    dest_summary = {
        "sync_mode": "full_refresh_append",
        "primary_key_columns": ["id"],
        "target_rows_before": 0,
        "rejected_rows": 2,
        # Row numbers 6 and 8 are the 1-based positions of ids 5 and 7.
        "rejected_details": [_finding(6, 5), _finding(8, 7)],
    }
    rs._maybe_attach_verification_ladder(
        report,
        endpoint=_ep("dst_t"),
        source_endpoint=_ep("src_t"),
        records=[],
        columns=["id", "name", "qty"],
        dest_summary=dest_summary,
        mappings=[
            {"target": "id", "target_type": "INT4"},
            {"target": "name", "target_type": "TEXT"},
            {"target": "qty", "target_type": "INT4"},
        ],
        validation_mode="strict",
    )

    assert captured, "ladder did not run"
    compared = sorted(int(r["id"]) for r in captured["source_rows"])
    assert compared == [1, 2, 3, 3, 4, 6, 8, 9]
