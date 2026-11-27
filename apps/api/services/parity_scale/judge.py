"""The cell judge: Gate-8's own rules on the few rows the engines found differing.

The engine digest is at least as strict as fingerprint v2, so a row it calls
different may still be the same value under the destination's own equality (a
case-insensitive collation, a JSON key order, a SQLite separator spelling).
Each differing row is therefore fetched and every cell pair is compared with
:func:`services.reconciliation.fingerprint_for_reconcile` under the destination
column's type — the same call Gate-8 makes — and, where the cells really
differ, given a cause by :mod:`services.parity_classifier`. A row whose cells
all fingerprint equal is *not* a difference; it is counted in
``ce_stricter_than_fp`` (a high count points to a projection gap).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Sequence

from services.parity_scale import ce1
from services.parity_scale.plan import ColumnPlan, TablePlan


def wire_text(value: Any) -> str | None:
    """The wire text the classifier reads (``None`` for SQL NULL)."""
    if value is None:
        return None
    from services.value_serializer import cell_to_string

    return cell_to_string(value, preserve_sql_null=True)


def source_as_written(value: Any, col: ColumnPlan, dest_engine: str) -> Any:
    """The source cell after the write contract's projection (decimal fit)."""
    if col.decimal_fit:
        projected = ce1.project_source_value(value, col.field_spec(dest_engine))
        return None if projected is ce1.HELD else projected
    return value


def fingerprint(value: Any, col: ColumnPlan, dest_engine: str, *, source: bool) -> str:
    from services.reconciliation import fingerprint_for_reconcile

    if source:
        value = source_as_written(value, col, dest_engine)
    return fingerprint_for_reconcile(
        value,
        ddl_type=col.dest_type,
        engine=dest_engine,
        transform=(col.transform or None) if source else None,
    )


@dataclass
class RowVerdict:
    key: str
    equal_by_fingerprint: bool
    cells: list[dict[str, Any]] = field(default_factory=list)

    @property
    def explained(self) -> bool:
        return all(bool((c.get("cause") or {}).get("explained")) for c in self.cells)


def judge_row(
    plan: TablePlan,
    source_row: Sequence[Any],
    dest_row: Sequence[Any],
    *,
    key_text: str,
    quarantined_keys: Iterable[str] = (),
) -> RowVerdict:
    """Compare one row pair cell by cell (values in ``plan.compared_columns`` order)."""
    from services.parity_classifier import CellContext, classify_value

    cols = plan.compared_columns
    verdict = RowVerdict(key=key_text, equal_by_fingerprint=True)
    for col, src, dst in zip(cols, source_row, dest_row, strict=True):
        if fingerprint(src, col, plan.dest_engine, source=True) == fingerprint(
            dst, col, plan.dest_engine, source=False
        ):
            continue
        verdict.equal_by_fingerprint = False
        ctx = CellContext(
            source_type=col.source_type,
            dest_type=col.dest_type,
            dest_engine=plan.dest_engine,
            transforms=(col.transform,) if col.transform else (),
        )
        written = source_as_written(src, col, plan.dest_engine)
        cause = classify_value(
            expected=wire_text(written),
            dest=wire_text(dst),
            ctx=ctx,
            source_raw=wire_text(src),
            raw_known=True,
            key=key_text,
            quarantined_keys=quarantined_keys,
        ) or {"code": "unexplained", "label": "Unexplained", "explained": False,
              "detail": "The values differ and no rule explains how."}
        verdict.cells.append({"column": col.target, "cause": cause})
    return verdict


def judge_missing_or_extra(kind: str, key_text: str | None, quarantined_keys: Iterable[str] = ()) -> dict[str, Any]:
    from services.parity_classifier import classify_row

    return classify_row(kind, key=key_text, quarantined_keys=quarantined_keys)


def key_text(plan: TablePlan, key: Sequence[Any]) -> str:
    """Operator-facing key text (``|``-joined wire cells)."""
    return "|".join("" if v is None else (wire_text(v) or "") for v in key)
