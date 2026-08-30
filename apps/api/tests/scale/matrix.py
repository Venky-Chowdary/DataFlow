"""Cell records, the real-engine run helper, and the evidence emitters.

One ``Cell`` is one route × mode measurement. Its fields are the ones Track D
must publish, and they are deliberately split between *what the writer said*
(``records_transferred``) and *what the destination holds* (``dest_rows``,
``dest_checksum``): the whole point of the matrix is that those two can
disagree, and a harness that records only the first cannot notice.

Every transfer here goes through ``UniversalTransferEngine.execute_tracked`` —
the same entrypoint the API uses — so preflight gates, reconcile and row
accounting run exactly as they do for an operator. There is no bypass path.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Sequence

ARTIFACT_DIR = Path(
    os.getenv("DATAFLOW_SCALE_ARTIFACTS", "/tmp/dataflow-scale")
).resolve()

PASS = "pass"
FAIL = "fail"
SKIP = "skip"


@dataclass
class Cell:
    """One measured route × mode result."""

    route: str
    mode: str
    direction: str = "->"
    status: str = FAIL
    source_rows: int = 0
    dest_rows: int = 0
    written: int = 0
    rejected: int = 0
    quarantined: int = 0
    coerced_null: int = 0
    skipped_rows: int = 0
    elapsed_seconds: float = 0.0
    rows_per_second: float = 0.0
    schema_shape: str = ""
    run_id: str = ""
    source_checksum: str = ""
    dest_checksum: str = ""
    reconcile: str = ""
    delete_capture: str = "n/a"
    delivery: str = ""
    notes: str = ""
    detail: dict[str, Any] = field(default_factory=dict)

    def mark(self, ok: bool, note: str = "") -> "Cell":
        self.status = PASS if ok else FAIL
        if note:
            self.notes = f"{self.notes}; {note}".strip("; ")
        return self

    def skip(self, reason: str) -> "Cell":
        self.status = SKIP
        self.notes = reason
        return self


@dataclass
class Matrix:
    """Collected cells plus the emitters that turn them into evidence."""

    cells: list[Cell] = field(default_factory=list)

    def add(self, cell: Cell) -> Cell:
        self.cells.append(cell)
        print(
            f"[{cell.status.upper():4}] {cell.route} · {cell.mode} · "
            f"src={cell.source_rows} dst={cell.dest_rows} "
            f"{cell.elapsed_seconds:.1f}s {cell.rows_per_second:.0f} r/s "
            f"{cell.notes}",
            flush=True,
        )
        return cell

    def counts(self) -> dict[str, int]:
        return {
            status: sum(1 for c in self.cells if c.status == status)
            for status in (PASS, FAIL, SKIP)
        }

    def write_json(self, path: Path | None = None) -> Path:
        ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
        target = path or ARTIFACT_DIR / "scale_matrix_modes_schedules.json"
        target.write_text(
            json.dumps(
                {"counts": self.counts(), "cells": [asdict(c) for c in self.cells]},
                indent=2,
                default=str,
            )
        )
        return target

    def markdown(self) -> str:
        head = (
            "| Route | Mode | Src rows | Dest rows (independent) | Rejected | "
            "Quarantined | Coerced null | Skipped | Elapsed s | Rows/s | "
            "Reconcile | Run id | Status | Notes |"
        )
        rule = "|" + "---|" * 14
        lines = [head, rule]
        for c in self.cells:
            lines.append(
                f"| {c.route} | {c.mode} | {c.source_rows} | {c.dest_rows} | "
                f"{c.rejected} | {c.quarantined} | {c.coerced_null} | "
                f"{c.skipped_rows} | {c.elapsed_seconds:.1f} | "
                f"{c.rows_per_second:.0f} | {c.reconcile or '-'} | "
                f"`{c.run_id or '-'}` | {c.status} | {c.notes or '-'} |"
            )
        return "\n".join(lines)


# --------------------------------------------------------------------------
# the real-engine run helper
# --------------------------------------------------------------------------

def contract(
    stream: str,
    mode: str,
    *,
    primary_key: str = "id",
    cursor_field: str = "",
    cursor_semantics: str = "modification_timestamp",
    **extra: Any,
) -> dict[str, Any]:
    """Stream contract carrying the identity a mode needs to be legal.

    ``cursor_semantics`` is mandatory whenever a cursor is declared: the
    product refuses to infer from the column's name whether the source moves
    the value on update, because a name cannot establish that.
    """
    c: dict[str, Any] = {
        "name": stream,
        "sync_mode": mode,
        "primary_key": primary_key,
        "selected": True,
    }
    if cursor_field:
        c["cursor_field"] = cursor_field
        c["cursor_semantics"] = cursor_semantics
    c.update(extra)
    return c


def mappings_for(
    cols: Sequence[str], omit: Sequence[str] = (), ack: Sequence[str] = ()
) -> list[dict[str, Any]]:
    """Write mappings, plus an explicit omission decision per dropped column.

    Gate G13 blocks a run whose source carries a column that is neither written
    nor declared omitted (Mongo's server-minted ``_id`` is the usual one), so
    the omission has to be stated rather than left to the writer.

    ``ack`` names columns where the operator has accepted a risk the product
    otherwise refuses to take silently — a zoneless ``TIMESTAMP`` landing in a
    BSON date, which is an instant and so needs *some* zone invented. The
    acknowledgement is carried per column, in the request, exactly as the UI
    sends it; it is not a harness-only bypass.
    """
    acked = set(ack)
    written = [
        {"source": c, "target": c, "confidence": 1.0}
        | ({"risk_acknowledged": True} if c in acked else {})
        for c in cols
    ]
    return written + [
        {"source": c, "target": "", "intentional_omit": True} for c in omit
    ]


def run_transfer(
    source: Any,
    destination: Any,
    *,
    mode: str,
    contracts: list[dict[str, Any]],
    cols: Sequence[str],
    job_id: str = "",
    validation_mode: str = "balanced",
    omit: Sequence[str] = (),
    ack: Sequence[str] = (),
    **request_kwargs: Any,
) -> tuple[Any, float, str]:
    """Execute one transfer through the product's tracked entrypoint."""
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import TransferRequest

    run_id = job_id or ("scale" + uuid.uuid4().hex[:12])
    request = TransferRequest(
        source=source,
        destination=destination,
        sync_mode=mode,
        skip_preflight=False,
        validation_mode=validation_mode,
        stream_contracts=contracts,
        mappings=mappings_for(cols, omit, ack),
        **request_kwargs,
    )
    started = time.perf_counter()
    result = UniversalTransferEngine().execute_tracked(request, run_id)
    return result, time.perf_counter() - started, run_id


def accounting(result: Any) -> dict[str, int]:
    """Rejected / quarantined / coerced-null / skipped, as the run reported them.

    These come from the run's own ledgers because they describe rows that never
    reached the destination — the destination cannot be asked about a row it
    was never offered. Landed rows are still proven independently.
    """
    ledger = dict(getattr(result, "row_accounting", None) or {})
    dest = dict(getattr(result, "destination_summary", None) or {})
    src = dict(getattr(result, "source_summary", None) or {})

    def pick(*keys: str) -> int:
        for holder in (ledger, dest, src):
            for key in keys:
                value = holder.get(key)
                if isinstance(value, (int, float)):
                    return int(value)
        return 0

    return {
        "rejected": pick("rejected", "rejected_rows", "rows_rejected"),
        "quarantined": pick("quarantined", "quarantined_rows", "rows_quarantined"),
        "coerced_null": pick("coerced_null", "coerced_nulls", "nulled"),
        "skipped_rows": pick("skipped", "skipped_rows", "rows_skipped"),
    }


def reconcile_verdict(result: Any) -> str:
    """Gate-8 reconcile status as a single honest token."""
    rec = dict(getattr(result, "reconciliation", None) or {})
    if not rec:
        return ""
    for key in ("status", "verdict", "result", "outcome"):
        value = rec.get(key)
        if isinstance(value, str) and value:
            return value
    matched = rec.get("matched")
    if isinstance(matched, bool):
        return "match" if matched else "mismatch"
    return "reported"


def fill(cell: Cell, result: Any, elapsed: float, run_id: str) -> Cell:
    cell.written = int(getattr(result, "records_transferred", 0) or 0)
    cell.elapsed_seconds = round(elapsed, 2)
    cell.rows_per_second = round(cell.written / elapsed, 1) if elapsed > 0 else 0.0
    cell.run_id = run_id
    cell.reconcile = reconcile_verdict(result)
    for key, value in accounting(result).items():
        setattr(cell, key, value)
    if not result.success:
        cell.notes = str(result.error or "")[:280]
    return cell


def guard(fn: Callable[[], list[Cell]], route: str, mode: str) -> list[Cell]:
    """Run one cell function; an exception is a ``fail``, never a silent skip."""
    try:
        return fn()
    except Exception as exc:  # noqa: BLE001 — the failure IS the measurement
        return [Cell(route=route, mode=mode, notes=f"{type(exc).__name__}: {exc}"[:280])]
