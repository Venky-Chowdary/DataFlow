"""Shared harness: what does a destination hold after two runs of one mode?

A transfer that returns ``success=True`` proves nothing about a sync mode. The
mode *is* its second-run behaviour, and each has a different one:

===================== =========================================================
``full_refresh_overwrite``  the destination is replaced, so N rows stay N
``full_refresh_append``     rows are added, so N rows become 2N
``incremental_append``      only rows past the watermark are read, so N stays N
``upsert``                  keyed writes are idempotent, so N stays N
===================== =========================================================

Only the row count after the second run separates them. A destination that
silently appends under ``upsert`` doubles the customer's data; one that appends
under ``incremental`` re-reads the whole source every schedule tick.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

RECORDS = [{"id": "1", "amount": "1000.00"}, {"id": "2", "amount": "2000.50"}]
COLUMNS = ["id", "amount"]
SCHEMA = {"id": "INTEGER", "amount": "DECIMAL(12,2)"}
MAPPINGS = [
    {"source": "id", "target": "id", "confidence": 0.99},
    {"source": "amount", "target": "amount", "confidence": 0.99},
]

#: Rows the destination must hold after running the same source twice, for a
#: row-addressed destination (a SQL table, a collection that mints its own id).
EXPECTED_AFTER_TWO_RUNS: dict[str, int] = {
    "full_refresh_overwrite": len(RECORDS),
    "full_refresh_append": len(RECORDS) * 2,
    "incremental_append": len(RECORDS),
    "upsert": len(RECORDS),
}


def expected_rows_scaled(mode: str, *, rows: int, key_addressed: bool = False) -> int:
    """Rows expected after two runs of ``mode`` over a source of ``rows`` rows.

    The same semantics table as :func:`expected_rows`, parameterised so a scale
    matrix moving 100K rows asks *one* owner what the mode means instead of
    re-deriving ``2N`` next to its own assertions.
    """
    if mode not in EXPECTED_AFTER_TWO_RUNS:
        raise KeyError(f"unknown sync mode: {mode}")
    if mode == "full_refresh_append":
        return rows if key_addressed else rows * 2
    return rows


def expected_rows(mode: str, *, key_addressed: bool = False) -> int:
    """Rows expected after two runs, accounting for how the sink is addressed.

    A **key-addressed** destination — a Redis keyspace, a DynamoDB table, an
    index keyed on a document id — cannot duplicate under append, because the
    key *is* the row's identity: writing the same key twice replaces it. Append
    there lands N, not 2N, and that is the store behaving correctly rather than
    the mode failing.

    Stating this per destination keeps the difference visible. Asserting 2N
    everywhere would have reported a correct Redis append as a defect, and
    asserting N everywhere would have hidden a SQL table that silently
    deduplicated rows an operator expected to accumulate.
    """
    return expected_rows_scaled(
        mode, rows=len(RECORDS), key_addressed=key_addressed
    )


@dataclass
class ModeOutcome:
    mode: str
    ok: bool
    first_rows: int
    second_rows: int
    destination_rows: int | None
    error: str = ""
    key_addressed: bool = False

    @property
    def expected(self) -> int:
        return expected_rows(self.mode, key_addressed=self.key_addressed)

    @property
    def correct(self) -> bool:
        return self.ok and self.destination_rows == self.expected


def stream_contract(name: str, mode: str) -> list[dict[str, Any]]:
    """Contract for one stream, carrying the identity each mode needs."""
    contract: dict[str, Any] = {
        "name": name,
        "sync_mode": mode,
        "primary_key": "id",
        "selected": True,
    }
    if mode.startswith("incremental"):
        # Without a cursor the engine has no watermark to seek past, and
        # "incremental" degrades into a full re-read.
        contract["cursor_field"] = "id"
    return contract


def run_mode(
    source: Any,
    destination: Any,
    mode: str,
    *,
    stream_name: str,
    count_rows: Any,
    validation_mode: str = "balanced",
    key_addressed: bool = False,
) -> ModeOutcome:
    """Run one mode twice and report what the destination ended up holding."""
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import TransferRequest

    engine = UniversalTransferEngine()
    results = []
    for _attempt in (1, 2):
        request = TransferRequest(
            source=source,
            destination=destination,
            sync_mode=mode,
            skip_preflight=False,
            validation_mode=validation_mode,
            stream_contracts=[stream_contract(stream_name, mode)],
            mappings=list(MAPPINGS),
        )
        results.append(engine.execute_tracked(request, uuid.uuid4().hex[:24]))

    failed = next((r for r in results if not r.success), None)
    if failed is not None:
        return ModeOutcome(
            mode=mode,
            ok=False,
            first_rows=results[0].records_transferred,
            second_rows=results[1].records_transferred if len(results) > 1 else 0,
            destination_rows=None,
            error=str(failed.error or "")[:400],
            key_addressed=key_addressed,
        )
    try:
        landed = count_rows()
    except Exception as exc:  # noqa: BLE001 — an unreadable destination is a result
        return ModeOutcome(
            mode=mode,
            ok=False,
            first_rows=results[0].records_transferred,
            second_rows=results[1].records_transferred,
            destination_rows=None,
            error=f"destination count failed: {exc}"[:400],
            key_addressed=key_addressed,
        )
    return ModeOutcome(
        mode=mode,
        ok=True,
        first_rows=results[0].records_transferred,
        second_rows=results[1].records_transferred,
        destination_rows=int(landed),
        key_addressed=key_addressed,
    )
