"""Row-count and checksum accounting for one transfer.

Loaded + quarantined + skipped must equal the source count. The checksum is
a multiset hash, so order does not matter and a quarantined row is still
part of the source. CDC stays at-least-once upsert; this does not claim
exactly-once.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from services.row_conservation import DEST_READBACK, account_population


def multiset_checksum(rows: list[dict[str, Any]] | None) -> str:
    """Order-independent checksum of a row multiset."""
    parts: list[str] = []
    for row in rows or []:
        payload = json.dumps(row, sort_keys=True, default=str, separators=(",", ":"))
        parts.append(hashlib.sha256(payload.encode()).hexdigest())
    parts.sort()
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()


def account_transfer(
    *,
    source_rows: list[dict[str, Any]],
    landed_rows: list[dict[str, Any]],
    quarantined_rows: list[dict[str, Any]] | None = None,
    skipped_rows: list[dict[str, Any]] | None = None,
    sync_mode: str = "full_refresh_append",
) -> dict[str, Any]:
    """100% accounted means the ledger balances and the checksums partition.

    Measured on the rows passed in — never a claim about an unmeasured run.
    """
    quarantined_rows = list(quarantined_rows or [])
    skipped_rows = list(skipped_rows or [])
    source_rows = list(source_rows or [])
    landed_rows = list(landed_rows or [])
    ledger = account_population(
        rows_read=len(source_rows),
        dest_count=len(landed_rows),
        dest_count_source=DEST_READBACK,
        dest_count_before=0,
        rejected_rows=len(quarantined_rows),
        coerced_null_rows=0,
        rows_skipped=len(skipped_rows),
        writer_ack=len(landed_rows),
        sync_mode=sync_mode,
    )
    source_checksum = multiset_checksum(source_rows)
    accounted_checksum = multiset_checksum(landed_rows + quarantined_rows + skipped_rows)
    equation = (
        len(landed_rows) + len(quarantined_rows) + len(skipped_rows)
    )
    return {
        "balanced": bool(ledger.balanced) and ledger.unaccounted == 0 and equation == len(source_rows),
        "rows_read": len(source_rows),
        "rows_loaded": len(landed_rows),
        "rows_quarantined": len(quarantined_rows),
        "rows_skipped": len(skipped_rows),
        "unaccounted": ledger.unaccounted,
        "source_checksum": source_checksum,
        "accounted_checksum": accounted_checksum,
        "checksums_match": source_checksum == accounted_checksum,
        "delivery": "at_least_once_upsert",
    }
