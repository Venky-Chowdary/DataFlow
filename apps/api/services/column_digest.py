"""Per-column digests that ride the Gate-8 row fingerprints.

A strict Gate-8 failure compares two order-independent population digests. When
they disagree, the run used to have only two other witnesses, and neither looks
at what the digests hash:

* the key-aligned sample compares at most the first 50 rows the write pass
  held, so a value outside them is never seen (QA round 12: 4,227 Rfam
  ``family`` rows, "no differing cell in 50 sampled rows");
* the failure-path key census hashes raw driver values with its own, looser
  canonicalizer, so a cell the two digests render differently can look equal
  to it.

Each row fingerprint is ``label=cell`` parts joined by ``\\x1f``, built from the
exact canonical cells the digest hashes. Summing a small hash of every part per
label gives one multiset digest per column on each side at almost no cost
(the parts are already rendered). When the row digests differ, the columns
whose sums differ are the columns whose canonical cells differ — named from
the same evidence the verdict used, never guessed.

Diagnostic only: these sums never decide a verdict.
"""

from __future__ import annotations

import zlib
from contextvars import ContextVar
from typing import Any

PART_SEP = "\x1f"
#: A fingerprint that is not ``label=cell`` parts (an unlabelled matrix row)
#: must not turn every distinct value into a "column". Past these limits the
#: tracker switches itself off rather than grow without bound.
_MAX_LABELS = 1024
_MAX_LABEL_LEN = 256
_MOD = 1 << 64

SOURCE_KEY = "source_column_digests"
DEST_KEY = "dest_column_digests"


class ColumnDigests:
    """Order-independent per-label multiset digests of fingerprint parts."""

    __slots__ = ("sums", "counts", "enabled")

    def __init__(self) -> None:
        self.sums: dict[str, int] = {}
        self.counts: dict[str, int] = {}
        self.enabled = True

    def add(self, fingerprint: str) -> None:
        if not self.enabled or not fingerprint:
            return
        sums = self.sums
        counts = self.counts
        for part in fingerprint.split(PART_SEP):
            cut = part.find("=")
            if cut <= 0 or cut > _MAX_LABEL_LEN:
                continue
            label = part[:cut]
            if label not in sums:
                if len(sums) >= _MAX_LABELS:
                    self.disable()
                    return
                sums[label] = 0
                counts[label] = 0
            sums[label] += zlib.crc32(part.encode("utf-8", "surrogatepass"))
            counts[label] += 1

    def disable(self) -> None:
        self.enabled = False
        self.sums = {}
        self.counts = {}

    def snapshot(self) -> dict[str, str]:
        """``{label: "<cells>:<hex>"}`` — strings, so any job store can hold it."""
        if not self.enabled:
            return {}
        return {
            label: f"{self.counts[label]}:{self.sums[label] % _MOD:016x}"
            for label in self.sums
        }


def stamp(summary: dict[str, Any], key: str, checksum: str, columns: dict[str, str]) -> None:
    """Record per-column digests next to the row digest they belong to."""
    if not isinstance(summary, dict) or not checksum or not columns:
        return
    summary[key] = {"checksum": str(checksum), "columns": dict(columns)}


def stamp_source(summary: dict[str, Any], accumulator: Any, checksum: str) -> None:
    tracker = getattr(accumulator, "columns", None)
    if isinstance(tracker, ColumnDigests):
        stamp(summary, SOURCE_KEY, checksum, tracker.snapshot())


_LAST_READBACK: ContextVar[tuple[str, dict[str, str]] | None] = ContextVar(
    "df_last_readback_columns", default=None
)


def note_readback(checksum: str, columns: dict[str, str]) -> None:
    """Remember the per-column digests of the read-back that produced ``checksum``."""
    _LAST_READBACK.set((str(checksum or ""), dict(columns or {})))


def clear_readback() -> None:
    _LAST_READBACK.set(None)


def stamp_readback(summary: dict[str, Any], checksum: str) -> None:
    """Stamp the last read-back's columns when it produced ``checksum``."""
    last = _LAST_READBACK.get()
    if last and checksum and last[0] == str(checksum):
        stamp(summary, DEST_KEY, checksum, last[1])


def differing_columns(
    summary: dict[str, Any] | None,
    *,
    source_checksum: str,
    target_checksum: str,
) -> list[str] | None:
    """Columns whose canonical cells differ between the two digests.

    ``None`` when either side's per-column digests are missing or belong to a
    different digest than the ones the verdict compared; ``[]`` when every
    column holds the same multiset of canonical cells on both sides.
    """
    if not isinstance(summary, dict):
        return None
    src = summary.get(SOURCE_KEY) or {}
    dst = summary.get(DEST_KEY) or {}
    if not isinstance(src, dict) or not isinstance(dst, dict):
        return None
    if not source_checksum or str(src.get("checksum") or "") != str(source_checksum):
        return None
    if not target_checksum or str(dst.get("checksum") or "") != str(target_checksum):
        return None
    s_cols = src.get("columns") or {}
    d_cols = dst.get("columns") or {}
    if not s_cols or not d_cols:
        return None
    return sorted(
        label
        for label in set(s_cols) | set(d_cols)
        if s_cols.get(label) != d_cols.get(label)
    )
