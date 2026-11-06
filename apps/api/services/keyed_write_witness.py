"""What this run wrote, per key: the witness that separates concurrent source drift from loss.

A route that re-reads the source after the write (the streaming SQL route, and
any route whose ladder gets ``records == []``) grades the destination against a
source scan taken *after* the write. A concurrent ``UPDATE`` on the source
between the write and that scan makes the two sides differ on a key the
destination holds exactly as written; a destination that lost or rewrote a
cell makes them differ too. Source and destination alone cannot tell the two
apart: only what this run wrote can.

The write pass already renders every row it sends through the Gate-8
fingerprint (``row_fingerprints`` over the mapped, collapsed rows), and the
ladder renders its source and destination populations through the same
function. The witness keeps one small digest per written key. At verify time,
for every key:

* the source still equals what was written -> the source/destination
  comparison stands; a destination that differs is a real loss and vetoes;
* the source moved since the write, and the destination equals what was
  written -> concurrent source drift: advisory, named on the report, never a
  veto (the key leaves the compared populations);
* the source moved and the destination does not equal what was written
  either -> the destination is not what this run wrote: it stays compared.

The witness lives in this process only (keyed by job id), is bounded by the
ladder budget, and is released when the job ends (``keyed_write_witness_scope``
in ``execute_tracked``), whatever the outcome. A run resumed at reconcile, or a route that never filled it,
has no witness; the ladder then cannot attribute a difference and says so.
"""

from __future__ import annotations

import hashlib
import threading
from collections import OrderedDict
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, Iterable

PART_SEP = "\x1f"
#: Report key carrying the disclosed drift.
DRIFT_KEY = "concurrent_source_drift"
#: Report key carrying a discrepancy the ladder could not attribute.
UNATTRIBUTED_KEY = "value_discrepancy_unattributed"
_SAMPLE = 20
_MAX_JOBS = 4
_LOCK = threading.Lock()
_REGISTRY: "OrderedDict[str, KeyedWriteWitness]" = OrderedDict()


def _h(text: str) -> int:
    return int.from_bytes(
        hashlib.blake2b(text.encode("utf-8", "surrogatepass"), digest_size=8).digest(), "big"
    )


def fingerprint_key(fingerprint: str, key_labels: tuple[str, ...]) -> int | None:
    """Digest of the key cells inside a ``label=cell`` fingerprint, or None."""
    cells: dict[str, str] = {}
    for part in str(fingerprint or "").split(PART_SEP):
        label, sep, cell = part.partition("=")
        if sep and label in key_labels:
            cells[label] = cell
    if not key_labels or len(cells) != len(key_labels):
        return None
    return _h(PART_SEP.join(f"{lab}={cells[lab]}" for lab in key_labels))


class KeyedWriteWitness:
    """``{key digest: row-fingerprint digest}`` of every row this run wrote."""

    def __init__(self, key_columns: Iterable[str], *, max_rows: int) -> None:
        self.key_labels = tuple(str(c).lower() for c in key_columns if str(c or ""))
        self.max_rows = int(max_rows)
        self.rows: dict[int, int] = {}
        self.complete = bool(self.key_labels)
        self.reason = "" if self.complete else "no_key_columns"

    def _give_up(self, reason: str) -> None:
        self.complete = False
        self.reason = reason
        self.rows = {}

    def add_many(self, pairs: Iterable[tuple[str, str]]) -> None:
        if not self.complete:
            return
        for _row_key, fingerprint in pairs:
            key = fingerprint_key(fingerprint, self.key_labels)
            if key is None:
                self._give_up("key_not_in_fingerprint")
                return
            # A later write of the same key is what the destination holds.
            self.rows[key] = _h(str(fingerprint))
            if len(self.rows) > self.max_rows:
                self._give_up("population_exceeds_ladder_budget")
                return


def begin_keyed_write_witness(
    *, job_id: Any, key_columns: list[str] | None, sync_mode: str,
    inline_fingerprints_used: bool, resumed: bool, expected_rows: int = 0,
) -> KeyedWriteWitness | None:
    """Start (and register) a witness for a keyed re-read route, else None.

    Only a route that does not already fingerprint its write pass, writes by
    key, and is not resuming (a resumed session sees part of the population)
    needs one. Overwrite routes replace the table, so there is no occupied
    destination for the ladder to batch-scope.
    """
    from services.primary_key import sync_collapses_in_file_duplicates
    from services.sync_cursor import is_overwrite_sync
    from services.verification_ladder import MAX_LADDER_ROWS

    if (
        inline_fingerprints_used
        or resumed
        or not job_id
        or not key_columns
        or is_overwrite_sync(sync_mode)
        or not sync_collapses_in_file_duplicates(sync_mode)
        or int(expected_rows or 0) > MAX_LADDER_ROWS
    ):
        return None
    witness = KeyedWriteWitness(key_columns, max_rows=MAX_LADDER_ROWS)
    with _LOCK:
        _REGISTRY[str(job_id)] = witness
        _REGISTRY.move_to_end(str(job_id))
        while len(_REGISTRY) > _MAX_JOBS:
            _REGISTRY.popitem(last=False)
    return witness


def take_keyed_write_witness(job_id: Any) -> KeyedWriteWitness | None:
    """Remove and return this job's witness (each run verifies once)."""
    if not job_id:
        return None
    with _LOCK:
        return _REGISTRY.pop(str(job_id), None)


def held_witness_job_ids() -> list[str]:
    """Job ids whose witness is still in memory (diagnostics and tests)."""
    with _LOCK:
        return list(_REGISTRY)


@contextmanager
def keyed_write_witness_scope(job_id: Any) -> Iterator[None]:
    """Job scope: whatever the outcome (completed, failed, cancelled, raised),
    the job's witness leaves memory when the job ends. The ladder normally
    takes it first; a run that fails before the ladder would otherwise keep
    up to a ladder budget of digests alive until four later jobs evict it."""
    try:
        yield
    finally:
        take_keyed_write_witness(job_id)


def _indexed(rows: list[dict], columns: list[str], labels: tuple[str, ...], **fp_kw: Any):
    """``({key: (row, row digest)}, [key of each row in order])``."""
    from services.reconciliation import _iter_fingerprints

    out: dict[int, tuple[dict, int]] = {}
    order: list[int | None] = []
    for row, (_rk, fp) in zip(rows, _iter_fingerprints(rows, columns, **fp_kw)):
        key = fingerprint_key(fp, labels)
        order.append(key)
        if key is not None:
            out[key] = (row, _h(fp))
    return out, order


def split_concurrent_source_drift(
    *,
    witness: KeyedWriteWitness | None,
    source_rows: list[dict],
    target_rows: list[dict],
    columns: list[str],
    key_columns: list[str],
    dest_db_type: str,
    dest_types: dict[str, str] | None,
) -> tuple[list[dict], list[dict], dict[str, Any]]:
    """Remove keys whose source moved after the write while the destination
    holds what was written. Returns the populations and an attribution record.

    ``attribution["attributed"]`` is True only when the witness was usable, so
    the caller may treat the populations as "this run's written cells".
    """
    if witness is None or not witness.complete or not witness.rows:
        return source_rows, target_rows, {
            "attributed": False,
            "reason": "no_write_witness" if witness is None else (witness.reason or "empty"),
        }
    labels = witness.key_labels
    if {str(c).lower() for c in key_columns} != set(labels):
        return source_rows, target_rows, {"attributed": False, "reason": "key_columns_differ"}
    fp_kw: dict[str, Any] = {"dest_db_type": dest_db_type, "dest_types": dest_types}
    src, src_order = _indexed(source_rows, columns, labels, **fp_kw)
    dst, dst_order = _indexed(target_rows, columns, labels, **fp_kw)
    if len(src) != len(source_rows):
        return source_rows, target_rows, {"attributed": False, "reason": "key_not_in_fingerprint"}
    agree = 0
    drifted: set[int] = set()
    for key, (_row, s_digest) in src.items():
        written = witness.rows.get(key)
        d_digest = dst[key][1] if key in dst else None
        if written is not None and (s_digest == written or d_digest == written):
            agree += 1
        if s_digest == written:
            continue  # the source is what was written: the comparison stands
        if d_digest == written:  # both None: written after the run, absent in dest
            drifted.add(key)
    if src and not agree and not drifted:
        # Neither side equals the witness on any key: the two fingerprint
        # schemes disagree (or every row was rewritten twice). Do not guess.
        return source_rows, target_rows, {"attributed": False, "reason": "witness_scheme_mismatch"}
    record: dict[str, Any] = {"attributed": True, "count": len(drifted)}
    if drifted:
        sample = [src[k][0] for k in list(drifted)[:_SAMPLE]]
        record["sample_keys"] = [
            {c: r.get(c) for c in key_columns} for r in sample
        ]
        source_rows = [r for r, k in zip(source_rows, src_order) if k not in drifted]
        target_rows = [r for r, k in zip(target_rows, dst_order) if k not in drifted]
    return source_rows, target_rows, record


def _render_keys(record: dict[str, Any]) -> str:
    keys = record.get("sample_keys") or []
    shown = ", ".join(
        "(" + ", ".join(f"{k}={v!r}" for k, v in key.items()) + ")" for key in keys[:5]
    )
    more = int(record.get("count") or 0) - min(len(keys), 5)
    return shown + (f" and {more} more" if more > 0 else "")


def attach_concurrent_drift(report: dict[str, Any], record: dict[str, Any]) -> dict[str, Any]:
    """Disclose drift on the report: a warning naming the keys, never a veto."""
    if not record or not int(record.get("count") or 0):
        return report
    out = dict(report)
    out[DRIFT_KEY] = {k: v for k, v in record.items() if k != "attributed"}
    sentence = (
        f"The source changed after this run wrote {int(record['count'])} key(s) "
        f"{_render_keys(record)}; the destination holds the values this run wrote, "
        "so the difference is a concurrent source update, not a loss. The next run "
        "picks it up."
    )
    out["warnings"] = [*list(out.get("warnings") or []), sentence]
    base = str(out.get("message") or "").rstrip()
    if sentence not in base:
        out["message"] = f"{base} {sentence}".strip() if base else sentence
    return out


def attach_unattributed_discrepancy(
    report: dict[str, Any], ladder: dict[str, Any] | None, reason: str
) -> dict[str, Any]:
    """An L2 value miss the ladder could not attribute stays visible (never silent green)."""
    layers = (ladder or {}).get("layers") or {}
    l2 = layers.get("L2") or {}
    details = l2.get("details") or {}
    if l2.get("passed") is not False or details.get("skipped"):
        return report
    cols = [str(c) for c in details.get("mismatched_columns") or []]
    out = dict(report)
    out[UNATTRIBUTED_KEY] = {"columns": cols, "reason": reason}
    sentence = (
        "Column aggregates differ between the source and the destination in "
        f"{', '.join(cols) or 'one or more columns'}, but the source was re-read after "
        "the write and this run holds no record of what it wrote, so a concurrent "
        "source update cannot be told from a destination loss. Per-cell fidelity is "
        "not proven for these columns."
    )
    out["warnings"] = [*list(out.get("warnings") or []), sentence]
    base = str(out.get("message") or "").rstrip()
    if sentence not in base:
        out["message"] = f"{base} {sentence}".strip() if base else sentence
    return out
