"""What this run wrote, per key: the witness that separates concurrent source drift from loss.

A route that re-reads the source after the write (the streaming SQL route, and
any route whose ladder gets ``records == []``) grades the destination against a
source scan taken *after* the write. A concurrent ``UPDATE`` on the source
between the write and that scan makes the two sides differ on a key the
destination holds exactly as written; a destination that lost or rewrote a
cell makes them differ too. Source and destination alone cannot tell the two
apart: only what this run wrote can.

The write pass renders every row it sends through the Gate-8 fingerprint
(``row_fingerprints`` over the mapped, collapsed rows). The witness keeps, per
written key, a digest of that fingerprint, together with the encoder that made
it (columns, bind dialect, digest types). The ladder re-hashes its own
populations with exactly that encoder, so both sides are rendered alike.

Keys are the row's key cells themselves (canonical per-cell renderings held as
a tuple), never parsed back out of a joined fingerprint string, so a cell
value can never pose as a key label.

A witness attributes nothing unless it is complete: every chunk was
fingerprinted, the population fits the ladder budget, the run did not resume
part-way, and the rows it saw equal the rows the writer reports. At verify
time, per key:

* source equals what was written, destination differs -> loss, vetoes;
* source moved, destination equals what was written -> concurrent source
  update: advisory, named, never a veto (the key leaves the cell layers);
* source deleted the written key, destination equals what was written ->
  concurrent source delete: advisory, named;
* a written key the destination no longer holds (or holds differently) while
  the source deleted it -> loss, vetoes (it is in neither compared population);
* a source key this run never wrote and the destination does not hold ->
  concurrent insert, a separate disclosed category; only when Gate-8's own
  source count equals the witnessed rows (the key was not in the write-time
  population). L1 and the census still grade the counts.

The witness lives in this process only, keyed by job id. It is released when
the job ends (``keyed_write_witness_scope`` in ``execute_tracked``) whatever the
outcome, and when a resumed attempt declines to start one. An in-flight
witness is never evicted: past ``_MAX_JOBS`` live witnesses a new run does not
start one and says why.
"""

from __future__ import annotations

import hashlib
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, Iterable

#: Report key carrying the disclosed drift.
DRIFT_KEY = "concurrent_source_drift"
#: Report key carrying a discrepancy the ladder could not attribute.
UNATTRIBUTED_KEY = "value_discrepancy_unattributed"
#: dest_summary key: whether this run started a witness, and why not.
SUMMARY_KEY = "keyed_write_witness"
_SAMPLE = 20
_MAX_JOBS = 4
_LOCK = threading.Lock()
_REGISTRY: dict[str, "KeyedWriteWitness"] = {}

KeyCells = tuple[str, ...]


def _h(text: str) -> int:
    return int.from_bytes(
        hashlib.blake2b(text.encode("utf-8", "surrogatepass"), digest_size=8).digest(), "big"
    )


class Encoder:
    """The exact Gate-8 fingerprint parameters one side was rendered with."""

    __slots__ = ("columns", "dest_db_type", "dest_types", "_ddl")

    def __init__(self, columns: Iterable[str], dest_db_type: str, dest_types: dict | None) -> None:
        from services.reconciliation import _column_fingerprint_ddl

        self.columns = [str(c) for c in columns]
        self.dest_db_type = str(dest_db_type or "").strip().lower()
        self.dest_types = dict(dest_types or {})
        self._ddl = {
            c: _column_fingerprint_ddl(c, self.dest_db_type, self.dest_types) for c in self.columns
        }

    def row_digests(self, rows: list[Any]) -> list[int]:
        from services.reconciliation import _iter_fingerprints

        return [
            _h(fp)
            for _rk, fp in _iter_fingerprints(
                rows, self.columns, dest_db_type=self.dest_db_type, dest_types=self.dest_types
            )
        ]

    def key_cells(self, row: Any, key_columns: list[str]) -> KeyCells | None:
        """Canonical rendering of the row's own key cells (tuple or dict row)."""
        from services.reconciliation import _fingerprint_with_ddl

        out: list[str] = []
        for col in key_columns:
            if isinstance(row, dict):
                if col in row:
                    val = row[col]
                else:
                    hit = next((k for k in row if str(k).lower() == col.lower()), None)
                    if hit is None:
                        return None
                    val = row[hit]
            else:
                idx = next((i for i, c in enumerate(self.columns) if c.lower() == col.lower()), -1)
                if idx < 0 or idx >= len(row):
                    return None
                val = row[idx]
            out.append(_fingerprint_with_ddl(val, self._ddl.get(col, ""), self.dest_db_type))
        return tuple(out)

    def describe(self) -> dict[str, Any]:
        return {"dest_db_type": self.dest_db_type, "columns": len(self.columns)}


class KeyedWriteWitness:
    """``{key cells: row-fingerprint digest}`` of every row this run wrote."""

    def __init__(self, key_columns: Iterable[str], *, max_rows: int, encoder: Encoder | None = None) -> None:
        self.key_columns = [str(c) for c in key_columns if str(c or "")]
        self.max_rows = int(max_rows)
        self.encoder = encoder
        self.rows: dict[KeyCells, int] = {}
        self.rows_added = 0
        self.complete = bool(self.key_columns) and encoder is not None
        self.reason = "" if self.complete else "no_key_columns_or_encoder"

    def _give_up(self, reason: str) -> None:
        self.complete = False
        self.reason = self.reason or reason
        self.rows = {}

    def add_rows(self, rows: list[Any], digests: list[int] | None = None) -> None:
        """Record rows exactly as the writer is sent them (mapped, collapsed)."""
        if not self.complete or self.encoder is None:
            return
        digests = digests if digests is not None else self.encoder.row_digests(rows)
        if len(digests) != len(rows):
            self._give_up("chunk_not_witnessed")
            return
        for row, digest in zip(rows, digests):
            key = self.encoder.key_cells(row, self.key_columns)
            if key is None:
                self._give_up("key_not_on_row")
                return
            self.rows_added += 1
            # A later write of the same key is what the destination holds.
            self.rows[key] = digest
            if len(self.rows) > self.max_rows:
                self._give_up("population_exceeds_ladder_budget")
                return


def begin_keyed_write_witness(
    *, job_id: Any, key_columns: list[str] | None, sync_mode: str,
    inline_fingerprints_used: bool, resumed: bool, expected_rows: int = 0,
    columns: list[str] | None = None, dest_db_type: str = "",
    dest_types: dict | None = None, summary: dict | None = None,
) -> KeyedWriteWitness | None:
    """Start (and register) a witness for a keyed re-read route, else None.

    Whenever this returns None the job's registry entry is dropped, so a stale
    witness from a failed earlier attempt can never be taken by a resumed run.
    """
    from services.primary_key import sync_collapses_in_file_duplicates
    from services.sync_cursor import is_overwrite_sync
    from services.verification_ladder import MAX_LADDER_ROWS

    jid = str(job_id or "")
    reason = ""
    if inline_fingerprints_used:
        reason = "write_pass_already_fingerprinted"
    elif resumed:
        reason = "resumed_attempt_sees_part_of_the_population"
    elif not jid or not key_columns:
        reason = "no_job_or_key"
    elif is_overwrite_sync(sync_mode) or not sync_collapses_in_file_duplicates(sync_mode):
        reason = "not_a_keyed_upsert"
    elif int(expected_rows or 0) > MAX_LADDER_ROWS:
        reason = "population_exceeds_ladder_budget"
    witness: KeyedWriteWitness | None = None
    with _LOCK:
        _REGISTRY.pop(jid, None)
        if not reason and len(_REGISTRY) >= _MAX_JOBS:
            reason = "witness_capacity_in_use_by_other_jobs"
        if not reason:
            witness = KeyedWriteWitness(
                key_columns or [], max_rows=MAX_LADDER_ROWS,
                encoder=Encoder(columns or [], dest_db_type, dest_types),
            )
            _REGISTRY[jid] = witness
    if isinstance(summary, dict):
        summary[SUMMARY_KEY] = {"started": witness is not None, "reason": reason}
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
    the job's witness leaves memory when the job ends."""
    try:
        yield
    finally:
        take_keyed_write_witness(job_id)


def _not_attributed(reason: str, source_rows, target_rows):
    return source_rows, target_rows, {"attributed": False, "reason": reason}


def split_concurrent_source_drift(
    *,
    witness: KeyedWriteWitness | None,
    source_rows: list[dict],
    target_rows: list[dict],
    key_columns: list[str],
    rows_written: int | None = None,
    gate8_source_rows: int | None = None,
    full_target_rows: list[dict] | None = None,
    excluded_key_values: Iterable[tuple] = (),
    not_started_reason: str = "",
) -> tuple[list[dict], list[dict], dict[str, Any]]:
    """Remove from the cell layers exactly the keys the witness proves were
    changed at the source after the write. Returns the populations and an
    attribution record; ``record["attributed"]`` is True only for a usable,
    complete witness. ``record["losses"]`` lists written keys lost outside
    both compared populations (the caller must fail the run on them).
    """
    if witness is None:
        return _not_attributed(not_started_reason or "no_write_witness", source_rows, target_rows)
    if not witness.complete or witness.encoder is None:
        return _not_attributed(witness.reason or "witness_incomplete", source_rows, target_rows)
    if [c.lower() for c in key_columns] != [c.lower() for c in witness.key_columns]:
        return _not_attributed("key_columns_differ", source_rows, target_rows)
    if rows_written is None or int(rows_written) != witness.rows_added:
        # The writer sent rows the witness did not see (or the reverse).
        return _not_attributed("witnessed_rows_differ_from_rows_written", source_rows, target_rows)
    enc = witness.encoder
    keys = witness.key_columns

    def _index(rows: list[dict]) -> tuple[dict[KeyCells, tuple[dict, int]], list[KeyCells | None]]:
        out: dict[KeyCells, tuple[dict, int]] = {}
        order: list[KeyCells | None] = []
        for row, digest in zip(rows, enc.row_digests(rows)):
            k = enc.key_cells(row, keys)
            order.append(k)
            if k is not None:
                out[k] = (row, digest)
        return out, order

    src, src_order = _index(source_rows)
    dst, dst_order = _index(target_rows)
    if len(src) != len(source_rows) or len(dst) != len(target_rows):
        return _not_attributed("key_not_on_row", source_rows, target_rows)
    full = dst if full_target_rows is None else _index(full_target_rows)[0]
    w = witness.rows
    agree = sum(
        1 for k, (_r, d) in src.items() if w.get(k) is not None and (d == w[k] or dst.get(k, (None, None))[1] == w[k])
    )
    if src and w and not agree:
        # Neither side equals the witness on any key: the renderings disagree
        # (or every row was rewritten twice). Do not guess.
        return _not_attributed("witness_scheme_mismatch", source_rows, target_rows)
    moved: set[KeyCells] = set()
    inserted: set[KeyCells] = set()
    for k, (_row, s_digest) in src.items():
        written = w.get(k)
        if written is None:
            if k not in dst:
                inserted.add(k)
            continue  # never drift: a key this run did not write
        if s_digest != written and dst.get(k, (None, None))[1] == written:
            moved.add(k)
    # Keys held out to quarantine never entered the destination by design.
    held_out = {
        enc.key_cells(dict(zip(keys, vals)), keys) for vals in excluded_key_values
    }
    deleted: list[KeyCells] = []
    losses: list[KeyCells] = []
    for k, written in w.items():
        if k in src or k in held_out:
            continue
        held = full.get(k)
        if held is not None and held[1] == written:
            deleted.append(k)  # the source deleted it after the write
        else:
            losses.append(k)  # in neither population and not as written
    snapshot_ok = gate8_source_rows is not None and int(gate8_source_rows) == witness.rows_added
    if inserted and not snapshot_ok:
        inserted = set()  # cannot prove the key was not in the write-time population
    excused = moved | inserted

    def _named(ks: Iterable[KeyCells], rows_by_key: dict) -> list[dict]:
        out = []
        for k in list(ks)[:_SAMPLE]:
            row = rows_by_key.get(k, (None,))[0]
            out.append({c: (row.get(c) if isinstance(row, dict) else cell) for c, cell in zip(keys, k)})
        return out

    record: dict[str, Any] = {
        "attributed": True,
        "count": len(moved),
        "sample_keys": _named(moved, src),
        "inserted_count": len(inserted),
        "inserted_sample_keys": _named(inserted, src),
        "deleted_count": len(deleted),
        "deleted_sample_keys": _named(deleted, full),
        "losses": _named(losses, {}),
        "loss_count": len(losses),
    }
    if excused:
        source_rows = [r for r, k in zip(source_rows, src_order) if k not in excused]
        target_rows = [r for r, k in zip(target_rows, dst_order) if k not in excused]
    return source_rows, target_rows, record


def _render(keys: list[dict], total: int) -> str:
    shown = ", ".join(
        "(" + ", ".join(f"{k}={v!r}" for k, v in key.items()) + ")" for key in keys[:5]
    )
    more = total - min(len(keys), 5)
    return shown + (f" and {more} more" if more > 0 else "")


def _with_sentence(out: dict[str, Any], sentence: str) -> dict[str, Any]:
    out["warnings"] = [*list(out.get("warnings") or []), sentence]
    base = str(out.get("message") or "").rstrip()
    if sentence not in base:
        out["message"] = f"{base} {sentence}".strip() if base else sentence
    return out


def attach_concurrent_drift(report: dict[str, Any], record: dict[str, Any]) -> dict[str, Any]:
    """Disclose drift on the report (warnings naming the keys); fail on losses."""
    if not record or not record.get("attributed"):
        return report
    moved = int(record.get("count") or 0)
    ins = int(record.get("inserted_count") or 0)
    dele = int(record.get("deleted_count") or 0)
    lost = int(record.get("loss_count") or 0)
    if not (moved or ins or dele or lost):
        return report
    out = dict(report)
    out[DRIFT_KEY] = {k: v for k, v in record.items() if k != "attributed"}
    if moved:
        out = _with_sentence(out, (
            f"The source changed after this run wrote {moved} key(s) "
            f"{_render(record['sample_keys'], moved)}; the destination holds the values "
            "this run wrote, so the difference is a concurrent source update, not a loss. "
            "The next run picks it up."
        ))
    if ins:
        out = _with_sentence(out, (
            f"{ins} source key(s) {_render(record['inserted_sample_keys'], ins)} appeared "
            "after this run read the source (concurrent insert); this run did not write "
            "them. Row counts are still graded by the key census."
        ))
    if dele:
        out = _with_sentence(out, (
            f"The source deleted {dele} key(s) {_render(record['deleted_sample_keys'], dele)} "
            "after this run wrote them (concurrent delete); the destination holds the "
            "values this run wrote."
        ))
    if lost:
        out["passed"] = False
        out = _with_sentence(out, (
            f"{lost} key(s) this run wrote {_render(record['losses'], lost)} are missing "
            "from the destination or differ from what was written, and the source no "
            "longer holds them: a destination loss."
        ))
    return out


def attach_unattributed_discrepancy(
    report: dict[str, Any], ladder: dict[str, Any] | None, reason: str
) -> dict[str, Any]:
    """A batch-scoped re-read the ladder could not attribute: say what it did
    not prove (never silently green)."""
    layers = (ladder or {}).get("layers") or {}
    l2 = layers.get("L2") or {}
    l3 = layers.get("L3") or {}
    l2_details = l2.get("details") or {}
    l3_skipped = bool((l3.get("details") or {}).get("skipped"))
    l2_skipped = bool(l2_details.get("skipped"))
    l2_miss = l2.get("passed") is False and not l2_skipped
    if not (l2_miss or l2_skipped or l3_skipped):
        return report  # L2 and L3 both compared the scoped cells and agreed
    cols = [str(c) for c in l2_details.get("mismatched_columns") or []]
    out = dict(report)
    out[UNATTRIBUTED_KEY] = {
        "columns": cols, "reason": reason,
        "l2": "skipped" if l2_skipped else ("failed" if l2_miss else "passed"),
        "l3": "skipped" if l3_skipped else ("passed" if l3.get("passed") else "failed"),
    }
    parts = []
    if l2_miss:
        parts.append(
            "column aggregates differ between the source and the destination in "
            f"{', '.join(cols) or 'one or more columns'}"
        )
    if l2_skipped:
        parts.append("the column-aggregate layer did not compare these rows")
    if l3_skipped:
        parts.append(
            "the row checksum was not compared on the written keys, so a value swapped "
            "between two written rows would not be seen"
        )
    sentence = (
        "Per-cell fidelity is not proven for the keys this run wrote: "
        + "; ".join(parts)
        + ". The source was re-read after the write and this run holds no usable record "
        f"of what it wrote ({reason}), so a concurrent source update cannot be told from "
        "a destination loss."
    )
    return _with_sentence(out, sentence)
