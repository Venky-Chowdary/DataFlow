"""Turning a stream of row fingerprints into one order-independent digest.

Split out of ``services.reconciliation`` (a module at its size budget). The
accumulator is what lets a full-population checksum cover a billion-row table
without holding every fingerprint in memory: it sorts and hashes in place until
a threshold, then spills sorted chunks to disk and merges them. The digest is
identical either way — with no spill it sorts and hashes exactly as the plain
list path does.

Ordering is by the **fingerprint**, never by the row key. Only the fingerprint is
hashed, so ordering by anything else makes the digest depend on data the digest
does not cover: a source re-read supplies the primary key as the row key while a
destination re-read supplies ``""``, so one byte-identical population sorted into
two different orders and produced two different digests. Gate-8 then reported a
checksum mismatch it could not localise — the cell-level sample compared every
row and found no differing cell. Sorting on the hashed value itself makes the
digest a property of the fingerprint multiset alone, which is what
order-independence has to mean for two sides to compare at all. Row keys stay in
the stream because the keyed compare paths downstream need them.
"""

from __future__ import annotations

import hashlib
import heapq
import os
import struct
import tempfile
from collections.abc import Iterable

from services.brand_env import getenv_brand
from services.reconcile_progress import note_hashed_rows

SPILL_THRESHOLD = int(getenv_brand("FINGERPRINT_SPILL_THRESHOLD", "1000000"))
# Publish scan progress in batches. A per-row Mongo write is what made a
# million-row checksum look frozen; the heartbeat reads the counter.
_PROGRESS_EVERY = 8192


class FingerprintAccumulator:
    """Streaming, order-independent checksum accumulator for arbitrary row counts.

    Keeps fingerprints in memory until ``DATAFLOW_FINGERPRINT_SPILL_THRESHOLD``
    is reached, then spills sorted chunks to disk and merges them at the end.
    This lets the engine compute a strict source checksum for billion-row files
    without holding every row's fingerprint in RAM.
    """

    def __init__(self, threshold: int | None = None) -> None:
        self.threshold = threshold or SPILL_THRESHOLD
        self.buffer: list[tuple[str, str]] = []
        self.chunk_files: list[str] = []
        self.total = 0
        self._progress_noted = 0
        self._tempdir: tempfile.TemporaryDirectory | None = None

    def _flush_progress(self) -> None:
        pending = self.total - self._progress_noted
        if pending <= 0:
            return
        note_hashed_rows(pending)
        self._progress_noted = self.total

    def add(self, key: str, fingerprint: str) -> None:
        self.buffer.append((key, fingerprint))
        self.total += 1
        if self.total - self._progress_noted >= _PROGRESS_EVERY:
            self._flush_progress()
        if len(self.buffer) >= self.threshold:
            self._spill()

    def add_many(self, fingerprints: Iterable[tuple[str, str]]) -> None:
        for key, fingerprint in fingerprints:
            self.add(key, fingerprint)

    def _spill(self) -> None:
        if not self.buffer:
            return
        self.buffer.sort(key=lambda x: x[1])
        if self._tempdir is None:
            self._tempdir = tempfile.TemporaryDirectory(prefix="dataflow_fp_")
        fd, path = tempfile.mkstemp(dir=self._tempdir.name, suffix=".chk")
        with os.fdopen(fd, "wb") as f:
            for key, fp in self.buffer:
                key_b = key.encode("utf-8")
                fp_b = fp.encode("utf-8")
                f.write(struct.pack(">I", len(key_b)))
                f.write(key_b)
                f.write(struct.pack(">I", len(fp_b)))
                f.write(fp_b)
        self.chunk_files.append(path)
        self.buffer = []

    def _read_chunk(self, path: str) -> Iterable[tuple[str, str]]:
        with open(path, "rb") as f:
            while True:
                key_len_b = f.read(4)
                if not key_len_b:
                    break
                key_len = struct.unpack(">I", key_len_b)[0]
                key = f.read(key_len).decode("utf-8")
                fp_len_b = f.read(4)
                if not fp_len_b:
                    break
                fp_len = struct.unpack(">I", fp_len_b)[0]
                fp = f.read(fp_len).decode("utf-8")
                yield (key, fp)

    def _sorted_stream(self) -> Iterable[tuple[str, str]]:
        if not self.chunk_files:
            self.buffer.sort(key=lambda x: x[1])
            yield from self.buffer
            return
        if self.buffer:
            self._spill()
        streams = [self._read_chunk(p) for p in self.chunk_files]
        yield from heapq.merge(*streams, key=lambda x: x[1])

    def _identity_stream(self) -> Iterable[tuple[str, str]]:
        """(key, fingerprint) pairs sorted by identity, without consuming the buffer.

        The value digest sorts by fingerprint alone so a destination re-read
        (empty key) can match a source re-read. Identity alignment is the other
        question: the same cells under a different key are not the same row.
        """
        parts: list[Iterable[tuple[str, str]]] = []
        for path in self.chunk_files:
            chunk = list(self._read_chunk(path))
            chunk.sort(key=lambda pair: (pair[0], pair[1]))
            parts.append(chunk)
        if self.buffer:
            parts.append(sorted(self.buffer, key=lambda pair: (pair[0], pair[1])))
        if not parts:
            return
        if len(parts) == 1:
            yield from parts[0]
            return
        yield from heapq.merge(*parts, key=lambda pair: (pair[0], pair[1]))

    def identity_digest(self) -> str:
        """Order-independent SHA-256 of ``key`` and fingerprint. Does not close.

        Call this before :meth:`digest`. The value digest hashes fingerprints
        only; this one also hashes the row key, so two populations with the
        same cells and different identities do not align.
        """
        self._flush_progress()
        h = hashlib.sha256()
        for key, fp in self._identity_stream():
            h.update(key.encode("utf-8"))
            h.update(b"\0")
            h.update(fp.encode("utf-8"))
            h.update(b"\n")
        return h.hexdigest()

    def digest(self) -> str:
        """Full SHA-256 hex digest (audit §2.8 — never truncate to 64 bits)."""
        self._flush_progress()
        h = hashlib.sha256()
        for _, fp in self._sorted_stream():
            h.update(fp.encode("utf-8"))
        self.close()
        return h.hexdigest()

    def close(self) -> None:
        if self._tempdir is not None:
            self._tempdir.cleanup()
            self._tempdir = None
        self.chunk_files = []
        self.buffer = []


def fingerprint_checksum(fingerprints: Iterable[tuple[str, str]]) -> str:
    """Hash a list/iterable of (row_key, fingerprint) tuples.

    For small inputs the in-memory sort+hash path is used; for large or
    streaming inputs an ``FingerprintAccumulator`` spills to disk so the
    checksum stays memory-bounded.
    """
    if isinstance(fingerprints, list) and len(fingerprints) <= SPILL_THRESHOLD:
        return _hash_fingerprints(fingerprints)
    acc = FingerprintAccumulator()
    acc.add_many(fingerprints)
    return acc.digest()


def _hash_fingerprints(fingerprints: list[tuple[str, str]]) -> str:
    fingerprints.sort(key=lambda x: x[1])
    h = hashlib.sha256()
    for _, fp in fingerprints:
        h.update(fp.encode("utf-8"))
    return h.hexdigest()
