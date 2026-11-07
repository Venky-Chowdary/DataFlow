"""Write-time raw-pin ledger for the transform-applied proof.

Split out of ``stream`` (module size budget); ``stream`` re-exports every
public name here because the engine and tests import them from there.
"""

from __future__ import annotations

import hashlib
import os
import tempfile
import threading
import weakref
from typing import Any

from services.keyset_pagination import header_column_index

_TEXT_DDL_RE = __import__("re").compile(
    r"^(?:VAR)?CHAR(?:ACTER)?(?:\s+VARYING)?|BPCHAR|(?:N)?(?:VAR)?CHAR2|TEXT|"
    r"TINYTEXT|MEDIUMTEXT|LONGTEXT|NTEXT|CLOB|NCLOB|CHARACTER|STRING|ENUM|SET\b",
    __import__("re").I,
)


def _pin_norm(v: Any, ddl: str = "") -> str:
    # Identical to the proof-side key/raw-hash normalization
    # (reconcile_step._src_norm): SOURCE-type-driven — declared text columns
    # compare literally ('01' never folds into '1', 'A ' keeps its pad),
    # numeric/date/uuid columns canonicalize so write-side COPY-text and
    # re-read driver reprs of the same value agree. NULL stays distinct
    # from ''.
    if v is None:
        return "\x00NULL\x00"
    try:
        # The read wire carries SQL NULL as the ``__DF_SQL_NULL__`` sentinel
        # string (services.value_serializer.SQL_NULL_SENTINEL); the proof's
        # re-read sees real None. Both must hash alike or every row holding
        # a NULL is miscounted as "source changed during run" (X-33).
        from services.value_serializer import SQL_NULL_SENTINEL, unescape_wire_text

        if v == SQL_NULL_SENTINEL:
            return "\x00NULL\x00"
        v = unescape_wire_text(v)
        if isinstance(v, str) and ddl and _TEXT_DDL_RE.match(str(ddl).strip()):
            return v
        from services.reconciliation import normalize_cell

        return normalize_cell(v, ddl_type=str(ddl or ""), engine="")
    except Exception:
        return str(v)


_ACTIVE_PIN_LEDGERS: "weakref.WeakSet[_StepRawLedger]" = weakref.WeakSet()


def _discard_unconsumed_pin_ledgers() -> None:
    """Remove spill files of ledgers that never reached the proof.

    Called from the transfer engine's exit path (success, refusal,
    exception, cancel): a ledger whose exception unwound _stream before
    summary() is still registered here, and its df_pin_*.tsv must not
    outlive the run. Ledgers already consumed by the proof unregistered
    themselves and are untouched.
    """
    tid = threading.get_ident()
    for ledger in list(_ACTIVE_PIN_LEDGERS):
        # Same-thread only: a concurrent run on another thread owns its
        # own ledger and must not have its in-flight spill file removed.
        if ledger._tid != tid:
            continue
        try:
            ledger.discard()
        except Exception:
            pass


class _StepRawLedger:
    """Write-time raw-pin ledger for the transform-applied proof.

    While the write path reads the source, each keyed raw row is hashed
    (all read columns) before ``shape_runner.matrix`` reshapes it. At
    verify time a key whose re-read hash equals this hash is pinned and
    judged strictly; a moved/new/missing key is drift. Past ``_SPILL_AT``
    keys the dict flushes to a temp file and later pages stream there,
    so memory stays bounded on million-key runs.

    Every instance registers itself in ``_ACTIVE_PIN_LEDGERS`` until
    ``summary()`` (handed to the proof) or ``discard()`` marks it
    consumed; the transfer engine's exit path calls
    ``_discard_unconsumed_pin_ledgers`` so no exit path — success,
    refusal, exception, cancel — can leave a ``df_pin_*.tsv`` behind.
    """

    _SPILL_AT = 50_000

    def __init__(
        self,
        key_columns: list[str],
        col_ddl: dict[str, str] | None = None,
    ) -> None:
        self.key_columns = list(key_columns or [])
        # Source-declared type per column — drives honest normalization so
        # COPY-text reprs and driver reprs of the same cell hash alike.
        self._col_ddl = dict(col_ddl or {})
        self._d: dict[str, str] = {}
        self._file: str | None = None
        self._fh = None
        self._consumed = False
        self.keys = 0
        self.hashed_columns: list[str] = []
        self._tid = threading.get_ident()
        _ACTIVE_PIN_LEDGERS.add(self)

    def _hash_row(self, row: Any) -> str:
        return hashlib.sha256(
            "\x1f".join(
                _pin_norm(v, self._col_ddl.get(h, ""))
                for h, v in zip(self.hashed_columns, row)
            ).encode()
        ).hexdigest()[:16]

    def add_page(self, headers: list[str], rows: Any) -> None:
        if not rows or not self.key_columns:
            return
        self.hashed_columns = list(headers)
        kid = [header_column_index(headers, k) for k in self.key_columns]
        if any(i is None for i in kid):
            return
        for row in rows:
            try:
                key = "\x1f".join(
                    _pin_norm(row[i], self._col_ddl.get(headers[i], ""))
                    for i in kid
                )
            except (IndexError, TypeError):
                continue
            self._put(key, self._hash_row(row))
            self.keys += 1

    def _put(self, key: str, h: str) -> None:
        if self._fh is not None:
            self._fh.write(f"{key}\x00{h}\n")
            return
        self._d[key] = h
        if len(self._d) > self._SPILL_AT:
            fd, self._file = tempfile.mkstemp(
                prefix="df_pin_", suffix=".tsv"
            )
            self._fh = os.fdopen(fd, "w")
            for k2, h2 in self._d.items():
                self._fh.write(f"{k2}\x00{h2}\n")
            self._d = {}

    def __del__(self) -> None:
        # If _stream dies before summary() hands the spill file to the
        # transform proof, nobody else will remove it.
        try:
            if not self._consumed:
                if self._fh is not None:
                    self._fh.close()
                    self._fh = None
                if self._file:
                    try:
                        os.unlink(self._file)
                    except OSError:
                        pass
        except Exception:
            pass

    def discard(self) -> None:
        """Remove the spill file when the run dies before the proof can
        consume it; called for every still-active ledger from the
        engine's exit path."""
        self._consumed = True
        _ACTIVE_PIN_LEDGERS.discard(self)
        try:
            if self._fh is not None:
                self._fh.close()
                self._fh = None
            if self._file:
                os.unlink(self._file)
                self._file = None
        except Exception:
            pass

    def summary(self) -> dict[str, Any] | None:
        # Handed to the transform proof via dest_summary — the proof/engine
        # finally owns the spill file from here, so unregister now.
        _ACTIVE_PIN_LEDGERS.discard(self)
        if self._fh is not None:
            self._fh.close()
            self._fh = None
        if not self.keys:
            return None
        self._consumed = True
        out: dict[str, Any] = {
            "keys": self.keys,
            "key_columns": list(self.key_columns),
            "hashed_columns": list(self.hashed_columns),
        }
        if self._file:
            out["file"] = self._file
        else:
            out["entries"] = self._d
        return out
