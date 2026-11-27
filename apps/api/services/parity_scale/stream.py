"""The streamed fingerprint pass: columns CE1 does not encode in-engine.

Floats across engines, JSON, intervals, arrays, transformed and uncertified
columns are compared here with Gate-8's *own* fingerprint
(:func:`services.reconciliation.fingerprint_for_reconcile`), so fingerprint v2
semantics hold exactly. Each side is read once, streamed in batches, and
reduced to ``(key identity, SHA-256 of the row's fingerprints)``; both reduced
streams spill to a temporary SQLite file on disk and are merged there by SQL,
so memory stays bounded by the batch size whatever the table size. Disk is
about 100 bytes per row per side.

Three shapes:

* ``changed`` — keyed table whose key is encoded in-engine: the engine stage
  already found missing and extra keys; this pass reports keys whose streamed
  columns differ.
* ``keyed`` — keyed table compared entirely here (its key could not be encoded
  in-engine): missing, extra, duplicate and changed keys.
* ``multiset`` — keyless table with a streamed column: the multiset of row
  fingerprints ("Content match (no key)").
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from services.parity_scale.dialects.base import Side
from services.parity_scale.judge import fingerprint, wire_text
from services.parity_scale.plan import ColumnPlan, TablePlan
from services.parity_scale.store import encode_key


def row_fingerprint_hash(values: Sequence[Any], columns: Sequence[ColumnPlan], dest_engine: str, *, source: bool,
                         number_locale: str = "", date_locale: str = "") -> str:
    """SHA-256 over the length-prefixed Gate-8 fingerprints of ``values`` (source cells under the
    run's locales)."""
    digest = hashlib.sha256()
    loc = {"number_locale": number_locale, "date_locale": date_locale} if source else {}
    for value, col in zip(values, columns, strict=True):
        cell = fingerprint(value, col, dest_engine, source=source, **loc).encode("utf-8", "surrogatepass")
        digest.update(str(len(cell)).encode("ascii") + b":" + cell)
    return digest.hexdigest()


@dataclass
class StreamResult:
    shape: str
    rows_source: int = 0
    rows_destination: int = 0
    missing: int = 0
    extra: int = 0
    changed: int = 0
    duplicate: int = 0
    missing_keys: list[list[Any]] = field(default_factory=list)
    extra_keys: list[list[Any]] = field(default_factory=list)
    changed_keys: list[list[Any]] = field(default_factory=list)
    duplicate_keys: list[list[Any]] = field(default_factory=list)
    missing_examples: list[list[str | None]] = field(default_factory=list)
    extra_examples: list[list[str | None]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)


def _spill(conn: sqlite3.Connection, table: str, side: Side, columns: Sequence[ColumnPlan], *,
           keyed: bool, keep_values: bool, batch: int, lo: Any = None, hi: Any = None,
           deadline: float | None = None, tick: Any = None, nulls: bool = False) -> int:
    import time

    from services.parity_scale.dialects.base import UnitTimeout

    plan = side.plan
    count = 0
    pending: list[tuple[str | None, str, str | None, str | None]] = []
    extra: dict[str, Any] = {"nulls": True} if nulls else {}
    rows = side.iter_stream(columns, batch=batch, lo=lo, hi=hi, **extra) if (lo is not None or hi is not None) \
        else side.iter_stream(columns, batch=batch, **extra)
    for key, values in rows:
        h = row_fingerprint_hash(values, columns, plan.dest_engine, source=side.is_source,
                                 number_locale=plan.run_number_locale, date_locale=plan.run_date_locale)
        ident = side.key_hex(key) if keyed and key is not None else None
        key_json = json.dumps(encode_key(key)) if keyed and key is not None else None
        vals = json.dumps([wire_text(v) for v in values]) if keep_values else None
        pending.append((ident, h, key_json, vals))
        count += 1
        if len(pending) >= batch:
            conn.executemany(f"INSERT INTO {table} VALUES (?, ?, ?, ?)", pending)
            pending.clear()
            if tick is not None:
                tick()
            if deadline is not None and time.monotonic() > deadline:
                raise UnitTimeout("the scan's wall-clock budget ran out in the streamed pass")
    if pending:
        conn.executemany(f"INSERT INTO {table} VALUES (?, ?, ?, ?)", pending)
    conn.commit()
    return count


def stream_compare(
    source: Side,
    destination: Side,
    plan: TablePlan,
    *,
    shape: str,
    columns: Sequence[ColumnPlan] | None = None,
    keys_cap: int = 10_000,
    examples_cap: int = 20,
    batch: int = 5000,
    spill_dir: str | None = None,
    lo: Any = None,
    hi: Any = None,
    deadline: float | None = None,
    tick: Any = None,
    nulls: bool = False,
) -> StreamResult:
    """Compare the sides' streamed fingerprints (``nulls``: only the rows with no key value,
    as a multiset of whole rows, since no key pairs them)."""
    if shape not in {"changed", "keyed", "multiset"}:
        raise ValueError(f"unknown stream shape {shape!r}")
    cols = list(columns if columns is not None else plan.compared_columns)
    keyed = shape != "multiset"
    result = StreamResult(shape=shape)
    fd, path = tempfile.mkstemp(prefix="psd1-stream-", suffix=".sqlite", dir=spill_dir)
    os.close(fd)
    conn = sqlite3.connect(path)
    if tick is not None or deadline is not None:
        # The spill's own work (inserts, CREATE INDEX, the merge) can run for minutes on a large
        # table: it is progress for the worker's heartbeat, and it stops at the wall-clock budget.
        import time

        def progress() -> int:
            if tick is not None:
                tick()
            return 1 if deadline is not None and time.monotonic() > deadline else 0

        conn.set_progress_handler(progress, 20_000)
    try:
        conn.execute("PRAGMA journal_mode=OFF")
        conn.execute("PRAGMA synchronous=OFF")
        conn.execute("CREATE TABLE s (k TEXT, h TEXT NOT NULL, key TEXT, vals TEXT)")
        conn.execute("CREATE TABLE d (k TEXT, h TEXT NOT NULL, key TEXT, vals TEXT)")
        bounds = {"lo": lo, "hi": hi, "deadline": deadline, "tick": tick, "nulls": nulls}
        result.rows_source = _spill(conn, "s", source, cols, keyed=keyed, keep_values=not keyed, batch=batch,
                                    **bounds)
        result.rows_destination = _spill(conn, "d", destination, cols, keyed=keyed, keep_values=not keyed,
                                         batch=batch, **bounds)
        try:
            if keyed:
                conn.execute("CREATE INDEX s_k ON s(k)")
                conn.execute("CREATE INDEX d_k ON d(k)")
                _merge_keyed(conn, result, full=shape == "keyed", keys_cap=keys_cap)
            else:
                _merge_multiset(conn, result, examples_cap=examples_cap)
        except sqlite3.OperationalError as exc:
            if "interrupt" in str(exc).lower():
                from services.parity_scale.dialects.base import UnitTimeout

                raise UnitTimeout("the scan's wall-clock budget ran out while merging the streamed pass") from exc
            raise
    finally:
        conn.close()
        try:
            os.unlink(path)
        except OSError:
            pass
    return result


def _merge_keyed(conn: sqlite3.Connection, result: StreamResult, *, full: bool, keys_cap: int) -> None:
    changed_sql = (
        "SELECT s.key FROM s JOIN (SELECT k, min(h) AS h, max(h) AS h2 FROM d GROUP BY k) dd ON dd.k = s.k "
        "WHERE s.h <> dd.h OR s.h <> dd.h2"
    )
    for (key_json,) in conn.execute(changed_sql):
        result.changed += 1
        if len(result.changed_keys) < keys_cap:
            result.changed_keys.append(json.loads(key_json))
    if not full:
        return
    for (key_json,) in conn.execute("SELECT key FROM s WHERE k NOT IN (SELECT k FROM d)"):
        result.missing += 1
        if len(result.missing_keys) < keys_cap:
            result.missing_keys.append(json.loads(key_json))
    for (key_json,) in conn.execute("SELECT key FROM d WHERE k NOT IN (SELECT k FROM s)"):
        result.extra += 1
        if len(result.extra_keys) < keys_cap:
            result.extra_keys.append(json.loads(key_json))
    for key_json, n in conn.execute("SELECT min(key), count(*) FROM d GROUP BY k HAVING count(*) > 1"):
        result.duplicate += int(n) - 1
        if len(result.duplicate_keys) < keys_cap:
            result.duplicate_keys.append(json.loads(key_json))


def _merge_multiset(conn: sqlite3.Connection, result: StreamResult, *, examples_cap: int) -> None:
    sql = (
        "SELECT h, sum(sc), sum(dc), min(vals) FROM ("
        "SELECT h, 1 AS sc, 0 AS dc, vals FROM s UNION ALL SELECT h, 0, 1, vals FROM d"
        ") GROUP BY h HAVING sum(sc) <> sum(dc)"
    )
    for _h, sc, dc, vals in conn.execute(sql):
        delta = int(sc) - int(dc)
        example = json.loads(vals) if vals else []
        if delta > 0:
            result.missing += delta
            if len(result.missing_examples) < examples_cap:
                result.missing_examples.append(example)
        else:
            result.extra += -delta
            if len(result.extra_examples) < examples_cap:
                result.extra_examples.append(example)
