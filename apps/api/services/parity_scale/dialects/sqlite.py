"""SQLite dialect: the reference engine.

SQLite has no SHA-256 and its 64-bit ``SUM`` would overflow, so CE1, the row
hash and the exact sums run as Python UDFs that call
:mod:`services.parity_scale.ce1` directly. The driver logic above it is the
same as on every other engine, which makes SQLite the unit-test engine for the
driver; it says nothing about another engine's SQL rendering.
"""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Iterator, Sequence
from typing import Any

from services.parity_scale import ce1
from services.parity_scale.dialects.base import Digest, KeyBound, LeafRow, Side, UnitTimeout, range_predicate
from services.parity_scale.plan import ColumnPlan, TablePlan


def _quote(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


class _ExactDigest:
    """Aggregate ``psd1_agg(c1..cn)``: count and exact Σa, Σb as ``'n:sa:sb'``."""

    encoder: Any = None

    def __init__(self) -> None:
        self.n = 0
        self.sa = 0
        self.sb = 0

    def step(self, *values: Any) -> None:
        row = self.encoder(values)
        if row is None:
            return
        _h, a, b = row
        self.n += 1
        self.sa += a
        self.sb += b

    def finalize(self) -> str:
        return f"{self.n}:{self.sa}:{self.sb}"


class SqliteSide(Side):
    def __init__(
        self,
        *,
        role: str,
        plan: TablePlan,
        table: str,
        schema: str = "",
        path: str | None = None,
        connection: sqlite3.Connection | None = None,
    ) -> None:
        super().__init__(role=role, plan=plan, table=table, schema=schema)
        if connection is None:
            if not path:
                raise ValueError("SqliteSide needs a path or a connection")
            # The driver may run this side's statements on a worker thread
            # (one at a time); the connection is never shared between sides.
            connection = sqlite3.connect(path, check_same_thread=False)
        self.conn = connection
        self._deadline: float | None = None
        self.engine_cols = list(plan.engine_columns)
        self.compared_cols = list(plan.compared_columns)
        self.key_cols = list(plan.key_plans()) if plan.key else []
        dest_engine = plan.dest_engine
        self._row_specs = [c.field_spec(dest_engine) for c in self.engine_cols]
        self._key_specs = [c.field_spec(dest_engine) for c in self.key_cols]
        self._register()

    # UDFs ----------------------------------------------------------------------
    def _encode_row(self, values: Sequence[Any]) -> tuple[bytes, int, int] | None:
        row = ce1.encode_row(list(values), self._row_specs, source=self.is_source)
        if row is ce1.HELD:
            return None
        return ce1.hash_words(row)  # type: ignore[arg-type]

    def _encode_key(self, values: Sequence[Any]) -> bytes:
        row = ce1.encode_row(list(values), self._key_specs, source=self.is_source)
        if row is ce1.HELD:  # pragma: no cover - plan refuses lossy keys
            raise ValueError("a key column cannot be held out")
        return row  # type: ignore[return-value]

    def _register(self) -> None:
        conn = self.conn
        encode_row = self._encode_row
        encode_key = self._encode_key

        def rowwords(*values: Any) -> str | None:
            row = encode_row(values)
            if row is None:
                return None
            h, a, b = row
            return f"{h.hex()}:{a}:{b}"

        def held(*values: Any) -> int:
            return 1 if encode_row(values) is None else 0

        def keyhex(*values: Any) -> str:
            return encode_key(values).hex()

        def kbucket(level: int, *values: Any) -> int:
            return ce1.key_bucket(ce1.row_hash(encode_key(values)), int(level))

        def rbucket(level: int, *values: Any) -> int | None:
            row = encode_row(values)
            if row is None:
                return None
            return ce1.row_bucket(row[0], int(level))

        agg = type("_SideDigest", (_ExactDigest,), {"encoder": staticmethod(encode_row)})
        conn.create_function("psd1_rowwords", -1, rowwords, deterministic=True)
        conn.create_function("psd1_held", -1, held, deterministic=True)
        conn.create_function("psd1_keyhex", -1, keyhex, deterministic=True)
        conn.create_function("psd1_kbucket", -1, kbucket, deterministic=True)
        conn.create_function("psd1_rbucket", -1, rbucket, deterministic=True)
        conn.create_aggregate("psd1_agg", -1, agg)

    # helpers -------------------------------------------------------------------
    @property
    def _table_ref(self) -> str:
        if self.schema:
            return f"{_quote(self.schema)}.{_quote(self.table)}"
        return _quote(self.table)

    def _cols(self, cols: Sequence[ColumnPlan]) -> str:
        return ", ".join(_quote(self.column_name(c)) for c in cols)

    @property
    def _row_args(self) -> str:
        return self._cols(self.engine_cols)

    @property
    def _key_args(self) -> str:
        return self._cols(self.key_cols)

    def _range(self, lo: KeyBound, hi: KeyBound) -> tuple[str, list[Any]]:
        if not self.key_cols:
            return "1", []
        quoted = [_quote(self.column_name(c)) for c in self.key_cols]
        sql, params = range_predicate(quoted, lo, hi, "?")
        return ("1" if sql == "TRUE" else sql), params

    def _progress(self) -> int:
        if self._deadline is not None and time.monotonic() > self._deadline:
            return 1
        return 0

    def _run(self, sql: str, params: Sequence[Any] = ()) -> list[tuple[Any, ...]]:
        if self.timeout_seconds:
            self._deadline = time.monotonic() + float(self.timeout_seconds)
            self.conn.set_progress_handler(self._progress, 1000)
        try:
            return self.conn.execute(sql, list(params)).fetchall()
        except sqlite3.OperationalError as exc:
            if "interrupt" in str(exc).lower():
                raise UnitTimeout(str(exc)) from exc
            raise
        finally:
            if self.timeout_seconds:
                self.conn.set_progress_handler(None, 0)
                self._deadline = None

    @staticmethod
    def _digest(text: str | None) -> Digest:
        n, sa, sb = (text or "0:0:0").split(":")
        return int(n), int(sa), int(sb)

    def _bucket_path_filter(
        self, parents: Sequence[tuple[int, ...]], params: list[Any]
    ) -> str:
        if not parents or not parents[0]:
            return "1"
        depth = len(parents[0])
        fn = "psd1_kbucket" if self.key_cols else "psd1_rbucket"
        args = self._key_args if self.key_cols else self._row_args
        exprs = [f"{fn}({lvl}, {args})" for lvl in range(depth)]
        lhs = f"({', '.join(exprs)})" if depth > 1 else exprs[0]
        rows = []
        for path in parents:
            params.extend(path)
            marks = ", ".join("?" for _ in path)
            rows.append(f"({marks})" if depth > 1 else marks)
        return f"{lhs} IN ({', '.join(rows)})"

    # operations ----------------------------------------------------------------
    def split_points(self, lo: KeyBound, hi: KeyBound, step: int) -> list[tuple[Any, ...]]:
        where, params = self._range(lo, hi)
        keys = self._key_args
        sql = (
            f"SELECT {keys} FROM (SELECT {keys}, row_number() OVER (ORDER BY {keys}) AS rn "
            f"FROM {self._table_ref} WHERE {where}) WHERE rn > 1 AND (rn - 1) % ? = 0 ORDER BY {keys}"
        )
        return [tuple(r) for r in self._run(sql, [*params, max(1, int(step))])]

    def range_digest(self, lo: KeyBound, hi: KeyBound) -> Digest:
        where, params = self._range(lo, hi)
        sql = f"SELECT psd1_agg({self._row_args}) FROM {self._table_ref} WHERE {where}"
        return self._digest(self._run(sql, params)[0][0])

    def range_leaf(self, lo: KeyBound, hi: KeyBound) -> list[LeafRow]:
        where, params = self._range(lo, hi)
        sql = (
            f"SELECT {self._key_args}, psd1_keyhex({self._key_args}), psd1_rowwords({self._row_args}) "
            f"FROM {self._table_ref} WHERE {where}"
        )
        return self._leaf_rows(self._run(sql, params))

    def _leaf_rows(self, rows: list[tuple[Any, ...]]) -> list[LeafRow]:
        out: list[LeafRow] = []
        width = len(self.key_cols)
        for row in rows:
            words = row[-1]
            if words is None:
                continue  # held out by the write contract
            h, a, b = words.split(":")
            if width:
                out.append(LeafRow(tuple(row[:width]), row[width], int(a), int(b)))
            else:
                out.append(LeafRow(None, h, int(a), int(b)))
        return out

    def bucket_digests(
        self, level: int, parents: Sequence[tuple[int, ...]], lo: KeyBound = None, hi: KeyBound = None
    ) -> dict[tuple[int, ...], Digest]:
        where, params = self._range(lo, hi)
        path_params: list[Any] = []
        path_filter = self._bucket_path_filter(parents, path_params)
        fn = "psd1_kbucket" if self.key_cols else "psd1_rbucket"
        args = self._key_args if self.key_cols else self._row_args
        groups = [f"{fn}({lvl}, {args})" for lvl in range(level + 1)]
        sql = (
            f"SELECT {', '.join(groups)}, psd1_agg({self._row_args}) FROM {self._table_ref} "
            f"WHERE {where} AND {path_filter} GROUP BY {', '.join(f'{i + 1}' for i in range(level + 1))}"
        )
        out: dict[tuple[int, ...], Digest] = {}
        for row in self._run(sql, [*params, *path_params]):
            path = tuple(row[: level + 1])
            if path[-1] is None:
                continue  # keyless row held out entirely
            digest = self._digest(row[-1])
            if digest[0]:
                out[path] = digest
        return out

    def bucket_leaf(
        self, parents: Sequence[tuple[int, ...]], lo: KeyBound = None, hi: KeyBound = None
    ) -> list[LeafRow]:
        where, params = self._range(lo, hi)
        path_params: list[Any] = []
        path_filter = self._bucket_path_filter(parents, path_params)
        if self.key_cols:
            sql = (
                f"SELECT {self._key_args}, psd1_keyhex({self._key_args}), psd1_rowwords({self._row_args}) "
                f"FROM {self._table_ref} WHERE {where} AND {path_filter}"
            )
        else:
            sql = (
                f"SELECT psd1_rowwords({self._row_args}) FROM {self._table_ref} "
                f"WHERE {where} AND {path_filter}"
            )
        return self._leaf_rows(self._run(sql, [*params, *path_params]))

    def fetch_rows(self, keys: Sequence[tuple[Any, ...]]) -> dict[str, list[tuple[Any, ...]]]:
        out: dict[str, list[tuple[Any, ...]]] = {}
        if not keys:
            return out
        quoted = [_quote(self.column_name(c)) for c in self.key_cols]
        lhs = f"({', '.join(quoted)})" if len(quoted) > 1 else quoted[0]
        width = len(quoted)
        for start in range(0, len(keys), 500):
            page = keys[start : start + 500]
            params: list[Any] = []
            rows = []
            for key in page:
                params.extend(key)
                marks = ", ".join("?" for _ in key)
                rows.append(f"({marks})" if width > 1 else marks)
            sql = (
                f"SELECT {self._key_args}, {self._cols(self.compared_cols)} FROM {self._table_ref} "
                f"WHERE {lhs} IN ({', '.join(rows)})"
            )
            for row in self._run(sql, params):
                out.setdefault(self.key_hex(row[:width]), []).append(tuple(row[width:]))
        return out

    def keyless_examples(self, row_hashes: Sequence[str], limit: int) -> list[tuple[str, tuple[Any, ...]]]:
        wanted = set(row_hashes)
        if not wanted:
            return []
        out: list[tuple[str, tuple[Any, ...]]] = []
        sql = f"SELECT psd1_rowwords({self._row_args}), {self._cols(self.compared_cols)} FROM {self._table_ref}"
        cur = self.conn.execute(sql)
        for row in cur:
            words = row[0]
            if words is None:
                continue
            h = words.split(":", 1)[0]
            if h in wanted:
                out.append((h, tuple(row[1:])))
                if len(out) >= limit:
                    break
        cur.close()
        return out

    def iter_stream(
        self, columns: Sequence[ColumnPlan], batch: int = 5000
    ) -> Iterator[tuple[tuple[Any, ...] | None, tuple[Any, ...]]]:
        width = len(self.key_cols)
        select = ", ".join(x for x in (self._key_args, self._cols(columns)) if x)
        held = f" WHERE NOT psd1_held({self._row_args})" if self.is_source and self.engine_cols else ""
        cur = self.conn.execute(f"SELECT {select} FROM {self._table_ref}{held}")
        try:
            while True:
                rows = cur.fetchmany(batch)
                if not rows:
                    return
                for row in rows:
                    yield (tuple(row[:width]) if width else None), tuple(row[width:])
        finally:
            cur.close()

    def held_count(self) -> int:
        if not self.is_source or not any(c.decimal_fit for c in self.engine_cols):
            return 0
        sql = f"SELECT count(*) FROM {self._table_ref} WHERE psd1_held({self._row_args})"
        return int(self._run(sql)[0][0])

    def close(self) -> None:
        try:
            self.conn.close()
        except sqlite3.Error:  # pragma: no cover - closing a closed handle
            pass
