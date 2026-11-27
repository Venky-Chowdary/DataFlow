"""The SQL every dialect shares: one shape, engine-specific spellings.

Each side reduces a row in one derived table::

    SELECT k0…, kx (hex of the key's CE1 bytes), kh (hex SHA-256 of them),
           h (hex SHA-256 of the row's CE1 bytes)
    FROM <table> WHERE <key range> AND NOT <held out by the write contract>

and every operation (range digest, leaf, bucket digests, bucket leaf) is a
query over that derived table. A dialect supplies the spellings: CE1 fields
(:meth:`SqlSide.field`), UTF-8 + SHA-256 to hex, the two 60-bit words from
hex, a 10-bit bucket from a hex window, exact sums, range predicates, a
statement timeout, and a streaming cursor.

The bucket of level ``i`` is read from a 4-hex-character window of the key
hash (or of the row hash's third word, for a keyless table): it starts at hex
character ``⌊10i/4⌋`` and the 10 bits sit ``10i mod 4`` bits into it, which is
exactly :func:`services.parity_scale.ce1.key_bucket` / ``row_bucket``.
"""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Iterator, Sequence
from typing import Any

from services.parity_scale.dialects.base import Digest, KeyBound, LeafRow, Side, UnitTimeout
from services.parity_scale.plan import ColumnPlan, TablePlan

#: Engines whose timestamp storage is PostgreSQL's (microseconds since 2000-01-01).
PG_ENGINES = frozenset({"postgresql", "postgres", "pg", "timescaledb", "alloydb", "supabase", "pgvector"})
#: 2000-01-01 00:00 UTC in microseconds since 1970: PostgreSQL's timestamp epoch.
PG_EPOCH_US = 946_684_800_000_000


class SqlSide(Side):
    """A side on a SQL engine. Subclasses supply the spellings."""

    #: Extra text that keeps the engine from inlining the derived table (PG).
    materialize = ""
    #: Rows per ``fetch_rows`` statement (parameter limits differ).
    fetch_page = 1000

    def __init__(
        self,
        *,
        role: str,
        plan: TablePlan,
        table: str,
        schema: str = "",
        connection: Any = None,
        connect: Any = None,
    ) -> None:
        super().__init__(role=role, plan=plan, table=table, schema=schema)
        if connection is None:
            if connect is None:
                raise ValueError(f"{type(self).__name__} needs a connection or a connect() factory")
            connection = connect()
        self.conn = connection
        self.engine_cols = list(plan.engine_columns)
        self.compared_cols = list(plan.compared_columns)
        self.key_cols = list(plan.key_plans()) if plan.key else []
        self.prepare_session()
        self._row_fields = [self.field(c, self.column_name(c)) for c in self.engine_cols]
        self._key_fields = [self.field(c, self.column_name(c)) for c in self.key_cols]
        self._held = self.held_predicate()

    # ── spellings a dialect supplies ─────────────────────────────────────────
    @abstractmethod
    def prepare_session(self) -> None: ...

    @abstractmethod
    def quote(self, name: str) -> str: ...

    @property
    @abstractmethod
    def table_ref(self) -> str: ...

    @abstractmethod
    def field(self, col: ColumnPlan, column: str) -> str:
        """The CE1 field of ``column`` as a text expression (NULL when held out)."""

    @abstractmethod
    def concat(self, parts: Sequence[str]) -> str: ...

    @abstractmethod
    def sha256_hex(self, text: str) -> str:
        """64 lowercase hex characters of SHA-256 over the UTF-8 bytes of ``text``."""

    @abstractmethod
    def utf8_hex(self, text: str) -> str:
        """Lowercase hex of the UTF-8 bytes of ``text``."""

    @abstractmethod
    def word(self, h: str, which: str) -> str:
        """The 60-bit word ``a`` (hex chars 2–16) or ``b`` (18–32) of a hex digest."""

    @abstractmethod
    def hex4_int(self, hex4: str) -> str:
        """An integer from 4 hex characters."""

    @abstractmethod
    def substr(self, text: str, start: int, length: int) -> str: ...

    @abstractmethod
    def exact_sum(self, expr: str) -> str: ...

    @abstractmethod
    def idiv(self, a: str, b: int) -> str: ...

    @abstractmethod
    def mod(self, a: str, b: str) -> str: ...

    @abstractmethod
    def held_predicate(self) -> str: ...

    @abstractmethod
    def execute(self, sql: str, params: Sequence[Any]) -> list[tuple[Any, ...]]:
        """Run one read-only statement under the side's timeout; raise UnitTimeout."""

    @abstractmethod
    def stream(self, sql: str, params: Sequence[Any], batch: int) -> Iterator[tuple[Any, ...]]: ...

    def limit(self, sql_without_limit: str, n: int) -> tuple[str, list[Any]]:
        return f"{sql_without_limit} LIMIT %s", [int(n)]

    def row_values(self) -> bool:
        """Whether ``(a, b) >= (x, y)`` and ``(a, b) IN ((…))`` are supported."""
        return True

    # ── shared SQL ───────────────────────────────────────────────────────────
    def quoted_keys(self) -> list[str]:
        return [self.quote(self.column_name(c)) for c in self.key_cols]

    def range_predicate(self, lo: KeyBound, hi: KeyBound) -> tuple[str, list[Any]]:
        keys = self.quoted_keys()
        clauses: list[str] = []
        params: list[Any] = []
        for bound, inclusive, op in ((lo, True, ">"), (hi, False, "<")):
            if bound is None:
                continue
            values = list(bound)
            if self.row_values() or len(keys) == 1:
                lhs = f"({', '.join(keys)})" if len(keys) > 1 else keys[0]
                marks = ", ".join("%s" for _ in values)
                rhs = f"({marks})" if len(values) > 1 else marks
                clauses.append(f"{lhs} {op}{'=' if inclusive else ''} {rhs}")
                params.extend(values)
                continue
            # Lexicographic expansion (SQL Server has no row values), as in
            # services.keyset_pagination.keyset_successor_predicate.
            ors = []
            for i, key in enumerate(keys):
                eqs = [f"{keys[j]} = %s" for j in range(i)]
                last = i == len(keys) - 1
                cmp = f"{key} {op}{'=' if (inclusive and last) else ''} %s"
                ors.append("(" + " AND ".join([*eqs, cmp]) + ")")
                params.extend(values[: i + 1])
            clauses.append("(" + " OR ".join(ors) + ")")
        return (" AND ".join(clauses) or "1 = 1"), params

    def where(self, lo: KeyBound, hi: KeyBound) -> tuple[str, list[Any]]:
        sql, params = self.range_predicate(lo, hi) if self.key_cols else ("1 = 1", [])
        if self._held:
            sql = f"{sql} AND NOT ({self._held})"
        return sql, params

    def hashed(self, lo: KeyBound, hi: KeyBound, *, extra_cols: Sequence[str] = ()) -> tuple[str, list[Any]]:
        where, params = self.where(lo, hi)
        cols = []
        if self.key_cols:
            key_text = self.concat(self._key_fields)
            cols += [f"{q} AS k{i}" for i, q in enumerate(self.quoted_keys())]
            cols += [f"{self.utf8_hex(key_text)} AS kx", f"{self.sha256_hex(key_text)} AS kh"]
        cols.append(f"{self.sha256_hex(self.concat(self._row_fields))} AS h")
        cols += list(extra_cols)
        sql = f"SELECT {', '.join(cols)} FROM {self.table_ref} WHERE {where}{self.materialize}"
        return sql, params

    def bucket(self, level: int) -> str:
        hex_col, offset = ("kh", 0) if self.key_cols else ("h", 32)
        start = offset + (10 * level) // 4
        shift = 6 - (10 * level) % 4
        window = self.hex4_int(self.substr(hex_col, start + 1, 4))
        return self.mod(self.idiv(window, 2**shift), "1024")

    def _bucketed(self, level: int, inner: str) -> str:
        buckets = ", ".join(f"{self.bucket(lvl)} AS b{lvl}" for lvl in range(level + 1))
        return f"SELECT r.*, {buckets} FROM ({inner}) r"

    def _path_filter(self, parents: Sequence[tuple[int, ...]], params: list[Any]) -> str:
        if not parents or not parents[0]:
            return ""
        ors = []
        for path in parents:
            ors.append("(" + " AND ".join(f"b{i} = %s" for i in range(len(path))) + ")")
            params.extend(int(p) for p in path)
        return " WHERE " + " OR ".join(ors)

    def _a_b(self) -> str:
        return f"{self.word('h', 'a')} AS a, {self.word('h', 'b')} AS b"

    # ── level-0 boundaries for large tables ──────────────────────────────────
    def estimate_rows(self) -> int | None:
        """The engine's own row estimate (catalog statistics), or ``None``."""
        return None

    def sample_keys(self, fraction: float, limit: int) -> list[tuple[Any, ...]] | None:
        """Ordered keys from a block sample of about ``fraction`` of the table, or None."""
        return None

    def boundaries(self, segment_rows: int, *, strategy: str = "auto",
                   large_table_rows: int = 2_000_000) -> tuple[list[tuple[Any, ...]], str]:
        """Quantiles on small tables; on large ones a linear split of a single
        integer key, else boundaries from a block sample of the keys.

        Any boundaries are correct — they only partition the key space and the
        first and last units are unbounded; quality only changes how evenly
        rows fall into units (a heavy unit is split again by measured count,
        and a slow one halved on timeout).
        """
        segment_rows = max(1, int(segment_rows))
        estimate = self.estimate_rows() if strategy != "quantile" else None
        large = strategy in {"linear", "sample"} or (estimate is not None and estimate >= large_table_rows)
        if strategy == "quantile" or not large:
            return self.split_points(None, None, segment_rows), "quantile"
        estimate = max(int(estimate or 0), 1)
        units = max(1, -(-estimate // segment_rows))
        single_int = len(self.key_cols) == 1 and self.key_cols[0].kind == "integer"
        if single_int and strategy in {"auto", "linear"}:
            key = self.quoted_keys()[0]
            bounds_sql = f"SELECT MIN({key}), MAX({key}) FROM {self.table_ref}"
            low, high = self.execute(bounds_sql, [])[0]
            if low is None:
                return [], "linear"
            step = max(1, -(-(int(high) - int(low) + 1) // units))
            points = [(int(low) + i * step,) for i in range(1, units) if int(low) + i * step <= int(high)]
            return points, "linear"
        if strategy in {"auto", "sample"}:
            per_unit = 50
            fraction = min(1.0, units * per_unit / estimate)
            keys = self.sample_keys(fraction, units * per_unit * 4)
            if keys:
                pick = max(1, len(keys) // units)
                points = []
                for i in range(pick, len(keys), pick):
                    if not points or tuple(keys[i]) > points[-1]:
                        points.append(tuple(keys[i]))
                return points, "sample"
        return self.split_points(None, None, segment_rows), "quantile"

    # ── operations ──────────────────────────────────────────────────────────
    def split_points(self, lo: KeyBound, hi: KeyBound, step: int) -> list[tuple[Any, ...]]:
        keys = ", ".join(self.quoted_keys())
        where, params = self.where(lo, hi)
        rn = f"ROW_NUMBER() OVER (ORDER BY {keys})"
        sql = (
            f"SELECT {keys} FROM (SELECT {keys}, {rn} AS rn FROM {self.table_ref} WHERE {where}) s "
            f"WHERE rn > 1 AND {self.mod('rn - 1', str(max(1, int(step))))} = 0 ORDER BY {keys}"
        )
        return [tuple(r) for r in self.execute(sql, params)]

    def range_digest(self, lo: KeyBound, hi: KeyBound) -> Digest:
        inner, params = self.hashed(lo, hi)
        sql = (
            f"SELECT COUNT(*), {self.exact_sum('a')}, {self.exact_sum('b')} "
            f"FROM (SELECT {self._a_b()} FROM ({inner}) r) w"
        )
        row = self.execute(sql, params)[0]
        return int(row[0] or 0), int(row[1] or 0), int(row[2] or 0)

    def _leaf_select(self, source: str) -> str:
        if self.key_cols:
            ks = ", ".join(f"k{i}" for i in range(len(self.key_cols)))
            return f"SELECT {ks}, kx, h, {self._a_b()} FROM ({source}) l"
        return f"SELECT h, {self._a_b()} FROM ({source}) l"

    def _leaf_rows(self, rows: list[tuple[Any, ...]]) -> list[LeafRow]:
        width = len(self.key_cols)
        out = []
        for row in rows:
            if width:
                out.append(LeafRow(tuple(row[:width]), str(row[width]).lower(), int(row[width + 2]),
                                   int(row[width + 3])))
            else:
                out.append(LeafRow(None, str(row[0]).lower(), int(row[1]), int(row[2])))
        return out

    def range_leaf(self, lo: KeyBound, hi: KeyBound) -> list[LeafRow]:
        inner, params = self.hashed(lo, hi)
        return self._leaf_rows(self.execute(self._leaf_select(inner), params))

    def bucket_digests(
        self, level: int, parents: Sequence[tuple[int, ...]], lo: KeyBound = None, hi: KeyBound = None
    ) -> dict[tuple[int, ...], Digest]:
        inner, params = self.hashed(lo, hi)
        path_params: list[Any] = []
        path = self._path_filter(parents, path_params)
        groups = ", ".join(f"b{lvl}" for lvl in range(level + 1))
        sql = (
            f"SELECT {groups}, COUNT(*), {self.exact_sum('a')}, {self.exact_sum('b')} FROM ("
            f"SELECT q.*, {self._a_b()} FROM ({self._bucketed(level, inner)}) q{path}) w GROUP BY {groups}"
        )
        out: dict[tuple[int, ...], Digest] = {}
        for row in self.execute(sql, [*params, *path_params]):
            out[tuple(int(v) for v in row[: level + 1])] = (
                int(row[level + 1] or 0), int(row[level + 2] or 0), int(row[level + 3] or 0))
        return out

    def bucket_leaf(
        self, parents: Sequence[tuple[int, ...]], lo: KeyBound = None, hi: KeyBound = None
    ) -> list[LeafRow]:
        inner, params = self.hashed(lo, hi)
        path_params: list[Any] = []
        depth = len(parents[0]) if parents and parents[0] else 0
        if depth:
            path = self._path_filter(parents, path_params)
            source = f"SELECT * FROM ({self._bucketed(depth - 1, inner)}) q{path}"
        else:
            source = inner
        return self._leaf_rows(self.execute(self._leaf_select(source), [*params, *path_params]))

    def _key_in(self, keys: Sequence[tuple[Any, ...]]) -> tuple[str, list[Any]]:
        quoted = self.quoted_keys()
        params: list[Any] = []
        if self.row_values() or len(quoted) == 1:
            lhs = f"({', '.join(quoted)})" if len(quoted) > 1 else quoted[0]
            rows = []
            for key in keys:
                params.extend(key)
                marks = ", ".join("%s" for _ in key)
                rows.append(f"({marks})" if len(quoted) > 1 else marks)
            return f"{lhs} IN ({', '.join(rows)})", params
        ors = []
        for key in keys:
            ors.append("(" + " AND ".join(f"{q} = %s" for q in quoted) + ")")
            params.extend(key)
        return "(" + " OR ".join(ors) + ")", params

    def fetch_rows(self, keys: Sequence[tuple[Any, ...]]) -> dict[str, list[tuple[Any, ...]]]:
        out: dict[str, list[tuple[Any, ...]]] = {}
        if not keys:
            return out
        width = len(self.key_cols)
        cols = ", ".join(self.quote(self.column_name(c)) for c in self.compared_cols)
        page = max(1, self.fetch_page // max(1, width))
        for start in range(0, len(keys), page):
            pred, params = self._key_in(keys[start : start + page])
            sql = f"SELECT {', '.join(self.quoted_keys())}, {cols} FROM {self.table_ref} WHERE {pred}"
            for row in self.execute(sql, params):
                out.setdefault(self.key_hex(row[:width]), []).append(tuple(row[width:]))
        return out

    def keyless_examples(self, row_hashes: Sequence[str], limit: int) -> list[tuple[str, tuple[Any, ...]]]:
        if not row_hashes:
            return []
        cols = [self.quote(self.column_name(c)) for c in self.compared_cols]
        inner, params = self.hashed(None, None, extra_cols=[f"{c} AS c{i}" for i, c in enumerate(cols)])
        marks = ", ".join("%s" for _ in row_hashes)
        names = ", ".join(f"c{i}" for i in range(len(cols)))
        sql, lim = self.limit(f"SELECT h, {names} FROM ({inner}) s WHERE h IN ({marks})", limit)
        rows = self.execute(sql, [*params, *[h.lower() for h in row_hashes], *lim])
        return [(str(r[0]).lower(), tuple(r[1:])) for r in rows]

    range_stream = True

    def iter_stream(
        self, columns: Sequence[ColumnPlan], batch: int = 5000, lo: KeyBound = None, hi: KeyBound = None
    ) -> Iterator[tuple[tuple[Any, ...] | None, tuple[Any, ...]]]:
        width = len(self.key_cols)
        names = [*self.quoted_keys(), *(self.quote(self.column_name(c)) for c in columns)]
        where, params = self.where(lo, hi)
        sql = f"SELECT {', '.join(names)} FROM {self.table_ref} WHERE {where}"
        for row in self.stream(sql, params, batch):
            yield (tuple(row[:width]) if width else None), tuple(row[width:])

    def held_count(self) -> int:
        if not self._held:
            return 0
        sql = f"SELECT COUNT(*) FROM {self.table_ref} WHERE {self._held}"
        return int(self.execute(sql, [])[0][0])

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:  # noqa: BLE001 — closing a broken connection
            return None

    # ── helpers for dialect field builders ──────────────────────────────────
    @property
    def dest_is_pg(self) -> bool:
        return str(self.plan.dest_engine or "").lower() in PG_ENGINES


def fit_held_predicate(side: SqlSide, classes_for) -> str:
    """``OR`` of every decimal-fit column's held-out condition (source side only)."""
    if not side.is_source:
        return ""
    parts = []
    for col in side.engine_cols:
        fit = col.decimal_fit
        if not fit:
            continue
        x = side.quote(side.column_name(col))
        whens = " ".join(
            f"WHEN {cond} THEN 1"
            for name, cond in classes_for(x, fit)
            if fit["actions"].get(name, "hold") == "hold"
        )
        if whens:
            parts.append(f"({x} IS NOT NULL AND (CASE {whens} ELSE 0 END) = 1)")
    return " OR ".join(parts)


def fit_field(x: str, body: str, fit: dict[str, Any], classes_for) -> str:
    """A decimal field under a fit contract: keep, ``N`` (written NULL) or NULL (held out)."""
    acts = {"keep": body, "null": "'N'", "hold": "NULL"}
    whens = " ".join(f"WHEN {cond} THEN {acts[fit['actions'].get(name, 'hold')]}" for name, cond in classes_for(x, fit))
    return f"(CASE WHEN {x} IS NULL THEN 'N' {whens} ELSE {body} END)"


def timeout_error(exc: BaseException) -> UnitTimeout:
    return UnitTimeout(str(exc).strip() or type(exc).__name__)
