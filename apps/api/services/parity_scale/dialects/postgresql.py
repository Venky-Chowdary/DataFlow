"""PostgreSQL dialect: CE1 rendered in SQL, hashed with ``sha256()``, summed as ``numeric``.

Every rule below is the SQL spelling of a rule in :mod:`services.parity_scale.ce1`
and is checked against the Python reference value by value on a live engine
(``tests/test_parity_scale_pg_live.py``). Requires PostgreSQL 14+ (exact
``numeric`` from ``extract(epoch …)``, ``trim_scale``, ``sha256``).

This module also owns the legacy whole-table digest of
:mod:`services.engine_checksum` (md5 of ``::text`` renderings, which Gate-8 uses
today when ``DATAFLOW_ENGINE_DIGEST=1``). It moved here unchanged so the
PostgreSQL rendering rules live in one place; Gate-8 keeps calling it until
phase 3 switches that path to the CE1 digest.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import Any

from services.parity_scale.dialects.base import Digest, KeyBound, LeafRow, Side, UnitTimeout, range_predicate
from services.parity_scale.plan import ColumnPlan, TablePlan

#: Session settings every rendering depends on (``native`` ``::text`` above all).
SESSION_SETTINGS: tuple[str, ...] = (
    "SET TIME ZONE 'UTC'",
    "SET extra_float_digits = 1",
    "SET DateStyle = 'ISO, YMD'",
    "SET IntervalStyle = 'postgres'",
    "SET bytea_output = 'hex'",
)

def quote_ident(name: str) -> str:
    from connectors.sql_identifiers import quote_sql_identifier, require_safe_identifier

    return quote_sql_identifier(require_safe_identifier(name, preserve_case=True))


# ── legacy whole-table digest (engine_checksum) ─────────────────────────────


def legacy_md5_checksum_sql(table_ref: str, columns: list[str], *, where: str = "") -> str:
    """``SELECT count, digest`` over every row of ``table_ref`` (pre-CE1 rule).

    Each row renders to one text value, is hashed with md5, and the leading 64
    bits are summed as ``numeric``. Summing rather than aggregating in order is
    what makes the result independent of scan order; ``numeric`` rather than
    ``bigint`` is what keeps a wide table from overflowing the accumulator.
    """
    fields = []
    for col in columns:
        quoted = quote_ident(col)
        # The IS NULL flag rides inside the hashed text so that NULL and the
        # empty string cannot digest alike — the same distinction the Python
        # fingerprint path draws, and one that has already been a real bug here.
        fields.append(
            f"md5(coalesce({quoted}::text, '') || ({quoted} IS NULL)::text)"
        )
    row_text = " || ".join(fields)
    where_sql = f" WHERE {where}" if (where or "").strip() else ""
    return (
        "SELECT count(*)::bigint AS n, "
        f"coalesce(sum(('x' || substr(md5({row_text}), 1, 15))::bit(60)::bigint::numeric), 0) AS digest "
        f"FROM {table_ref}{where_sql}"  # nosec B608 — identifiers quoted above
    )


# ── CE1 in SQL ──────────────────────────────────────────────────────────────


def _inline(text_sql: str) -> str:
    # The database encoding is checked to be UTF8, so octet_length is the
    # UTF-8 byte length CE1 prefixes.
    return f"('V' || octet_length({text_sql})::text || ':' || {text_sql})"


def _digest(text_sql: str) -> str:
    return f"('D' || encode(sha256(convert_to({text_sql}, 'UTF8')), 'hex'))"


def _foreign(cls: str, text_sql: str) -> str:
    return f"('X{cls}:' || octet_length({text_sql})::text || ':' || {text_sql})"


def _units(u_sql: str, digits: int, *, project: bool, clock: bool = False) -> str:
    """Epoch (or since-midnight) units at ``digits``; ``u_sql`` is exact microseconds."""
    if digits >= 6:
        return _inline(f"({u_sql})::text")
    step = 10 ** (6 - digits)
    if project:
        rounded = f"floor(({u_sql} + {step // 2}) / {step})"
        if clock:
            # 23:59:59.9… cannot carry into the next day inside a TIME column:
            # carrier_instant keeps the second it can store.
            rounded = (
                f"(CASE WHEN floor(({u_sql} + {step // 2}) / {step}) * {step} >= 86400000000 "
                f"THEN floor({u_sql} / 1000000) * {1_000_000 // step} "
                f"ELSE floor(({u_sql} + {step // 2}) / {step}) END)"
            )
        return _inline(f"({rounded})::text")
    return (
        f"(CASE WHEN mod({u_sql}, {step}) = 0 THEN {_inline(f'div({u_sql}, {step})::text')} "
        f"ELSE {_foreign('us', f'({u_sql})::text')} END)"
    )


def field_sql(col: ColumnPlan, *, column: str, role: str, column_type: str) -> str:
    """The CE1 field of one column as a SQL ``text`` expression."""
    x = quote_ident(column)
    kind = col.kind
    source = role == "source"
    if kind == "integer":
        body = _inline(f"{x}::text")
    elif kind == "boolean":
        body = _inline(f"(CASE WHEN {x} THEN '1' ELSE '0' END)")
    elif kind == "decimal":
        body = _inline(f"trim_scale({x})::text")
    elif kind in {"text", "char"}:
        text = f"rtrim({x}::text, ' ')" if kind == "char" else f"{x}::text"
        body = _digest(text) if col.width == "digest" else _inline(text)
        if col.empty_is_null:
            body = f"(CASE WHEN octet_length({x}::text) = 0 THEN 'N' ELSE {body} END)"
    elif kind == "date":
        body = (
            f"(CASE WHEN isfinite({x}) THEN {_inline(f'({x} - DATE {chr(39)}1970-01-01{chr(39)})::text')} "
            f"WHEN {x} > DATE '1970-01-01' THEN 'V4:+inf' ELSE 'V4:-inf' END)"
        )
    elif kind in {"timestamp", "timestamptz"}:
        u = f"trunc(extract(epoch FROM {x}) * 1000000)"
        body = (
            f"(CASE WHEN NOT isfinite({x}) THEN (CASE WHEN {x} > 'epoch' THEN 'V4:+inf' ELSE 'V4:-inf' END) "
            f"ELSE {_units(u, col.digits, project=source)} END)"
        )
    elif kind == "time":
        u = f"trunc(extract(epoch FROM {x}) * 1000000)"
        body = _units(u, col.digits, project=source, clock=True)
    elif kind == "uuid":
        body = _inline(f"{x}::text")
    elif kind == "binary":
        body = f"('D' || encode(sha256({x}), 'hex'))"
    elif kind == "native":
        body = _inline(f"{x}::text")
    else:  # pragma: no cover - the plan only emits known kinds
        raise ValueError(f"no PostgreSQL rendering for CE1 kind {kind!r}")
    fit = col.decimal_fit if source else None
    if fit:
        classes = decimal_fit_classes(x, fit)
        # NULL (not 'N') marks a held-out cell; the row filter removes those rows.
        acts = {"keep": body, "null": "'N'", "hold": "NULL"}
        whens = " ".join(
            f"WHEN {cond} THEN {acts[fit['actions'].get(name, 'hold')]}" for name, cond in classes
        )
        return f"(CASE WHEN {x} IS NULL THEN 'N' {whens} ELSE {body} END)"
    return f"(CASE WHEN {x} IS NULL THEN 'N' ELSE {body} END)"


def decimal_fit_classes(x: str, fit: dict[str, Any]) -> list[tuple[str, str]]:
    """``(class, SQL condition)`` in evaluation order, replaying the writer's fit test.

    ``fits_decimal`` strips trailing zeros, refuses any significant digit past
    the scale and counts integer digits of the *normalised* value — zero counts
    one integer digit, so ``0`` does not fit ``NUMERIC(p,p)`` there, and does
    not here either. Conditions are tried in order (``CASE`` short-circuits),
    so ``trunc`` never sees ``NaN`` or an infinity.
    """
    s = int(fit["scale"])
    max_int = max(int(fit["precision"]) - s, 0)
    scale_over = f"{x} <> trunc({x}, {s})"
    int_over = f"NOT (abs(trunc({x})) < 1e{max_int} AND ({x} <> 0 OR {max_int} >= 1))"
    return [
        ("nan", f"{x} = 'NaN'"),
        ("inf", f"{x} IN ('Infinity'::numeric, '-Infinity'::numeric)"),
        ("both", f"({scale_over}) AND ({int_over})"),
        ("scale", scale_over),
        ("int", int_over),
    ]


class PostgresSide(Side):
    """One PostgreSQL table. Read-only: every statement runs in a read-only transaction."""

    def __init__(
        self,
        *,
        role: str,
        plan: TablePlan,
        table: str,
        schema: str = "public",
        connection: Any = None,
        connect: Any = None,
    ) -> None:
        super().__init__(role=role, plan=plan, table=table, schema=schema or "public")
        if connection is None:
            if connect is None:
                raise ValueError("PostgresSide needs a connection or a connect() factory")
            connection = connect()
        self.conn = connection
        self.engine_cols = list(plan.engine_columns)
        self.compared_cols = list(plan.compared_columns)
        self.key_cols = list(plan.key_plans()) if plan.key else []
        self._prepare_session()
        self._row_fields = [
            field_sql(c, column=self.column_name(c), role=role, column_type=self.column_type(c))
            for c in self.engine_cols
        ]
        self._key_fields = [
            field_sql(c, column=self.column_name(c), role=role, column_type=self.column_type(c))
            for c in self.key_cols
        ]
        self._held = self._held_predicate()

    # session -----------------------------------------------------------------
    def _prepare_session(self) -> None:
        self.conn.autocommit = False
        with self.conn.cursor() as cur:
            cur.execute("SHOW server_encoding")
            encoding = str(cur.fetchone()[0]).upper()
            cur.execute("SHOW server_version_num")
            version = int(cur.fetchone()[0])
        self.conn.rollback()
        if encoding not in {"UTF8", "UNICODE"}:
            raise ValueError(f"Parity at Scale needs a UTF8 database (this one is {encoding})")
        if version < 140000:
            raise ValueError("Parity at Scale needs PostgreSQL 14 or later (exact extract(epoch))")
        self.conn.autocommit = True
        with self.conn.cursor() as cur:
            for stmt in SESSION_SETTINGS:
                cur.execute(stmt)
        self.conn.autocommit = False

    # SQL pieces ----------------------------------------------------------------
    @property
    def _table_ref(self) -> str:
        from connectors.sql_identifiers import quote_table_ref

        return quote_table_ref(self.table, self.schema, dialect="postgresql")

    def _quoted_keys(self) -> list[str]:
        return [quote_ident(self.column_name(c)) for c in self.key_cols]

    def _held_predicate(self) -> str:
        if not self.is_source:
            return ""
        parts = []
        for col in self.engine_cols:
            fit = col.decimal_fit
            if not fit:
                continue
            x = quote_ident(self.column_name(col))
            whens = " ".join(
                f"WHEN {cond} THEN {'TRUE' if fit['actions'].get(name, 'hold') == 'hold' else 'FALSE'}"
                for name, cond in decimal_fit_classes(x, fit)
            )
            parts.append(f"({x} IS NOT NULL AND (CASE {whens} ELSE FALSE END))")
        return " OR ".join(parts) if parts else ""

    def _where(self, lo: KeyBound, hi: KeyBound) -> tuple[str, list[Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if self.key_cols:
            sql, p = range_predicate(self._quoted_keys(), lo, hi, "%s")
            if sql != "TRUE":
                clauses.append(sql)
                params.extend(p)
        if self._held:
            clauses.append(f"NOT ({self._held})")
        return (" AND ".join(clauses) or "TRUE"), params

    def _hashed(self, lo: KeyBound, hi: KeyBound, *, extra: str = "") -> tuple[str, list[Any]]:
        """Subquery yielding ``k…, kb (key bytes), h (row hash hex)`` per row."""
        where, params = self._where(lo, hi)
        row = " || ".join(self._row_fields)
        keys = ""
        if self.key_cols:
            kexpr = " || ".join(self._key_fields)
            qk = ", ".join(f"{q} AS k{i}" for i, q in enumerate(self._quoted_keys()))
            keys = f"{qk}, convert_to({kexpr}, 'UTF8') AS kb, "
        sql = (
            f"SELECT {keys}encode(sha256(convert_to({row}, 'UTF8')), 'hex') AS h "
            f"FROM {self._table_ref} WHERE {where}{extra} OFFSET 0"  # nosec B608 — identifiers quoted
        )
        return sql, params

    @staticmethod
    def _a(h: str = "h") -> str:
        return f"('x' || substr({h}, 2, 15))::bit(60)::bigint"

    @staticmethod
    def _b(h: str = "h") -> str:
        return f"('x' || substr({h}, 18, 15))::bit(60)::bigint"

    def _bucket(self, level: int) -> str:
        if self.key_cols:
            top = "('x' || substr(encode(sha256(kb), 'hex'), 1, 16))::bit(64)::bigint"
        else:
            top = "('x' || substr(h, 33, 16))::bit(64)::bigint"
        return f"(({top} >> {54 - 10 * level}) & 1023)"

    def _run(self, sql: str, params: Sequence[Any] = ()) -> list[tuple[Any, ...]]:
        import psycopg2

        try:
            with self.conn.cursor() as cur:
                cur.execute("SET TRANSACTION READ ONLY")
                if self.timeout_seconds:
                    cur.execute("SET LOCAL statement_timeout = %s", (int(self.timeout_seconds * 1000),))
                cur.execute(sql, list(params))
                rows = cur.fetchall()
            self.conn.commit()
            return rows
        except psycopg2.errors.QueryCanceled as exc:
            self.conn.rollback()
            raise UnitTimeout(str(exc).strip()) from exc
        except Exception:
            self.conn.rollback()
            raise

    @staticmethod
    def _digest_row(row: Sequence[Any]) -> Digest:
        return int(row[0] or 0), int(row[1] or 0), int(row[2] or 0)

    def _path_filter(self, parents: Sequence[tuple[int, ...]], params: list[Any]) -> str:
        if not parents or not parents[0]:
            return ""
        depth = len(parents[0])
        lhs = f"({', '.join(self._bucket(lvl) for lvl in range(depth))})" if depth > 1 else self._bucket(0)
        rows = []
        for path in parents:
            params.extend(path)
            marks = ", ".join("%s" for _ in path)
            rows.append(f"({marks})" if depth > 1 else marks)
        return f" WHERE {lhs} IN ({', '.join(rows)})"

    # operations ----------------------------------------------------------------
    def split_points(self, lo: KeyBound, hi: KeyBound, step: int) -> list[tuple[Any, ...]]:
        keys = ", ".join(self._quoted_keys())
        where, params = self._where(lo, hi)
        sql = (
            f"SELECT {keys} FROM (SELECT {keys}, row_number() OVER (ORDER BY {keys}) AS rn "
            f"FROM {self._table_ref} WHERE {where}) s WHERE rn > 1 AND mod(rn - 1, %s) = 0 ORDER BY {keys}"
        )
        return [tuple(r) for r in self._run(sql, [*params, max(1, int(step))])]

    def range_digest(self, lo: KeyBound, hi: KeyBound) -> Digest:
        inner, params = self._hashed(lo, hi)
        sql = f"SELECT count(*), sum({self._a()}), sum({self._b()}) FROM ({inner}) r"
        return self._digest_row(self._run(sql, params)[0])

    def _leaf_select(self, inner: str) -> str:
        if self.key_cols:
            ks = ", ".join(f"k{i}" for i in range(len(self.key_cols)))
            return f"SELECT {ks}, encode(kb, 'hex'), h, {self._a()}, {self._b()} FROM ({inner}) r"
        return f"SELECT h, {self._a()}, {self._b()} FROM ({inner}) r"

    def _leaf_rows(self, rows: list[tuple[Any, ...]], path_filtered: bool = False) -> list[LeafRow]:
        width = len(self.key_cols)
        out = []
        for row in rows:
            if width:
                out.append(LeafRow(tuple(row[:width]), row[width], int(row[width + 2]), int(row[width + 3])))
            else:
                out.append(LeafRow(None, row[0], int(row[1]), int(row[2])))
        return out

    def range_leaf(self, lo: KeyBound, hi: KeyBound) -> list[LeafRow]:
        inner, params = self._hashed(lo, hi)
        return self._leaf_rows(self._run(self._leaf_select(inner), params))

    def bucket_digests(
        self, level: int, parents: Sequence[tuple[int, ...]], lo: KeyBound = None, hi: KeyBound = None
    ) -> dict[tuple[int, ...], Digest]:
        inner, params = self._hashed(lo, hi)
        path_params: list[Any] = []
        path = self._path_filter(parents, path_params)
        groups = [self._bucket(lvl) for lvl in range(level + 1)]
        sql = (
            f"SELECT {', '.join(groups)}, count(*), sum({self._a()}), sum({self._b()}) "
            f"FROM ({inner}) r{path} GROUP BY {', '.join(str(i + 1) for i in range(level + 1))}"
        )
        out: dict[tuple[int, ...], Digest] = {}
        for row in self._run(sql, [*params, *path_params]):
            out[tuple(int(v) for v in row[: level + 1])] = self._digest_row(row[level + 1 :])
        return out

    def bucket_leaf(
        self, parents: Sequence[tuple[int, ...]], lo: KeyBound = None, hi: KeyBound = None
    ) -> list[LeafRow]:
        inner, params = self._hashed(lo, hi)
        path_params: list[Any] = []
        path = self._path_filter(parents, path_params)
        sql = f"SELECT * FROM ({self._leaf_select(inner)}) l" if not path else (
            self._leaf_select(f"SELECT * FROM ({inner}) q{path}")
        )
        return self._leaf_rows(self._run(sql, [*params, *path_params]))

    def fetch_rows(self, keys: Sequence[tuple[Any, ...]]) -> dict[str, list[tuple[Any, ...]]]:
        out: dict[str, list[tuple[Any, ...]]] = {}
        if not keys:
            return out
        quoted = self._quoted_keys()
        width = len(quoted)
        lhs = f"({', '.join(quoted)})" if width > 1 else quoted[0]
        cols = ", ".join(quote_ident(self.column_name(c)) for c in self.compared_cols)
        for start in range(0, len(keys), 1000):
            page = keys[start : start + 1000]
            params: list[Any] = []
            rows = []
            for key in page:
                params.extend(key)
                marks = ", ".join("%s" for _ in key)
                rows.append(f"({marks})" if width > 1 else marks)
            sql = (
                f"SELECT {', '.join(quoted)}, {cols} FROM {self._table_ref} "
                f"WHERE {lhs} IN ({', '.join(rows)})"  # nosec B608 — identifiers quoted
            )
            for row in self._run(sql, params):
                out.setdefault(self.key_hex(row[:width]), []).append(tuple(row[width:]))
        return out

    def keyless_examples(self, row_hashes: Sequence[str], limit: int) -> list[tuple[str, tuple[Any, ...]]]:
        if not row_hashes:
            return []
        where, params = self._where(None, None)
        row = " || ".join(self._row_fields)
        cols = ", ".join(quote_ident(self.column_name(c)) for c in self.compared_cols)
        sql = (
            f"SELECT h, {cols} FROM (SELECT encode(sha256(convert_to({row}, 'UTF8')), 'hex') AS h, {cols} "
            f"FROM {self._table_ref} WHERE {where} OFFSET 0) s WHERE h = ANY(%s) LIMIT %s"
        )
        return [(r[0], tuple(r[1:])) for r in self._run(sql, [*params, list(row_hashes), int(limit)])]

    def iter_stream(
        self, columns: Sequence[ColumnPlan], batch: int = 5000
    ) -> Iterator[tuple[tuple[Any, ...] | None, tuple[Any, ...]]]:
        import uuid as _uuid

        width = len(self.key_cols)
        names = [*self._quoted_keys(), *(quote_ident(self.column_name(c)) for c in columns)]
        where, params = self._where(None, None)
        sql = f"SELECT {', '.join(names)} FROM {self._table_ref} WHERE {where}"
        cur = self.conn.cursor(name=f"psd1_stream_{_uuid.uuid4().hex[:12]}")
        cur.itersize = batch
        try:
            cur.execute(sql, params)
            while True:
                rows = cur.fetchmany(batch)
                if not rows:
                    break
                for row in rows:
                    yield (tuple(row[:width]) if width else None), tuple(row[width:])
        finally:
            cur.close()
            self.conn.rollback()

    def held_count(self) -> int:
        if not self._held:
            return 0
        sql = f"SELECT count(*) FROM {self._table_ref} WHERE {self._held}"
        return int(self._run(sql)[0][0])

    def close(self) -> None:
        import psycopg2

        try:
            self.conn.close()
        except psycopg2.Error:  # pragma: no cover - closing a broken connection
            return None
