"""Read source CHECK constraints and carry the portable ones onto create-new.

Two separate jobs, deliberately in one module because they share the honesty
contract:

``probe_check_constraints``
    Reads the source catalog. A catalog that cannot be read reports
    ``status="unavailable"`` — never an empty list, which would certify that a
    table has no CHECK constraints when we simply could not look.

``plan_check_carry``
    Decides, per predicate, whether the destination dialect can enforce the
    *same rule*. A CHECK is a data-integrity guarantee: emitting a predicate
    that means something subtly different on the destination is worse than
    admitting we did not carry it. So the grammar is a whitelist, every
    identifier is remapped through the Map contract and re-quoted, and anything
    outside the grammar (casts, subqueries, unknown functions, engine-specific
    operators) is refused rather than approximated.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Literal

logger = logging.getLogger(__name__)

CheckStatus = Literal["measured", "unavailable"]


@dataclass(frozen=True)
class CheckConstraint:
    name: str
    predicate: str
    columns: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "predicate": self.predicate, "columns": list(self.columns)}


@dataclass(frozen=True)
class CheckConstraints:
    """CHECK predicates for one table, or an explicit "could not read"."""

    dialect: str
    status: CheckStatus
    detail: str = ""
    items: tuple[CheckConstraint, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "dialect": self.dialect,
            "status": self.status,
            "detail": self.detail,
            "items": [i.to_dict() for i in self.items],
        }


# ---------------------------------------------------------------------------
# catalog probes
# ---------------------------------------------------------------------------

_NOT_NULL_ECHO = re.compile(r'^\(?\s*"?[\w$#]+"?\s*\)?\s+is\s+not\s+null\s*\)?$', re.I)


def is_not_null_echo(predicate: str) -> bool:
    """PostgreSQL/Oracle store every NOT NULL as a CHECK; not_null owns those."""
    return bool(_NOT_NULL_ECHO.match((predicate or "").strip()))


def _rows(cursor: Any, sql: str, params: tuple[Any, ...] | dict[str, Any]) -> list[tuple]:
    cursor.execute(sql, params)
    return list(cursor.fetchall() or [])


def probe_check_constraints(
    dialect: str,
    cursor: Any,
    schema: str,
    table: str,
) -> CheckConstraints:
    """Read CHECK predicates for ``schema.table``; never raise, never guess."""
    d = (dialect or "").strip().lower()
    if d in {"postgres", "redshift", "timescale", "cockroach"}:
        d = "postgresql"
    if d in {"mssql", "azuresql"}:
        d = "sqlserver"
    if d in {"mariadb"}:
        d = "mysql"
    reader = {
        "postgresql": _postgres_checks,
        "mysql": _mysql_checks,
        "sqlserver": _sqlserver_checks,
        "oracle": _oracle_checks,
        "sqlite": _sqlite_checks,
    }.get(d)
    if reader is None:
        return CheckConstraints(
            dialect=d or "unknown",
            status="unavailable",
            detail=f"No CHECK-constraint catalog reader for dialect {d or 'unknown'!r}.",
        )
    try:
        from services.physical_storage_metadata import as_driver_cursor

        return reader(as_driver_cursor(cursor), schema, table)
    except Exception as exc:  # noqa: BLE001 — a refused catalog is evidence
        return CheckConstraints(
            dialect=d,
            status="unavailable",
            detail=(
                f"{d} CHECK catalog unreadable ({exc}); this is not proof that the "
                "table has no CHECK constraints."
            ),
        )


def _postgres_checks(cursor: Any, schema: str, table: str) -> CheckConstraints:
    rows = _rows(
        cursor,
        """
        SELECT c.conname,
               pg_get_constraintdef(c.oid, true),
               ARRAY(
                 SELECT a.attname FROM unnest(c.conkey) k
                 JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k
               )
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE c.contype = 'c' AND t.relname = %s AND n.nspname = %s
        """,
        (table, schema or "public"),
    )
    items = []
    for name, definition, cols in rows:
        predicate = _strip_check_prefix(str(definition or ""))
        if not predicate or is_not_null_echo(predicate):
            continue
        items.append(
            CheckConstraint(str(name), predicate, tuple(str(c) for c in (cols or [])))
        )
    return CheckConstraints("postgresql", "measured", items=tuple(items))


def _mysql_checks(cursor: Any, schema: str, table: str) -> CheckConstraints:
    # information_schema.check_constraints only exists from MySQL 8.0.16 /
    # MariaDB 10.2 — an older server cannot be read, it is not check-free.
    rows = _rows(
        cursor,
        """
        SELECT cc.constraint_name, cc.check_clause
        FROM information_schema.check_constraints cc
        JOIN information_schema.table_constraints tc
          ON tc.constraint_schema = cc.constraint_schema
         AND tc.constraint_name = cc.constraint_name
        WHERE tc.table_name = %s
          AND tc.constraint_schema = COALESCE(NULLIF(%s, ''), DATABASE())
        """,
        (table, schema or ""),
    )
    items = [
        CheckConstraint(str(name), _strip_check_prefix(str(clause or "")))
        for name, clause in rows
        if str(clause or "").strip() and not is_not_null_echo(str(clause))
    ]
    return CheckConstraints("mysql", "measured", items=tuple(items))


def _sqlserver_checks(cursor: Any, schema: str, table: str) -> CheckConstraints:
    sql = """
        SELECT cc.name, cc.definition, COL_NAME(cc.parent_object_id, cc.parent_column_id)
        FROM sys.check_constraints cc
        JOIN sys.tables t ON t.object_id = cc.parent_object_id
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        WHERE t.name = {p} AND s.name = COALESCE(NULLIF({p}, ''), 'dbo')
    """
    try:
        rows = _rows(cursor, sql.format(p="%s"), (table, schema or ""))
    except Exception:  # noqa: BLE001 — driver paramstyle fallback
        rows = _rows(cursor, sql.format(p="?"), (table, schema or ""))
    items = []
    for name, definition, col in rows:
        predicate = _strip_check_prefix(str(definition or ""))
        if not predicate or is_not_null_echo(predicate):
            continue
        items.append(CheckConstraint(str(name), predicate, (str(col),) if col else ()))
    return CheckConstraints("sqlserver", "measured", items=tuple(items))


def _oracle_checks(cursor: Any, schema: str, table: str) -> CheckConstraints:
    # search_condition is LONG (unreadable in most drivers); the VC mirror is
    # 12c+. Oracle also materializes every NOT NULL as a generated CHECK.
    rows = _rows(
        cursor,
        """
        SELECT constraint_name, search_condition_vc
        FROM all_constraints
        WHERE constraint_type = 'C'
          AND UPPER(table_name) = UPPER(:tbl)
          AND (:own IS NULL OR UPPER(owner) = UPPER(:own))
        """,
        {"tbl": table, "own": schema or None},
    )
    items = [
        CheckConstraint(str(name), _strip_check_prefix(str(cond or "")))
        for name, cond in rows
        if str(cond or "").strip() and not is_not_null_echo(str(cond))
    ]
    return CheckConstraints("oracle", "measured", items=tuple(items))


def _sqlite_checks(cursor: Any, schema: str, table: str) -> CheckConstraints:
    rows = _rows(
        cursor,
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    )
    ddl = str(rows[0][0]) if rows and rows[0] and rows[0][0] else ""
    items = [
        CheckConstraint("", predicate)
        for predicate in _extract_sqlite_checks(ddl)
        if not is_not_null_echo(predicate)
    ]
    return CheckConstraints("sqlite", "measured", items=tuple(items))


def _extract_sqlite_checks(ddl: str) -> list[str]:
    """SQLite has no constraint catalog — the CREATE statement is the catalog."""
    out: list[str] = []
    for match in re.finditer(r"\bCHECK\s*\(", ddl or "", re.I):
        depth = 0
        start = match.end() - 1
        for i in range(start, len(ddl)):
            if ddl[i] == "(":
                depth += 1
            elif ddl[i] == ")":
                depth -= 1
                if depth == 0:
                    out.append(ddl[start + 1 : i].strip())
                    break
    return [p for p in out if p]


def _strip_check_prefix(text: str) -> str:
    """``CHECK ((qty > 0))`` / ``((qty>0))`` -> ``qty > 0``."""
    out = (text or "").strip()
    if out.upper().startswith("CHECK"):
        out = out[5:].strip()
    while out.startswith("(") and out.endswith(")") and _balanced_outer(out):
        out = out[1:-1].strip()
    return out


def _balanced_outer(text: str) -> bool:
    """True when the outermost parens wrap the whole expression."""
    depth = 0
    for i, ch in enumerate(text):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return i == len(text) - 1
    return False


# ---------------------------------------------------------------------------
# portability
# ---------------------------------------------------------------------------

_KEYWORDS = {
    "AND",
    "OR",
    "NOT",
    "IN",
    "IS",
    "NULL",
    "BETWEEN",
    "LIKE",
    "TRUE",
    "FALSE",
}

# Char-length spelling per destination. MySQL's LENGTH counts *bytes*, so a
# source LENGTH predicate cannot be carried to MySQL as LENGTH and vice versa.
_CHAR_LENGTH = {
    "postgresql": "LENGTH",
    "mysql": "CHAR_LENGTH",
    "sqlserver": "LEN",
    "oracle": "LENGTH",
    "sqlite": "LENGTH",
}
_PORTABLE_FUNCTIONS = {"UPPER", "LOWER", "ABS", "TRIM"}
_LENGTH_FUNCTIONS = {"LENGTH", "CHAR_LENGTH", "LEN"}
# Engines without a boolean literal: TRUE/FALSE cannot be carried to them.
_NO_BOOLEAN_LITERAL = {"sqlserver", "oracle"}

# PostgreSQL stores `status IN ('a','b')` as an ANY(ARRAY[...]) over text casts,
# and MySQL prefixes every literal with its charset introducer. Both are the
# engine's own spelling of a portable rule, not a different rule.
_PG_TEXT_CAST = re.compile(
    r"::\s*(?:text|character\s+varying|varchar|bpchar|character|char|name)(?:\[\])?",
    re.I,
)
_PG_ANY_ARRAY = re.compile(
    r"=\s*ANY\s*\(\s*\(?\s*ARRAY\[(?P<items>[^\]]*)\]\s*\)?\s*\)",
    re.I,
)
_MYSQL_INTRODUCER = re.compile(r"_[A-Za-z0-9]+(?=')")
# ``(qty) > 0`` is the engine echoing its own parse tree, not a grouping. The
# lookbehind keeps ``LEN(qty)`` — a call — intact.
_BARE_PARENS = re.compile(
    r"(?<![\w\"`\]])\(\s*([A-Za-z_][\w$#]*|\"[^\"]*\"|`[^`]*`|\[[^\]]*\])\s*\)"
)


def normalize_source_predicate(predicate: str, source_dialect: str) -> str:
    """Undo the engine's own storage spelling before portability is judged.

    Refusing PostgreSQL's ``(status)::text = ANY (ARRAY['a'::varchar])`` would
    drop a plainly portable ``status IN ('a')`` on the most common source in
    the fleet. Only lossless rewrites belong here: text casts that compare a
    character column against character literals, and charset introducers.
    """
    text = (predicate or "").strip()
    src = (source_dialect or "").strip().lower()
    if src in {"postgresql", "postgres", "redshift", "timescale", "cockroach"}:
        text = _PG_TEXT_CAST.sub("", text)
        text = _PG_ANY_ARRAY.sub(lambda m: f"IN ({m.group('items').strip()})", text)
    if src in {"mysql", "mariadb"}:
        text = _mysql_unescape(text)
        text = _MYSQL_INTRODUCER.sub("", text)
    return _BARE_PARENS.sub(r"\1", text).strip()


def _mysql_unescape(text: str) -> str:
    r"""MySQL stores ``s <> _utf8mb4\'it\\\'s\'`` — backslash-escaped, not SQL.

    Undo one level of backslash escaping, then respell an embedded quote the
    standard way (``''``) so the predicate parses on every other engine.
    """
    out: list[str] = []
    i = 0
    while i < len(text):
        ch = text[i]
        if ch == "\\" and i + 1 < len(text):
            out.append(text[i + 1])
            i += 2
            continue
        out.append(ch)
        i += 1
    return "".join(out).replace("\\'", "''")


_TOKEN_RE = re.compile(
    r"""
    (?P<ws>\s+)
  | (?P<string>'(?:[^']|'')*')
  | (?P<number>-?\d+(?:\.\d+)?)
  | (?P<quoted>"(?:[^"]|"")*"|`[^`]*`|\[[^\]]*\])
  | (?P<word>[A-Za-z_][A-Za-z_0-9$#]*)
  | (?P<op><=|>=|<>|!=|=|<|>|\(|\)|,|\+|-|\*|/|%)
    """,
    re.X,
)


@dataclass
class CheckCarryDecision:
    """One source CHECK and what the destination can do with it."""

    source: CheckConstraint
    carried: bool
    dest_sql: str = ""
    reason: str = ""
    columns: tuple[str, ...] = field(default_factory=tuple)


def render_check_for_dialect(
    predicate: str,
    *,
    source_dialect: str,
    dest_dialect: str,
    column_map: dict[str, str],
    quote: Any,
) -> tuple[str, str]:
    """Return ``(dest_sql, "")`` or ``("", refusal reason)``.

    ``column_map`` maps a *folded* source column name to its destination name;
    a predicate touching a column that was not mapped cannot be carried.
    """
    src_d = (source_dialect or "").strip().lower()
    dest_d = (dest_dialect or "").strip().lower()
    text = normalize_source_predicate(predicate, src_d)
    if not text:
        return "", "Empty predicate."
    if ";" in text or "--" in text or "/*" in text:
        return "", "Predicate contains a statement terminator or comment."
    if re.search(r"\bSELECT\b", text, re.I):
        return "", "Subquery CHECKs are not carried; the destination rule would differ."
    if "::" in text:
        return "", "PostgreSQL cast syntax is not portable; refuse to reinterpret it."

    out: list[str] = []
    pos = 0
    pending_call = False
    while pos < len(text):
        match = _TOKEN_RE.match(text, pos)
        if not match:
            return "", f"Unparsable token at offset {pos} in predicate."
        pos = match.end()
        kind = match.lastgroup
        value = match.group()
        if kind == "ws":
            continue
        if kind in {"string", "number"}:
            out.append(value)
            continue
        if kind == "op":
            if value == "(" and pending_call:
                # ``LEN (x)`` is legal but ugly; keep the call spelling tight.
                out[-1] += "("
                pending_call = False
                continue
            out.append("<>" if value == "!=" else value)
            continue
        if kind == "quoted":
            ident = value[1:-1].replace('""', '"')
            dest_col = column_map.get(ident.casefold())
            if not dest_col:
                return "", f"CHECK references unmapped column {ident!r}."
            out.append(quote(dest_col, dest_d))
            continue
        # bare word: keyword, function, or identifier
        upper = value.upper()
        if upper in _KEYWORDS:
            # ``IN (`` is a keyword followed by a list, not a function call.
            if upper in {"TRUE", "FALSE"} and dest_d in _NO_BOOLEAN_LITERAL:
                return "", f"{dest_d} has no boolean literal; refuse to substitute 1/0."
            out.append(upper)
            continue
        is_call = text[pos:].lstrip().startswith("(")
        if is_call:
            pending_call = True
            if upper in _LENGTH_FUNCTIONS:
                if upper == "LENGTH" and src_d in {"mysql", "mariadb"} and dest_d != "mysql":
                    return "", (
                        "MySQL LENGTH() counts bytes; carrying it elsewhere would "
                        "change the rule for multi-byte text."
                    )
                if dest_d == "mysql" and upper == "LENGTH" and src_d not in {"mysql", "mariadb"}:
                    out.append("CHAR_LENGTH")
                else:
                    out.append(_CHAR_LENGTH.get(dest_d, "LENGTH"))
                continue
            if upper in _PORTABLE_FUNCTIONS:
                out.append(upper)
                continue
            return "", f"Function {value}() is not on the portable CHECK whitelist."
        dest_col = column_map.get(value.casefold())
        if not dest_col:
            return "", f"CHECK references unmapped column {value!r}."
        out.append(quote(dest_col, dest_d))

    rendered = _join_tokens(out)
    return rendered, ""


def _join_tokens(tokens: list[str]) -> str:
    out = ""
    for token in tokens:
        if not out:
            out = token
            continue
        if token in {")", ","} or out.endswith("("):
            out += token
        else:
            out += f" {token}"
    return out


def plan_check_carry(
    checks: CheckConstraints | None,
    *,
    dest_dialect: str,
    column_map: dict[str, str],
    quote: Any,
) -> list[CheckCarryDecision]:
    """Decide carry/refuse per CHECK. Callers still emit an aspect when empty."""
    if checks is None or checks.status != "measured":
        return []
    decisions: list[CheckCarryDecision] = []
    for item in checks.items:
        dest_sql, reason = render_check_for_dialect(
            item.predicate,
            source_dialect=checks.dialect,
            dest_dialect=dest_dialect,
            column_map=column_map,
            quote=quote,
        )
        decisions.append(
            CheckCarryDecision(
                source=item,
                carried=bool(dest_sql),
                dest_sql=dest_sql,
                reason=reason,
                columns=item.columns,
            )
        )
    return decisions
