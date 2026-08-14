"""Collation equality is a uniqueness contract, not a DDL cosmetic.

AWS DMS copies bytes and lets the destination's *default* collation decide
whether ``Alpha`` and ``alpha`` are the same row. MySQL/MariaDB default to a
Unicode CI collation; PostgreSQL/Oracle/SQLite default to case-sensitive
equality. A checksum of copied rows stays green while the UNIQUE key on the
destination silently refuses the second row (DMS ``MISSING_TARGET``) — or,
the other way, accepts a pair the source UNIQUE forbade.

Competitors paste a source collation *name* when the destination happens to
know it, and drop it otherwise. Name-copy is not equality. This module
classifies the source into an equality class (case / accent polarity), then:

- emits a destination-native spelling that preserves that class when one
  exists (PostgreSQL CS → MySQL ``utf8mb4_bin``);
- refuses a lying spelling when the destination cannot express the class
  (MySQL Unicode CI → PostgreSQL: no portable CI collation, and ``citext``
  would change the type);
- records uniqueness polarity (preserved / widened / tightened) so the
  certificate cannot say ``carried`` for a UNIQUE that changed meaning.

UCA version (0900 vs 1400) is an extension point on ``EqualityClass``, not a
claim that ``utf8mb4_unicode_ci`` equals ``utf8mb4_0900_ai_ci``.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

from services.type_system import (
    is_accent_insensitive_collation,
    is_case_insensitive_collation,
    parse_collation,
)

Polarity = Literal["sensitive", "insensitive", "unknown"]
UniquenessPolarity = Literal["preserved", "widened", "tightened", "n/a"]


class DBAPICursor(Protocol):
    def execute(self, operation: str, parameters: tuple[Any, ...], /) -> object: ...

    def fetchall(self) -> Sequence[Sequence[Any]]: ...


__all__ = [
    "CollationDecision",
    "EqualityClass",
    "classify_equality",
    "plan_collation_carry",
    "destination_column_collations",
]


_STRING_RE = re.compile(
    r"^(?:N?(?:VAR)?CHAR|CHARACTER(?:\s+VARYING)?|TEXT|CLOB|NCLOB|STRING|"
    r"CITEXT|NVARCHAR2|VARCHAR2|LONGTEXT|TINYTEXT|MEDIUMTEXT|NTEXT|SYSNAME)\b",
    re.I,
)
_BIN_RE = re.compile(r"(?:^|_)BIN(?:ARY)?(?:_|$)|^(?:C|POSIX)$", re.I)


@dataclass(frozen=True)
class EqualityClass:
    """How two strings compare under this column's collation."""

    case: Polarity
    accent: Polarity
    native_name: str = ""
    charset: str = ""
    deterministic: bool = True

    @property
    def case_sensitive(self) -> bool:
        return self.case == "sensitive"

    def to_dict(self) -> dict[str, Any]:
        return {
            "case": self.case,
            "accent": self.accent,
            "native_name": self.native_name,
            "charset": self.charset,
            "deterministic": self.deterministic,
        }


@dataclass
class CollationDecision:
    source_column: str
    dest_column: str
    status: str  # carried | unsupported | skipped
    reason: str
    equality: EqualityClass | None = None
    dest_charset: str = ""
    dest_collation: str = ""
    uniqueness_polarity: UniquenessPolarity = "n/a"
    column_prefixes: list[str] = field(default_factory=list)

    def to_item_kwargs(self) -> dict[str, Any]:
        dest_ddl = " ".join(self.column_prefixes)
        return {
            "aspect": "collation",
            "name": self.dest_column or self.source_column,
            "status": self.status,
            "reason": self.reason,
            "source_detail": (
                self.equality.native_name if self.equality else self.source_column
            ),
            "dest_ddl": dest_ddl,
        }


@dataclass
class CollationCarryPlan:
    decisions: list[CollationDecision] = field(default_factory=list)
    # dest column -> fragments that must sit on the type, before NOT NULL.
    column_prefixes: dict[str, list[str]] = field(default_factory=dict)

    @property
    def carried(self) -> list[CollationDecision]:
        return [d for d in self.decisions if d.status == "carried"]


def _norm(dialect: str | None) -> str:
    d = (dialect or "").strip().lower()
    if d in {"postgres", "postgresql", "redshift", "timescale", "cockroach"}:
        return "redshift" if d == "redshift" else "postgresql"
    if d in {"mssql", "azure_sql", "azure_sql_database", "sqlserver"}:
        return "sqlserver"
    if d == "tidb":
        return "mysql"
    if d == "mariadb":
        return "mariadb"
    return d


def _is_string_carrier(declared: str | None) -> bool:
    text = (declared or "").strip()
    if not text:
        return False
    # Strip COLLATE / CHARSET so VARCHAR(32) COLLATE x still matches.
    base = re.split(r"\s+(?:COLLATE|CHARACTER\s+SET|CHARSET)\s+", text, maxsplit=1, flags=re.I)[0]
    return bool(_STRING_RE.match(base.strip()))


def classify_equality(
    dialect: str,
    *,
    collation: str = "",
    charset: str = "",
    inferred_type: str = "",
    deterministic: bool = True,
) -> EqualityClass:
    """Equality class from a catalog collation, or the engine's default.

    An empty name is not "no collation": PostgreSQL/Oracle/SQLite default to
    case-sensitive equality; MySQL/SQL Server default to case-insensitive.
    """
    name = (collation or parse_collation(inferred_type) or "").strip().strip('"').strip("`")
    d = _norm(dialect)
    cs = (charset or "").strip()
    if not deterministic:
        return EqualityClass(
            case="insensitive",
            accent="unknown",
            native_name=name,
            charset=cs,
            deterministic=False,
        )
    if not name:
        if d in {"mysql", "mariadb"}:
            return EqualityClass(
                case="insensitive", accent="insensitive", native_name="", charset=cs or "utf8mb4"
            )
        if d == "sqlserver":
            return EqualityClass(
                case="insensitive", accent="sensitive", native_name="", charset=cs
            )
        return EqualityClass(
            case="sensitive", accent="sensitive", native_name="", charset=cs
        )
    stamped = f"TEXT COLLATE {name}"
    if _BIN_RE.search(name) and not is_case_insensitive_collation(stamped):
        return EqualityClass(
            case="sensitive", accent="sensitive", native_name=name, charset=cs
        )
    case: Polarity = (
        "insensitive" if is_case_insensitive_collation(stamped) else "sensitive"
    )
    if is_accent_insensitive_collation(stamped):
        accent: Polarity = "insensitive"
    elif case == "insensitive":
        # CI without an AI token is accent-sensitive on SQL Server (CI_AS)
        # and accent-insensitive on MySQL legacy unicode/general_ci (handled
        # above). Remaining CI is unknown accent, not a guess of AI.
        accent = "sensitive" if d == "sqlserver" else "unknown"
    else:
        accent = "sensitive"
    return EqualityClass(case=case, accent=accent, native_name=name, charset=cs)


def _mysql_dest_spelling(eq: EqualityClass) -> tuple[str, str, str]:
    """(charset, collation, refusal). Portable names only — no UCA-version invent."""
    charset = eq.charset if eq.charset.lower() in {"utf8mb4", "utf8mb3", "utf8", "latin1"} else "utf8mb4"
    if charset in {"utf8", "utf8mb3"}:
        # 3-byte UTF-8 cannot hold supplementary-plane characters. Promote
        # rather than emit a dest that would truncate emoji the source held.
        charset = "utf8mb4"
    if eq.case == "sensitive":
        return charset, f"{charset}_bin", ""
    if eq.case == "insensitive" and eq.accent == "insensitive":
        native = eq.native_name
        if native and re.match(r"^[A-Za-z0-9_]+$", native) and "UTF8" in native.upper():
            return charset, native, ""
        return charset, "utf8mb4_unicode_ci", ""
    if eq.case == "insensitive" and eq.accent == "sensitive":
        return "", "", (
            "MySQL/MariaDB have no portable accent-sensitive CI collation "
            "(utf8mb4_0900_as_ci is version-gated). Refusing a unicode_ci "
            "stand-in that would equate café and cafe."
        )
    return "", "", "Source collation equality class is unknown; refusing a guessed dest COLLATE."


def _uniqueness_polarity(src: EqualityClass, dest: EqualityClass) -> UniquenessPolarity:
    if src.case == "unknown" or dest.case == "unknown":
        return "n/a"
    if src.case == dest.case:
        if src.accent == dest.accent or "unknown" in {src.accent, dest.accent}:
            return "preserved"
        if src.accent == "sensitive" and dest.accent == "insensitive":
            return "tightened"
        if src.accent == "insensitive" and dest.accent == "sensitive":
            return "widened"
        return "preserved"
    if src.case == "sensitive" and dest.case == "insensitive":
        return "tightened"
    return "widened"


def _prefixes(charset: str, collation: str, dest: str) -> list[str]:
    parts: list[str] = []
    if dest in {"mysql", "mariadb"} and charset:
        parts.append(f"CHARACTER SET {charset}")
    if collation:
        if dest == "postgresql":
            safe = collation.replace('"', "")
            parts.append(f'COLLATE "{safe}"')
        else:
            parts.append(f"COLLATE {collation}")
    return parts


def _existing_collation(dest_type: str) -> str:
    return (parse_collation(dest_type) or "").strip().strip('"').strip("`")


def plan_collation_carry(
    *,
    catalog: Any,
    dest_dialect: str,
    dest_name_for_source: Callable[[str], str | None],
    dest_type_for_column: Callable[[str], str],
    unique_or_pk: set[str] | None = None,
) -> CollationCarryPlan:
    """Decide, per source string column, the destination equality spelling."""
    plan = CollationCarryPlan()
    dest = _norm(dest_dialect)
    src = _norm(getattr(catalog, "dialect", "") or "")
    collations = dict(getattr(catalog, "collations", None) or {})
    charsets = dict(getattr(catalog, "charsets", None) or {})
    column_types = dict(getattr(catalog, "column_types", None) or {})
    keyed = {str(c) for c in (unique_or_pk or [])}
    columns = list(getattr(catalog, "columns", None) or [])
    if not columns:
        return plan

    for src_col in columns:
        dest_col = dest_name_for_source(src_col)
        src_type = str(column_types.get(src_col) or "")
        dest_type = dest_type_for_column(dest_col) if dest_col else ""
        if not (_is_string_carrier(src_type) or _is_string_carrier(dest_type)):
            continue
        eq = classify_equality(
            src,
            collation=collations.get(src_col, ""),
            charset=charsets.get(src_col, ""),
            inferred_type=src_type,
        )
        if not dest_col:
            plan.decisions.append(
                CollationDecision(
                    source_column=src_col,
                    dest_column="",
                    status="unsupported",
                    reason="String column is not mapped; equality has nowhere to land.",
                    equality=eq,
                )
            )
            continue

        existing = _existing_collation(dest_type)
        charset = ""
        collation = ""
        refusal = ""
        if dest in {"mysql", "mariadb"}:
            charset, collation, refusal = _mysql_dest_spelling(eq)
        elif dest == "postgresql":
            if eq.case == "sensitive":
                # Default PG equality is CS. Emitting COLLATE "C" is optional;
                # an empty clause still preserves A ≠ a.
                charset, collation, refusal = "", "", ""
            else:
                refusal = (
                    "PostgreSQL cannot express Unicode case-insensitive equality "
                    "as a portable collation (citext would change the type). "
                    "UNIQUE on the destination will treat Alpha and alpha as "
                    "distinct; the source did not."
                )
        elif dest == "sqlite":
            if eq.case == "sensitive":
                charset, collation, refusal = "", "", ""
            else:
                refusal = (
                    "SQLite NOCASE is ASCII-only and is not Unicode CI; refusing "
                    "it as a stand-in for the source collation."
                )
        elif dest == "sqlserver":
            if eq.case == "sensitive":
                charset, collation, refusal = "", "Latin1_General_BIN", ""
            elif eq.accent == "insensitive":
                charset, collation, refusal = "", "Latin1_General_CI_AI", ""
            elif eq.case == "insensitive":
                charset, collation, refusal = "", "Latin1_General_CI_AS", ""
            else:
                refusal = "SQL Server equality class is unknown; refusing a guessed COLLATE."
        else:
            refusal = (
                f"Collation equality is not certified for destination '{dest}'; "
                "emitting an unproven COLLATE would be a claim, not a carry."
            )

        if existing:
            # Invent already copied a same-engine name. Do not double-emit.
            existing_eq = classify_equality(dest, collation=existing)
            polarity = _uniqueness_polarity(eq, existing_eq)
            keyed_here = src_col in keyed or dest_col in keyed
            if polarity == "tightened" and keyed_here:
                plan.decisions.append(
                    CollationDecision(
                        source_column=src_col,
                        dest_column=dest_col,
                        status="unsupported",
                        reason=(
                            f"Destination type already collates as {existing}; that "
                            "equality is tighter than the source and would reject "
                            "rows the source UNIQUE accepted (Alpha vs alpha)."
                        ),
                        equality=eq,
                        dest_collation=existing,
                        uniqueness_polarity=polarity,
                    )
                )
                continue
            reason = (
                f"Destination type already collates as {existing}; equality class "
                f"case={existing_eq.case} accent={existing_eq.accent}."
            )
            if polarity == "widened" and keyed_here:
                reason = (
                    f"Destination type collates as {existing} (case-sensitive) while "
                    "the source equated Alpha and alpha. UNIQUE is widened — not "
                    "the same uniqueness rule."
                )
            plan.decisions.append(
                CollationDecision(
                    source_column=src_col,
                    dest_column=dest_col,
                    status="carried" if polarity != "widened" else "unsupported",
                    reason=reason,
                    equality=eq,
                    dest_collation=existing,
                    uniqueness_polarity=polarity,
                )
            )
            continue

        if refusal:
            dest_eq = classify_equality(dest, collation="", charset=charset)
            polarity = _uniqueness_polarity(eq, dest_eq)
            plan.decisions.append(
                CollationDecision(
                    source_column=src_col,
                    dest_column=dest_col,
                    status="unsupported",
                    reason=refusal,
                    equality=eq,
                    uniqueness_polarity=polarity if dest_col in keyed or src_col in keyed else "n/a",
                )
            )
            continue

        dest_eq = classify_equality(dest, collation=collation, charset=charset) if collation else classify_equality(dest, collation="", charset=charset)
        polarity = _uniqueness_polarity(eq, dest_eq)
        prefixes = _prefixes(charset, collation, dest)
        keyed_here = src_col in keyed or dest_col in keyed
        if polarity == "tightened" and keyed_here:
            plan.decisions.append(
                CollationDecision(
                    source_column=src_col,
                    dest_column=dest_col,
                    status="unsupported",
                    reason=(
                        "Emitting this destination collation would tighten UNIQUE "
                        f"(source case={eq.case} → dest case={dest_eq.case}) and "
                        "reject rows the source accepted."
                    ),
                    equality=eq,
                    dest_charset=charset,
                    dest_collation=collation,
                    uniqueness_polarity=polarity,
                )
            )
            continue
        if polarity == "widened" and keyed_here and dest == "postgresql":
            # CS dest vs CI source: we correctly emit nothing, but UNIQUE changed.
            plan.decisions.append(
                CollationDecision(
                    source_column=src_col,
                    dest_column=dest_col,
                    status="unsupported",
                    reason=(
                        "PostgreSQL default equality is case-sensitive; the source "
                        "UNIQUE equated Alpha and alpha. Not claiming the uniqueness "
                        "rule was carried."
                    ),
                    equality=eq,
                    uniqueness_polarity=polarity,
                )
            )
            continue
        reason = (
            f"Equality class case={eq.case} accent={eq.accent} emitted as "
            f"{(' '.join(prefixes) or 'destination default (case-sensitive)')}."
        )
        if keyed_here:
            reason += f" UNIQUE polarity {polarity}."
        plan.decisions.append(
            CollationDecision(
                source_column=src_col,
                dest_column=dest_col,
                status="carried",
                reason=reason,
                equality=eq,
                dest_charset=charset,
                dest_collation=collation,
                uniqueness_polarity=polarity,
                column_prefixes=prefixes,
            )
        )
        if prefixes:
            plan.column_prefixes[dest_col] = prefixes
    return plan


_COLLATION_QUERY: dict[str, str] = {
    "postgresql": (
        "SELECT a.attname, COALESCE(col.collname, '') "
        "FROM pg_attribute a "
        "JOIN pg_class c ON c.oid = a.attrelid "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "LEFT JOIN pg_collation col ON col.oid = a.attcollation AND a.attcollation <> 0 "
        "WHERE c.relname = ? AND n.nspname = COALESCE(?, 'public') AND a.attnum > 0 "
        "AND NOT a.attisdropped"
    ),
    "mysql": (
        "SELECT column_name, collation_name FROM information_schema.columns "
        "WHERE table_name = ? AND table_schema = COALESCE(?, DATABASE())"
    ),
    "sqlserver": (
        "SELECT c.name, collation_name FROM sys.columns c "
        "JOIN sys.tables t ON t.object_id = c.object_id "
        "JOIN sys.schemas s ON s.schema_id = t.schema_id "
        "WHERE t.name = ? AND s.name = COALESCE(?, SCHEMA_NAME())"
    ),
}
_COLLATION_QUERY["mariadb"] = _COLLATION_QUERY["mysql"]


def destination_column_collations(
    *,
    dialect: str,
    schema: str,
    table: str,
    fetchall: Callable[[str, tuple[Any, ...]], Sequence[Sequence[Any]]],
) -> dict[str, str] | None:
    """Column → dest catalog collation, or None when the catalog could not be read."""
    dest = _norm(dialect)
    query = _COLLATION_QUERY.get(dest)
    if not query:
        return None
    unquoted = str(table).strip().strip('"').strip("`").strip("[").strip("]")
    try:
        rows = fetchall(query, (unquoted, schema or None))
    except Exception:  # noqa: BLE001 — unreadable catalog is unknown, not absent
        return None
    out: dict[str, str] = {}
    for row in rows or []:
        if not row or not row[0]:
            continue
        out[str(row[0])] = str(row[1] or "")
    return out
