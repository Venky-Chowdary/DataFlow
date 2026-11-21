"""Snowflake grammar the fakesnow emulator does not enforce.

fakesnow runs every statement on DuckDB, which accepts SQL that Snowflake's
compiler rejects with ``001003 SQL compilation error: syntax error``. Each
rule below names one such construct, with the Snowflake reference that
leaves it out:

* ``x IS [NOT] TRUE | FALSE | UNKNOWN`` — Snowflake's ``IS`` forms are only
  ``IS [NOT] NULL`` and ``IS [NOT] DISTINCT FROM`` (SQL reference,
  "Comparison operators" / "Conditional expression functions"). D47: the
  SCD2 ``is_current IS TRUE`` and mirror ``_deleted IS NOT TRUE``
  predicates failed every production Snowflake run while fakesnow passed.
* ``INSERT ... ON CONFLICT`` / ``... RETURNING`` — PostgreSQL extensions;
  Snowflake's INSERT / UPDATE / DELETE grammar has neither (use MERGE).

Literals and quoted identifiers are blanked first so a value or a column
named ``is true`` is never mistaken for the construct.
"""

from __future__ import annotations

import re

_STRING = re.compile(r"'(?:[^'\\]|''|\\.)*'")
_QUOTED_ID = re.compile(r'"(?:[^"]|"")*"')

RULES: tuple[tuple[re.Pattern[str], str], ...] = (
    (
        re.compile(r"\bIS\s+(?:NOT\s+)?(?:TRUE|FALSE|UNKNOWN)\b", re.IGNORECASE),
        "boolean test IS [NOT] TRUE/FALSE/UNKNOWN (Snowflake has only "
        "IS [NOT] NULL and IS [NOT] DISTINCT FROM)",
    ),
    (re.compile(r"\bON\s+CONFLICT\b", re.IGNORECASE), "INSERT ... ON CONFLICT"),
    (re.compile(r"\bRETURNING\b", re.IGNORECASE), "DML ... RETURNING"),
)


def _blank(sql: str) -> str:
    return _QUOTED_ID.sub('""', _STRING.sub("''", sql or ""))


def snowflake_grammar_violations(sql: str) -> list[str]:
    """The rules ``sql`` breaks (empty when Snowflake would compile it)."""
    text = _blank(sql)
    return [why for pattern, why in RULES if pattern.search(text)]
