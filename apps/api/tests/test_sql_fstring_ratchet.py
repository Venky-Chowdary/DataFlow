"""Ratchet: no new f-string ``.execute()`` sites.

DataFlow interpolates SQL through the canonical helpers (``quote_sql_identifier``,
``quote_table_ref``, bind params). A raw ``.execute(f"...{expr}...")`` call can
still be legitimate for already-quoted identifiers, but every new one is a fresh
injection surface that must be justified. The suite may only shrink from the
measured baseline — fix a site and the budget grows tighter.
"""

from __future__ import annotations

import pathlib
import re

BASELINE = 577

_PATTERN = re.compile(r"\.execute\(\s*f['\"]")
_ROOTS = ("services", "connectors", "src", "routers")


def _count_sites() -> tuple[int, dict[str, int]]:
    api = pathlib.Path(__file__).resolve().parents[1]
    per_file: dict[str, int] = {}
    for root in _ROOTS:
        for path in sorted((api / root).rglob("*.py")):
            n = len(_PATTERN.findall(path.read_text(errors="replace")))
            if n:
                per_file[str(path.relative_to(api))] = n
    return sum(per_file.values()), per_file


def test_no_new_fstring_execute_sites() -> None:
    total, per_file = _count_sites()
    assert total <= BASELINE, (
        f"f-string .execute() count grew {BASELINE} -> {total}; interpolate "
        "identifiers via quote_sql_identifier/quote_table_ref or bind params "
        f"instead. Offenders: {per_file}"
    )
