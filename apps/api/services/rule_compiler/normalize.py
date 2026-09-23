"""Name and header normalisation for a customer rule workbook.

The compiler must not invent a column. It only binds a cell to a name the
operator already has on the source or destination, after a conservative
normalisation (case, spaces, underscores). Ambiguous matches stay unbound.
"""

from __future__ import annotations

import re

_SPLIT = re.compile(r"[^a-z0-9]+")

# Keys are *folded* (case / punctuation stripped) because ``canonical_header``
# looks up ``fold(spoken)``. ``source_column`` and ``Source Column`` both
# become ``sourcecolumn``.
HEADER_ALIASES: dict[str, str] = {
    "source": "source_table",
    "src": "source_table",
    "srctable": "source_table",
    "sourcetable": "source_table",
    "fromtable": "source_table",
    "from": "source_table",
    "table": "source_table",
    "sourcecolumn": "source_column",
    "sourcecol": "source_column",
    "srccolumn": "source_column",
    "srccol": "source_column",
    "sourcefield": "source_column",
    "srcfield": "source_column",
    "fromcolumn": "source_column",
    "fromfield": "source_column",
    "column": "source_column",
    "field": "source_column",
    "destination": "dest_table",
    "dest": "dest_table",
    "desttable": "dest_table",
    "targettable": "dest_table",
    "totable": "dest_table",
    "destinationcolumn": "dest_column",
    "destcolumn": "dest_column",
    "destcol": "dest_column",
    "destinationfield": "dest_column",
    "destfield": "dest_column",
    "target": "dest_column",
    "targetcolumn": "dest_column",
    "targetfield": "dest_column",
    "tocolumn": "dest_column",
    "tofield": "dest_column",
    "to": "dest_column",
    "rule": "rule",
    "rules": "rule",
    "transformation": "rule",
    "transform": "rule",
    "mappingrule": "rule",
    "expression": "rule",
    "logic": "rule",
    "formula": "rule",
    "businessrule": "rule",
    "notes": "rule",
    "joinfrom": "join_from",
    "joinon": "join_on",
    "jointype": "join_type",
}


def fold(value: str) -> str:
    """Compare-key for a header or a column name."""
    return _SPLIT.sub("", (value or "").strip().lower())


def canonical_header(value: str) -> str:
    """Map a spoken header onto one of the compiler's columns, or ''."""
    return HEADER_ALIASES.get(fold(value), "")


def resolve_name(needle: str, candidates: list[str]) -> str:
    """The one candidate this cell names, or '' when none or several match.

    Exact (folded) wins. A unique startswith / contained match is accepted only
    when no other candidate also matches — ``id`` must not bind ``customer_id``
    and ``order_id`` at once.
    """
    want = fold(needle)
    if not want or not candidates:
        return ""
    exact = [c for c in candidates if fold(c) == want]
    if len(exact) == 1:
        return exact[0]
    if len(exact) > 1 or len(want) < 4:
        # Short tokens (`id`, `no`) match too many warehouse names. Fail closed.
        return ""
    loose = [c for c in candidates if fold(c).startswith(want) or want.startswith(fold(c))]
    if len(loose) == 1:
        return loose[0]
    return ""
