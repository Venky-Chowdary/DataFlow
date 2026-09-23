"""Name and header normalisation for a customer rule workbook.

The compiler must not invent a column. It only binds a cell to a name the
operator already has on the source or destination, after a conservative
normalisation (case, spaces, underscores). Ambiguous matches stay unbound.
"""

from __future__ import annotations

import re

_SPLIT = re.compile(r"[^a-z0-9]+")
_QUALIFIED = re.compile(
    r"^(?P<table>[A-Za-z_][\w]*)\.(?P<column>[A-Za-z_][\w]*(?:\s+[A-Za-z_][\w]*)*)$"
)

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
    "object": "source_table",
    "sourceobject": "source_table",
    "sourcecolumn": "source_column",
    "sourcecol": "source_column",
    "srccolumn": "source_column",
    "srccol": "source_column",
    "sourcefield": "source_column",
    "srcfield": "source_column",
    "fromcolumn": "source_column",
    "fromfield": "source_column",
    "srcname": "source_column",
    "sourcename": "source_column",
    "column": "source_column",
    "field": "source_column",
    "attribute": "source_column",
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
    "tgtname": "dest_column",
    "destname": "dest_column",
    "targetname": "dest_column",
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
    "instruction": "rule",
    "action": "rule",
    "mapping": "rule",
    "conversion": "rule",
    "rulename": "rule_name",
    "namedrule": "rule_name",
    "ruleid": "rule_name",
    "mapplet": "rule_name",
    "macroname": "rule_name",
    "reusablerule": "rule_name",
    "ruleref": "rule_name",
    "joinfrom": "join_from",
    "joinon": "join_on",
    "jointype": "join_type",
    "leftkey": "join_on",
    "rightkey": "join_on",
    "fromcode": "lookup_from",
    "fromvalue": "lookup_from",
    "sourcecode": "lookup_from",
    "oldvalue": "lookup_from",
    "oldcode": "lookup_from",
    "inbound": "lookup_from",
    "tocode": "lookup_to",
    "tovalue": "lookup_to",
    "destcode": "lookup_to",
    "newvalue": "lookup_to",
    "newcode": "lookup_to",
    "outbound": "lookup_to",
}


def fold(value: str) -> str:
    """Compare-key for a header or a column name."""
    return _SPLIT.sub("", (value or "").strip().lower())


def canonical_header(value: str) -> str:
    """Map a spoken header onto one of the compiler's columns, or ''."""
    return HEADER_ALIASES.get(fold(value), "")


def split_qualified(value: str) -> tuple[str, str]:
    """``Customer.fname`` → (Customer, fname). Unqualified → ('', name)."""
    text = (value or "").strip().strip("[]`\"'")
    match = _QUALIFIED.match(text)
    if match:
        return match.group("table"), match.group("column")
    return "", text


def resolve_name(needle: str, candidates: list[str]) -> str:
    """The one candidate this cell names, or '' when none or several match."""
    name, _method, _score = resolve_name_ex(needle, candidates)
    return name


def resolve_name_ex(needle: str, candidates: list[str]) -> tuple[str, str, float]:
    """Bind a spoken name. Method is exact | prefix | contained | linguistic.

    Cupid-style linguistic matching is last and fail-closed: unique winner,
    threshold, and a score gap. Short tokens (``id``) never bind.
    """
    table, column = split_qualified(needle)
    spoken = column or needle
    want = fold(spoken)
    if not want or not candidates:
        return "", "", 0.0
    exact = [c for c in candidates if fold(c) == want or fold(split_qualified(c)[1]) == want]
    if len(exact) == 1:
        return exact[0], "exact", 1.0
    if len(exact) > 1 or len(want) < 4:
        return "", "", 0.0
    loose = [
        c for c in candidates
        if fold(c).startswith(want) or want.startswith(fold(c))
    ]
    if len(loose) == 1:
        return loose[0], "prefix", 0.9
    if len(want) >= 5:
        contained = [c for c in candidates if want in fold(c) or fold(c) in want]
        if len(contained) == 1:
            return contained[0], "contained", 0.84
    from .match import unique_linguistic_match
    winner, score = unique_linguistic_match(spoken, candidates)
    if winner:
        return winner, "linguistic", score
    _ = table
    return "", "", 0.0
