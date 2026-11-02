"""Spreadsheet formula-injection guard for CSV/TSV/XLSX exports.

OWASP: a cell whose text starts with ``=``, ``+``, ``-``, ``@``, a tab, or
a CR is executed as a formula when the file is opened in Excel/Sheets.
Neutralize by prefixing a single quote — the cell then renders as text.
Mirrors ``classify.formula_prefix`` (minus the bullet exception: on export
we cannot tell prose from injection, so ``-``-leading text is quoted too).

Scope: operator-facing reports only — redacted samples and quarantine
reports. Never apply this to customer data-plane output (CSV/TSV/XLSX
destination writes, file-export landings): a ``'`` prefix silently mutates
values like ``-5`` or ``=A1`` in the delivered data.
"""

from __future__ import annotations

_FORMULA_PREFIXES = ("=", "+", "-", "@", "\t", "\r")


def neutralize_formula_cell(value: object) -> str:
    """Return the cell as text, ``'``-prefixed when it would open as a formula."""
    text = "" if value is None else str(value)
    if text and text[0] in _FORMULA_PREFIXES:
        return "'" + text
    return text
