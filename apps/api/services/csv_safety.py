"""Spreadsheet formula-injection guard for CSV/TSV/XLSX exports.

OWASP: a cell whose text starts with ``=``, ``+``, ``-``, ``@``, a tab, or
a CR is executed as a formula when the file is opened in Excel/Sheets.
Neutralize by prefixing a single quote — the cell then renders as text.
Mirrors ``classify.formula_prefix`` (minus the bullet exception: on export
we cannot tell prose from injection, so ``-``-leading text is quoted too).

Which export gets which guard (G7, CWE-1236) — decided per path:

* Operator reports (redacted samples, quarantine CSV): always
  ``neutralize_formula_cell``.
* Human downloads produced on request — the audit CSV export (always), the
  Query console's CSV/TSV file export and the email destination's CSV/TSV
  attachment (``formula_protection`` defaults to ``"prefix"``, ``"off"`` opts
  out): ``neutralize_export_cell``, header names included.
* Data-plane output — a transfer job's ``file_export`` destination, wherever
  it lands (exports folder, export bucket, the presigned/``/transfer/download``
  link that serves those stored bytes) — is data for machine consumption:
  Gate-8 re-reads it and downstream loads ingest it. It stays byte-exact
  (``formula_protection`` defaults to ``"off"``) unless the destination opts
  in with ``"prefix"``; Gate-8 then reads it back with ``restore_export_cell``.
  Database and object-store writers (S3/GCS/ADLS/SFTP) never apply a guard:
  a ``'`` prefix silently mutates delivered values like ``=A1``.

XLSX exports need no prefix: ``services.xlsx_cells`` writes text as explicit
string cells, never formulas.
"""

from __future__ import annotations

import re

_FORMULA_PREFIXES = ("=", "+", "-", "@", "\t", "\r")

# A plain numeral (``-5``, ``-1.5``, ``+3``, ``-1e-3``) opens as a number, never a
# formula: export guards leave it intact so negative numbers are not mangled.
_NUMERIC_LITERAL = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$")

FORMULA_PROTECTION_MODES = ("off", "prefix")


def neutralize_formula_cell(value: object) -> str:
    """Return the cell as text, ``'``-prefixed when it would open as a formula."""
    text = "" if value is None else str(value)
    if text and text[0] in _FORMULA_PREFIXES:
        return "'" + text
    return text


def _export_cell_needs_guard(text: str) -> bool:
    core = text.lstrip("'")
    return bool(core) and core[0] in _FORMULA_PREFIXES and not _NUMERIC_LITERAL.match(core)


def neutralize_export_cell(value: object) -> str:
    """One CSV/TSV export cell, ``'``-prefixed when it would open as a formula.

    Numerals are exempt (``-5`` stays a number). Text that already starts with
    quotes before a formula leader (``'=x``) gains one more quote, so the
    guard is reversible with ``restore_export_cell``.
    """
    text = "" if value is None else str(value)
    return "'" + text if _export_cell_needs_guard(text) else text


def restore_export_cell(text: str) -> str:
    """Inverse of ``neutralize_export_cell``: the exact value that was exported."""
    if text[:1] == "'" and _export_cell_needs_guard(text[1:]):
        return text[1:]
    return text


def formula_protection_mode(value: object, *, default: str = "off") -> str:
    """``"off"`` or ``"prefix"``; empty means ``default``. Anything else is refused."""
    text = "" if value is None else str(value).strip().lower()
    if not text:
        return default
    if text not in FORMULA_PROTECTION_MODES:
        raise ValueError(
            f"formula_protection must be one of {', '.join(FORMULA_PROTECTION_MODES)}; got {value!r}"
        )
    return text
