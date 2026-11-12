"""Gate-8 fingerprint rules — the versioned definition of "the same value".

A cell fingerprint is canonical text for one value under the *destination*
column's declared type; a row fingerprint is an injective encoding of its
cells. Two populations are reported equal when their multisets of row
fingerprints are equal, so every fold in here is a claim that two stored values
are the same value. A wrong fold reports different data as verified.

Intended equality per declared destination type (version 2)
------------------------------------------------------------
* **Character types** (CHAR, VARCHAR, NCHAR, NVARCHAR, VARCHAR2, TEXT, NTEXT,
  CITEXT, CLOB, STRING, ENUM, SET, NAME ... — decided from the logical type,
  see :func:`is_text_family`): exact text. No number, boolean, date or JSON
  re-reading: ``'01234'`` is not ``'1234'``, ``'true'`` is not ``'1'``. The
  folds that remain are the destination's own equality — blank padding of a
  fixed width CHAR, a declared case / accent / width / kana insensitive
  collation — and two carrier rules: a valid JSON document compares as the
  document (key order and insignificant whitespace are not the value, number
  scale is), because a character column is the JSON carrier on engines
  without a JSON type and the two Gate-8 sides can be steered by the declared
  JSON type and the physical text type of one column; and SQLite TEXT is that
  engine's temporal carrier (it has no temporal storage class), so the two
  separator spellings of one instant are equal there, and only there. A typed
  value headed for a character column is compared as the wire text the writer
  binds for it (``cell_to_string``). One carrier rule depends on the source:
  a character column whose source column is declared a boolean is fingerprinted
  under ``<type> CARRYING SOURCE_BOOLEAN`` (:func:`mark_typed_text_carriers`), where a
  bool equals the ``'1'`` / ``'true'`` (``'0'`` / ``'false'``) a writer stores
  for it; one whose source column is declared a datetime is fingerprinted under
  ``<type> CARRYING SOURCE_INSTANT``, where the stored text compares as the
  instant it spells (either separator, the fraction the writer rendered).
  Text that was text stays exact.
* **Exact numbers** (NUMERIC / DECIMAL / NUMBER, with or without a scale, and
  integers): by numeric value, every significant digit kept. Trailing
  fractional zeros are not significant (``1.50`` equals ``1.5`` — the engine's
  own ``=``), so ``NUMERIC(p,s)`` compares at scale ``s`` on both sides. No
  rounding to 15 digits: that is a property of binary doubles, not decimals.
* **Binary floats** (REAL / FLOAT / DOUBLE): 15 significant digits for a
  double, the float32 round-trip for single precision. A float is approximate
  by declaration; this is the one place residue is folded.
* **Non-finite numbers**: ``Infinity``, ``-Infinity`` and ``NaN`` are three
  distinct values, each with one token whatever the driver spelled.
* **SQL NULL**: out of band. Its token starts with a NUL byte and no text can
  fingerprint to it — text that begins with NUL is escaped (:func:`text_cell`).
* **Bytes**: the bytes themselves (base64 of the raw payload). Never a guess
  that bytes "look like" base64 text.
* **Unknown / unrecognised type names** (a domain, an extension type): the
  value-level compare of an untyped cell, as before. Promoting an unknown name
  to exact text would fail a faithful load of a numeric domain.

Row encoding: ``label=<len>:<cell>`` parts joined by ``\\x1f``, with the label
escaped, so no cell or column name can imitate a boundary.

Version history
---------------
* 1 — rules before the algorithm audit (no stamp on the stored record).
* 2 — the rules above. Digests of the two versions are not comparable: a
  stored digest is only ever compared with a digest of the same version (see
  :func:`stored_fingerprint_version` and the append continuity re-baseline).
"""

from __future__ import annotations

import re
from decimal import Decimal
from functools import lru_cache
from typing import Any, Final

#: Version of the rules in this module. Bump on any change to what fingerprints
#: equal; every stored digest carries the version it was computed under.
FINGERPRINT_VERSION: Final[int] = 2
#: Records written before the stamp existed.
LEGACY_FINGERPRINT_VERSION: Final[int] = 1
#: Key of the stamp on a stored record (reconciliation report, baseline, pack).
VERSION_KEY: Final[str] = "fingerprint_version"

NULL_TOKEN: Final[str] = "\x00NULL\x00"
_TEXT_ESCAPE: Final[str] = "\x00TEXT\x00"

POS_INFINITY: Final[str] = "Infinity"
NEG_INFINITY: Final[str] = "-Infinity"
NAN: Final[str] = "NaN"

#: Spellings a driver, a file or an engine uses for the three non-finite values.
_NONFINITE_SPELLINGS: Final[dict[str, str]] = {
    "inf": POS_INFINITY,
    "+inf": POS_INFINITY,
    "infinity": POS_INFINITY,
    "+infinity": POS_INFINITY,
    "-inf": NEG_INFINITY,
    "-infinity": NEG_INFINITY,
    "nan": NAN,
}

_PART_SEP: Final[str] = "\x1f"
_LABEL_UNSAFE_RE: Final[re.Pattern[str]] = re.compile(r"[%=\x1f]")
_LABEL_ESCAPES: Final[dict[str, str]] = {"%": "%25", "=": "%3D", "\x1f": "%1F"}

# A recognised character type name. ``normalize_logical_type`` answers "string"
# for every name it does not know, so the logical type alone would promote a
# domain or an extension type to exact text; the name must also be a character
# type an engine actually has.
_CHARACTER_TYPE_RE: Final[re.Pattern[str]] = re.compile(
    r"^(?:NATIONAL\s+)?(?:"
    r"N?(?:VAR)?CHAR2?|CHARACTER|BPCHAR|LONG\s+N?VARCHAR|"
    r"N?TEXT|CITEXT|TINYTEXT|MEDIUMTEXT|LONGTEXT|N?CLOB|"
    r"STRING|UTF8|LARGEUTF8|LARGE_STRING|ENUM|SET|NAME|SYSNAME|KEYWORD"
    r")(?![A-Za-z0-9_])",
    re.I,
)
_WRAPPED_TYPE_RE: Final[re.Pattern[str]] = re.compile(
    r"^(?:Nullable|LowCardinality)\((.+)\)$", re.I
)


def stored_fingerprint_version(record: Any) -> int:
    """Rules version a stored record's digests were computed under.

    A record without the stamp predates it and is version 1; so is anything
    that is not a record at all.
    """
    if not isinstance(record, dict):
        return LEGACY_FINGERPRINT_VERSION
    try:
        version = int(record.get(VERSION_KEY) or 0)
    except (TypeError, ValueError):
        version = 0
    return version if version > 0 else LEGACY_FINGERPRINT_VERSION


def is_current_version(record: Any) -> bool:
    """Whether a stored record's digests can be compared with a fresh digest."""
    return stored_fingerprint_version(record) == FINGERPRINT_VERSION


@lru_cache(maxsize=8192)
def is_text_family(ddl_type: str) -> bool:
    """True for a declared character column — the cell is text and nothing else."""
    text = str(ddl_type or "").strip()
    if not text:
        return False
    wrapped = _WRAPPED_TYPE_RE.match(text)
    while wrapped:
        text = wrapped.group(1).strip()
        wrapped = _WRAPPED_TYPE_RE.match(text)
    if not _CHARACTER_TYPE_RE.match(text):
        return False
    from services.type_system import normalize_logical_type

    return normalize_logical_type(text) in {"string", "text"}


@lru_cache(maxsize=8192)
def is_exact_decimal_carrier(ddl_type: str) -> bool:
    """True for NUMERIC / DECIMAL / NUMBER and integers: digits are exact."""
    text = str(ddl_type or "").strip()
    if not text:
        return False
    from services.type_system import normalize_logical_type

    return normalize_logical_type(text) in {"decimal", "integer"}


def nonfinite_token(value: Any) -> str | None:
    """The token of a non-finite float / Decimal / spelling, else ``None``."""
    if isinstance(value, float):
        if value != value:
            return NAN
        if value in (float("inf"), float("-inf")):
            return POS_INFINITY if value > 0 else NEG_INFINITY
        return None
    if isinstance(value, Decimal):
        if value.is_nan():
            return NAN
        if value.is_infinite():
            return POS_INFINITY if value > 0 else NEG_INFINITY
        return None
    if isinstance(value, str) and len(value) <= 12:
        return _NONFINITE_SPELLINGS.get(value.strip().lower())
    return None


#: A character column carrying a typed source boolean / datetime (module notes).
CARRIES_BOOLEAN: Final[str] = "SOURCE_BOOLEAN"
CARRIES_INSTANT: Final[str] = "SOURCE_INSTANT"
_CARRIED_KINDS: Final[dict[str, str]] = {
    "boolean": CARRIES_BOOLEAN,
    "datetime": CARRIES_INSTANT,
}
_CARRYING_RE: Final[re.Pattern[str]] = re.compile(
    r"^(?P<physical>.*\S)\s+CARRYING\s+(?P<kind>[A-Z_]+)$"
)
#: Text a writer stores for a typed boolean bound into a character column.
BOOL_STORED_TEXT: Final[dict[str, str]] = {"1": "1", "true": "1", "0": "0", "false": "0"}


def carrying(physical: str, kind: str) -> str:
    """The fingerprint type of a character column carrying a typed source ``kind``."""
    return f"{physical} CARRYING {kind}"


def split_carried(ddl_type: str) -> tuple[str, str]:
    """``(physical type, carried kind)``; the kind is ``''`` for any other type."""
    text = str(ddl_type or "")
    if "CARRYING" not in text:
        return text, ""
    match = _CARRYING_RE.match(text.strip())
    if not match or match.group("kind") not in _CARRIED_KINDS.values():
        return text, ""
    return match.group("physical"), match.group("kind")


def mark_typed_text_carriers(
    basis: dict[str, str],
    mappings: list[dict[str, Any]] | None,
    source_catalog: dict[str, str] | None,
) -> dict[str, str]:
    """Mark each character column whose source column is declared a boolean / datetime.

    ``basis`` is the per-column type both Gate-8 sides fingerprint under.
    ``source_catalog`` is the source table's catalog-declared types
    (``services.dest_physical_types.catalog_declared_types``). A mapping's
    ``source_type`` cannot decide this: it is refined from sampled values, so a
    text column holding ``'true'`` reads as BOOLEAN there.
    """
    from services.type_system import normalize_logical_type

    declared = {str(k).lower(): str(v or "") for k, v in (source_catalog or {}).items()}
    out = dict(basis)
    if not declared:
        return out
    for mapping in mappings or []:
        if not isinstance(mapping, dict):
            continue
        target = str(mapping.get("target") or "")
        ddl = str(out.get(target) or "")
        if not ddl or not is_text_family(ddl):
            continue
        source_ddl = declared.get(str(mapping.get("source") or "").lower(), "")
        kind = _CARRIED_KINDS.get(normalize_logical_type(source_ddl)) if source_ddl else None
        if kind:
            out[target] = carrying(ddl, kind)
    return out


def text_cell(text: str) -> str:
    """A text cell's fingerprint — never the NULL token.

    Only text that begins with a NUL byte is escaped, so every ordinary cell is
    its own fingerprint and the mapping stays injective.
    """
    if text[:1] == "\x00":
        return _TEXT_ESCAPE + text
    return text


def row_label(column: str) -> str:
    """Lower-cased column label with the encoding's own marks escaped."""
    label = column.lower()
    if _LABEL_UNSAFE_RE.search(label):
        return _LABEL_UNSAFE_RE.sub(lambda m: _LABEL_ESCAPES[m.group(0)], label)
    return label


def row_part(label: str, cell: str) -> str:
    """One ``label=<len>:<cell>`` part; ``label`` comes from :func:`row_label`."""
    return f"{label}={len(cell)}:{cell}"


def iter_row_parts(fingerprint: str):
    """Yield ``(label, part)`` for each part of a row fingerprint.

    Reads the length prefix, so a cell holding the separator stays one part.
    Stops at the first part that is not ``label=<len>:<cell>``.
    """
    pos = 0
    end = len(fingerprint)
    while pos < end:
        eq = fingerprint.find("=", pos)
        if eq <= pos:
            return
        colon = fingerprint.find(":", eq + 1)
        if colon < 0:
            return
        try:
            size = int(fingerprint[eq + 1 : colon])
        except ValueError:
            return
        stop = colon + 1 + size
        if size < 0 or stop > end:
            return
        yield fingerprint[pos:eq], fingerprint[pos:stop]
        if stop < end and fingerprint[stop] != _PART_SEP:
            return
        pos = stop + 1


def memo_key(value: Any, ddl: str) -> tuple[Any, ...]:
    """Per-scan memo key that never aliases two values that fingerprint apart.

    ``Decimal('1.0') == Decimal('1.00')`` and ``0.0 == -0.0`` hash alike, yet a
    text carrier renders them differently; keyed on the value alone the second
    one met took the first one's fingerprint, so the digest followed scan order.
    """
    kind = type(value)
    if kind is Decimal:
        return (kind, str(value), ddl)
    if kind is float and value == 0.0:
        return (kind, repr(value), ddl)
    return (kind, value, ddl)
