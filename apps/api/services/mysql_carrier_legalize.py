"""Legalise MySQL-only column tokens for a destination that cannot spell them.

MySQL catalogs report types such as ``int(10) unsigned``, ``bigint(20)``,
``tinyint(3) unsigned zerofill``, ``double(5,2)`` and ``year(4)``. Every one of
those is a syntax error on PostgreSQL (and on most other engines), yet the
type system used to treat a typmod-bearing carrier as an explicit physical
stamp and paste it into ``CREATE TABLE`` verbatim. On PostgreSQL the CREATE
failed, and the statements after it in the same transaction failed with
"current transaction is aborted" (QA round 11, MariaDB/MySQL → PG). The
introspect spelling ``INT4 UNSIGNED`` fell through every mapping to ``TEXT``
instead: a silent text carrier for an integer key.

This module is the single place that turns such a carrier into a portable,
signed carrier that holds every source value:

* UNSIGNED integers widen to the next signed width (``TINYINT UNSIGNED`` →
  ``INT2``, ``SMALLINT UNSIGNED``/``MEDIUMINT UNSIGNED`` → ``INT4``,
  ``INT UNSIGNED`` → ``INT8``) and ``BIGINT UNSIGNED`` becomes
  ``DECIMAL(20,0)`` (18446744073709551615 has 20 digits).
* Display widths and ``ZEROFILL`` are presentation, not range, and are dropped
  (``int(11)`` → ``INT4``, ``bigint(20)`` → ``INT8``).
* ``YEAR`` / ``YEAR(4)`` → ``INT2`` (0 and 1901–2155).
* ``DECIMAL(p,s) UNSIGNED`` → ``DECIMAL(p,s)``; ``FLOAT``/``DOUBLE`` with
  MySQL's ``(M,D)`` or ``UNSIGNED`` → ``FLOAT4``/``FLOAT8``.

``BIT``, ``ENUM`` and ``SET`` are not rewritten here: ``ddl_type`` already
gives them a destination carrier (BOOLEAN / BIT(n), a PG enum, TEXT[]).

The destination then maps the canonical carrier through ``ddl_type`` as usual,
so the answer is per dialect without a new list of engines. A MySQL-family
destination (resolved through ``resolve_driver_type``) spells these tokens
natively and is left alone.
"""

from __future__ import annotations

import re
from functools import lru_cache

_INT_BITS: dict[str, int] = {
    "TINYINT": 8,
    "INT1": 8,
    "SMALLINT": 16,
    "INT2": 16,
    "MEDIUMINT": 24,
    "MIDDLEINT": 24,
    "INT3": 24,
    "INT": 32,
    "INTEGER": 32,
    "INT4": 32,
    "BIGINT": 64,
    "INT8": 64,
}
#: Base tokens no SQL standard engine but MySQL spells.
_MYSQL_ONLY_INT_BASES = frozenset({"MEDIUMINT", "MIDDLEINT", "INT1", "INT3"})

_INT_RE = re.compile(
    r"^(TINYINT|SMALLINT|MEDIUMINT|MIDDLEINT|INTEGER|INT[1-8]?|BIGINT)"
    r"\s*(\(\s*\d+\s*\))?"
    r"((?:\s+(?:UNSIGNED|SIGNED|ZEROFILL))*)\s*$",
    re.IGNORECASE,
)
_DEC_RE = re.compile(
    r"^(DECIMAL|NUMERIC|DEC|FIXED)"
    r"\s*(?:\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\))?"
    r"((?:\s+(?:UNSIGNED|SIGNED|ZEROFILL))*)\s*$",
    re.IGNORECASE,
)
_FLOAT_RE = re.compile(
    r"^(FLOAT|DOUBLE\s+PRECISION|DOUBLE|REAL)"
    r"\s*(?:\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\))?"
    r"((?:\s+(?:UNSIGNED|SIGNED|ZEROFILL))*)\s*$",
    re.IGNORECASE,
)
_YEAR_RE = re.compile(r"^YEAR\s*(?:\(\s*[24]\s*\))?\s*$", re.IGNORECASE)


def _modifiers(text: str) -> set[str]:
    return {m.upper() for m in (text or "").split()}


def mysql_signed_carrier(carrier: str | None) -> str | None:
    """Portable signed carrier for a MySQL-only integer/numeric/YEAR token.

    ``None`` when ``carrier`` holds no MySQL-only token (plain ``BIGINT``,
    ``DECIMAL(10,2)``, ``TINYINT(1)`` boolean, ``BIT``, ``ENUM`` …), so callers
    keep their existing mapping for everything else.
    """
    from services.type_system import strip_identity_qualifier

    raw = strip_identity_qualifier(carrier or "").strip()
    if not raw:
        return None
    m = _INT_RE.match(raw)
    if m:
        base = m.group(1).upper()
        width_spec = m.group(2) or ""
        mods = _modifiers(m.group(3))
        unsigned = "UNSIGNED" in mods
        if base == "TINYINT" and not unsigned and re.fullmatch(r"\(\s*1\s*\)", width_spec):
            # TINYINT(1) is MySQL's BOOLEAN; ddl_type owns that convention.
            return None
        if not (unsigned or mods or width_spec or base in _MYSQL_ONLY_INT_BASES):
            return None
        bits = _INT_BITS[base]
        if unsigned:
            return {8: "INT2", 16: "INT4", 24: "INT4", 32: "INT8"}.get(bits, "DECIMAL(20,0)")
        return {8: "INT2", 16: "INT2", 24: "INT4", 32: "INT4"}.get(bits, "INT8")
    m = _DEC_RE.match(raw)
    if m:
        mods = _modifiers(m.group(4))
        if not mods and m.group(1).upper() != "FIXED":
            return None
        precision = int(m.group(2) or 10)
        scale = int(m.group(3) or 0)
        return f"DECIMAL({precision},{scale})"
    m = _FLOAT_RE.match(raw)
    if m:
        base = re.sub(r"\s+", " ", m.group(1).upper())
        mods = _modifiers(m.group(4))
        has_md = m.group(3) is not None
        if not mods and not has_md and not (base == "FLOAT" and m.group(2)):
            return None
        if base == "FLOAT":
            if m.group(2) and not has_md and int(m.group(2)) > 24:
                return "FLOAT8"
            return "FLOAT4"
        return "FLOAT8"
    if _YEAR_RE.match(raw):
        return "INT2"
    return None


@lru_cache(maxsize=256)
def _dest_spells_mysql_tokens(db_type: str) -> bool:
    from src.transfer.connector_capabilities import resolve_driver_type

    db = (db_type or "").strip().lower()
    if not db:
        # Unknown destination: no dialect to legalise for.
        return True
    return resolve_driver_type(db) == resolve_driver_type("mysql")


def legal_carrier_for_dest(db_type: str | None, carrier: str | None) -> str | None:
    """Canonical signed carrier to map instead of ``carrier`` on ``db_type``.

    ``None`` when the destination is MySQL-family (the token is native there)
    or the carrier has no MySQL-only token.
    """
    if _dest_spells_mysql_tokens(str(db_type or "")):
        return None
    return mysql_signed_carrier(carrier)


def _decimal_capacity(carrier: str) -> tuple[int, int] | None:
    m = re.match(r"^(?:DECIMAL|NUMERIC|NUMBER|BIGNUMERIC)\s*\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\)", carrier.strip(), re.I)
    if not m:
        return None
    return int(m.group(1)), int(m.group(2) or 0)


def legal_widen_holds_source(
    source_type: str | None, target_type: str | None, *, dest_db: str = ""
) -> bool:
    """True when ``target_type`` is a dialect-legal carrier holding every value.

    A destination that cannot spell ``UNSIGNED`` or ``YEAR`` has no narrower
    faithful carrier than the next wider signed type: that widening keeps every
    source value, so it is not a fidelity collapse. Without this the type
    system's own create-new answer (``TINYINT UNSIGNED`` → ``SMALLINT``,
    ``BIGINT UNSIGNED`` → ``NUMERIC(20,0)``, ``YEAR`` → ``SMALLINT``) was
    reported as lossy and blocked the run. Fail closed when the destination is
    unknown or MySQL-family (it could keep the source domain).
    """
    from services.type_system import (
        LOGICAL_DECIMAL,
        LOGICAL_INTEGER,
        integer_bit_width,
        normalize_logical_type,
        strip_identity_qualifier,
    )

    if not dest_db:
        return False
    canonical = legal_carrier_for_dest(dest_db, source_type)
    if not canonical:
        return False
    target = strip_identity_qualifier(target_type or "").strip()
    if not target:
        return False
    tgt_logical = normalize_logical_type(target)
    canon_logical = normalize_logical_type(canonical)
    if canon_logical == LOGICAL_INTEGER:
        need = integer_bit_width(canonical)
        if tgt_logical == LOGICAL_INTEGER:
            have = integer_bit_width(target)
            if have is None:
                # Bare INTEGER/INT is 32-bit on every SQL dialect that has it.
                have = 32 if target.upper() in {"INTEGER", "INT"} else None
            return have is not None and need is not None and have >= need
        if tgt_logical == LOGICAL_DECIMAL:
            cap = _decimal_capacity(target)
            digits = {16: 5, 32: 10, 64: 19}.get(int(need or 64), 19)
            return cap is not None and cap[0] - cap[1] >= digits
        return False
    if canon_logical == LOGICAL_DECIMAL:
        need_cap = _decimal_capacity(canonical)
        have_cap = _decimal_capacity(target)
        if need_cap is None or have_cap is None:
            return False
        return (
            have_cap[1] >= need_cap[1]
            and have_cap[0] - have_cap[1] >= need_cap[0] - need_cap[1]
        )
    return False
