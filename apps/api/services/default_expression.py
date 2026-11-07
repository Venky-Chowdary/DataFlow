"""One comparison for a column default expression.

Schema planning and the post-write catalog diff both decide whether two
defaults are the same rule. A second copy would certify ``DEFAULT 'n'`` as
carried when the destination stored ``DEFAULT 'x'``, or would flag a faithful
``CURRENT_TIMESTAMP`` as dropped because the other engine spells it ``now()``.
"""

from __future__ import annotations

import re
from typing import Any

# Clock/boolean default synonyms treated as equivalent across dialects so a
# faithfully-carried default is not falsely downgraded on cosmetic differences.
_CLOCK_DEFAULTS = {
    "current_timestamp", "current_timestamp()", "now()", "now", "getdate()",
    "getutcdate()", "sysdate", "systimestamp", "localtimestamp",
    "localtimestamp()", "statement_timestamp()", "transaction_timestamp()",
    "clock_timestamp()", "sysdatetime()",
}
_TRUE_DEFAULTS = {"true", "t", "1", "b'1'"}
_FALSE_DEFAULTS = {"false", "f", "0", "b'0'"}


def normalize_default_expr(expr: Any) -> str:
    """Fold a catalog/planned default into a comparable literal.

    Iteratively strips wrapping parens, trailing type casts (``'x'::text``,
    ``'x'::character varying``), national/escape/bit/hex string-literal prefixes
    (``N'x'``, ``E'x'``, ``B'1'``), and surrounding quotes to a fixed point — so
    ``('active'::character varying)``, ``N'active'`` and ``active`` all unify —
    then collapses clock precision (``current_timestamp(6)`` -> ``()``) and
    casefolds.
    """
    s = str(expr if expr is not None else "").strip()
    prev: str | None = None
    while s and s != prev:
        prev = s
        if len(s) >= 2 and s[0] == "(" and s[-1] == ")":
            s = s[1:-1].strip()
            continue
        stripped_cast = re.sub(r"::\s*[A-Za-z0-9_ \"\.\[\]]+\s*$", "", s).strip()
        if stripped_cast != s:
            s = stripped_cast
            continue
        prefix = re.match(r"^(?:[NnEeBbXx]|[Uu]&)(['\"].*)$", s)
        if prefix:
            s = prefix.group(1).strip()
            continue
        if len(s) >= 2 and s[0] in "'\"" and s[-1] == s[0]:
            s = s[1:-1].strip()
            continue
    # current_timestamp(6) / localtimestamp(3) → drop precision for clock compare.
    s = re.sub(r"\(\s*\d+\s*\)", "()", s)
    return s.casefold()


def default_exprs_equivalent(left: str, right: str) -> bool:
    """True when two already-normalized defaults are the same rule."""
    if left == right:
        return True
    if left in _CLOCK_DEFAULTS and right in _CLOCK_DEFAULTS:
        return True
    if left in _TRUE_DEFAULTS and right in _TRUE_DEFAULTS:
        return True
    if left in _FALSE_DEFAULTS and right in _FALSE_DEFAULTS:
        return True
    try:
        from decimal import Decimal

        from services.decimal_identity import extract_decimal_identity

        ia = extract_decimal_identity(left)
        ib = extract_decimal_identity(right)
    except ValueError:
        return False
    if ia is None or ib is None:
        return False
    return +Decimal(ia.to_canonical_text()) == +Decimal(ib.to_canonical_text())
