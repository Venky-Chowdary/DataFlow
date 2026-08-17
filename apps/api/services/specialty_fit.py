"""Specialty-carrier domain polarity SSOT (ObjectId and friends).

Split out of ``services.type_system`` (already over its module-size budget).
These answer *domain polarity* questions — "does the destination still enforce
the source's domain?" — which are distinct from value-loss questions answered
by ``specialty_carrier_would_collapse``.

Mongo ``ObjectId`` → unbounded ``TEXT``/``STRING``/``VARCHAR(MAX)`` keeps the
24-char hex value, so it is not a value collapse, but the destination no longer
enforces the ObjectId domain: any 40-char string can be written into that column
afterwards, joins against other ObjectId columns stop being type-checked, and
round-tripping back to Mongo needs a cast the catalog no longer proves. That is
a risk an operator must accept explicitly (Migration Risk Contract), exactly as
UUID→STRING already does. Width-pinned hex wires (``CHAR(24)``/``VARCHAR(24)``)
and ``BINARY(12)`` pin the domain and are not polarity losses.

Type-system imports are deferred inside the functions: ``type_system``
re-exports these names, so a module-level import would be circular.
"""

from __future__ import annotations

import re

_OBJECTID_EXACT_BINARY_WIRE = frozenset({"BINARY(12)", "VARBINARY(12)", "BYTES(12)"})

# Width-pinned character wires that still pin the 24-char hex domain.
_PINNED_CHAR_WIDTH_PATTERNS = (
    r"^(?:N?VAR)?CHAR(?:ACTER)?(?:\s+VARYING)?\s*\(\s*(\d+)\s*\)$",
    r"^VARCHAR2\s*\(\s*(\d+)\s*(?:BYTE|CHAR)?\s*\)$",
    r"^STRING\s*\(\s*(\d+)\s*\)$",
)


def _pinned_char_width(upper: str) -> int | None:
    for pattern in _PINNED_CHAR_WIDTH_PATTERNS:
        m = re.match(pattern, upper)
        if m:
            return int(m.group(1))
    return None


def objectid_text_domain_polarity(source_type: str, target_type: str) -> bool:
    """True when ObjectId lands in an unbounded text carrier (domain not enforced).

    ``False`` for ``BINARY(12)`` / ``CHAR(24)``-class pinned wires (domain kept)
    and for narrow ``VARCHAR(n<24)`` (a *value* collapse already reported by
    :func:`services.type_system.specialty_carrier_would_collapse`).
    """
    from services.type_system import (
        LOGICAL_STRING,
        LOGICAL_TEXT,
        normalize_logical_type,
        specialty_carrier_base,
        strip_identity_qualifier,
    )

    if specialty_carrier_base(source_type) != "OBJECTID":
        return False
    upper = strip_identity_qualifier(target_type).strip().upper()
    if not upper:
        return False
    if upper in _OBJECTID_EXACT_BINARY_WIRE:
        return False
    width = _pinned_char_width(upper)
    if width is not None:
        # ``VARCHAR(MAX)``-style sinks are unbounded and fall through below.
        return False
    if normalize_logical_type(target_type) not in {LOGICAL_STRING, LOGICAL_TEXT}:
        return False
    return True
