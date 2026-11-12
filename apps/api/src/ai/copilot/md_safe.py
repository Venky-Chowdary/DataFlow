"""Render operator-controlled text inert inside Pilot's markdown answers (review M5).

A job's source name, a driver's error text, a pipeline or dataset name is
written by whoever configured it. Interpolated raw, a name could add a bullet
("**completed** (9,999,999 records)"), a heading, a bold count or a link and
image to an answer Pilot vouches for. Everything such a value contains is
kept, but on one line and with markdown's control characters escaped.
"""

from __future__ import annotations

import re
from typing import Any

#: Bold/italic (*), code (`), links and images ([ ] !), HTML (< >), strikethrough
#: (~) and the escape itself. Newlines are flattened, so no value can start a
#: line: no bullet, heading, quote or table row. Intraword ``_`` is left alone
#: (GFM does not emphasise it; escaping it garbled table names like orders_wh).
_CONTROL = re.compile(r"([\\`*\[\]<>!~])")


def md_text(value: Any, limit: int = 240) -> str:
    """``value`` as plain text: one line, markdown control characters escaped."""
    flat = " ".join(str(value if value is not None else "").split())
    return _CONTROL.sub(r"\\\1", flat[:limit])


def md_code(value: Any, limit: int = 120) -> str:
    """``value`` inside a code span it cannot close."""
    flat = " ".join(str(value if value is not None else "").split()).replace("`", "'")
    return f"`{flat[:limit]}`"
