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

#: What the web client's renderer (``apps/web/src/lib/safeMarkdown.ts``)
#: interprets inside a line is ``**bold**`` and `` `code` ``, and it never
#: removes a backslash (review 2, H3). Escaping for GFM ("\\[", "\\*") left
#: "dbo.\\[Order Details\\]" on screen while an escaped backtick still opened
#: a code span. So exactly those two are neutralised, invisibly: a zero-width
#: space between adjacent asterisks, and a backtick read as an apostrophe.
#: Newlines are flattened, so no value can start a line: no bullet, heading,
#: table row or fence. "](" and "![" get the same zero-width space for any
#: client that renders links; on the web it changes nothing visible.
_ZWSP = "\u200b"
_ADJACENT_STARS = re.compile(r"\*(?=\*)")
_LINK_OPENERS = re.compile(r"(?<=\])(?=\()|(?<=!)(?=\[)")


def md_text(value: Any, limit: int = 240) -> str:
    """``value`` as plain text: one line, nothing the renderer would turn into markup."""
    flat = " ".join(str(value if value is not None else "").split())[:limit]
    flat = flat.replace("`", "'")
    flat = _ADJACENT_STARS.sub("*" + _ZWSP, flat)
    return _LINK_OPENERS.sub(_ZWSP, flat)


def md_code(value: Any, limit: int = 120) -> str:
    """``value`` inside a code span it cannot close."""
    flat = " ".join(str(value if value is not None else "").split()).replace("`", "'")
    return f"`{flat[:limit]}`"
