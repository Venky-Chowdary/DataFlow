"""One credential finding for a sign-in the database rejected (D436).

A connection whose token expired (Snowflake 394401), whose password is wrong
or whose key was refused reads the same on every surface: the connection
test, ``plan_transfer`` as source and as destination. Before this, a plan read
the driver text ("Could not read the source ... Ask me to list tables") or
carried on with an unread destination and raised a mapping review per column.
"""

from __future__ import annotations

from typing import Any


def credential_finding(detail: Any, *, engine: str = "", auth_mode: str = "") -> str:
    """The plain finding and its one action when ``detail`` is a rejected sign-in, else ``""``."""
    text = str(detail or "").strip()
    if not text:
        return ""
    from services.error_taxonomy import classify

    classified = classify(text, engine=engine, phase="connect", fallback=False)
    if classified is None or not classified.code.startswith("auth."):
        return ""
    if (engine or "").strip().lower() == "snowflake":
        from connectors.snowflake_conn import classify_snowflake_connect_error

        specific = classify_snowflake_connect_error(text, auth_mode=auth_mode)
        if specific:
            return specific
    return classified.text
