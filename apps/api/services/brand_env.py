"""Brand-aware environment reads for the Datawrap cutover.

Prefer ``DATAWRAP_*``. Fall back to ``DATAFLOW_*`` so Railway / existing
deploys keep working until operators rename variables. Never invent defaults
when the caller passed none — unset stays unset.
"""

from __future__ import annotations

import os
from typing import Optional


def _suffix(name: str) -> str:
    raw = (name or "").strip()
    if raw.startswith("DATAWRAP_"):
        return raw[len("DATAWRAP_") :]
    if raw.startswith("DATAFLOW_"):
        return raw[len("DATAFLOW_") :]
    return raw


def getenv_brand(name: str, default: Optional[str] = None) -> Optional[str]:
    """Read ``DATAWRAP_{suffix}`` then ``DATAFLOW_{suffix}``.

    ``name`` may be a full ``DATAFLOW_AUTH_SECRET`` / ``DATAWRAP_AUTH_SECRET``
    or a bare suffix like ``AUTH_SECRET``.
    """
    suffix = _suffix(name)
    if not suffix:
        return default
    wrap = os.environ.get(f"DATAWRAP_{suffix}")
    if wrap is not None:
        return wrap
    legacy = os.environ.get(f"DATAFLOW_{suffix}")
    if legacy is not None:
        return legacy
    return default


def getenv_brand_str(name: str, default: str = "") -> str:
    """String helper — never returns None."""
    val = getenv_brand(name, default)
    return default if val is None else val
