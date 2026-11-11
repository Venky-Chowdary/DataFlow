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


def setenv_brand(name: str, value: str) -> None:
    """Force a flag for this process under both names.

    For an entrypoint that *is* a role (``python -m src.worker_main``): setting
    only ``DATAFLOW_X`` loses to a ``DATAWRAP_X`` left in a shared environment
    block, because the reader prefers the new name.
    """
    suffix = _suffix(name)
    os.environ[f"DATAWRAP_{suffix}"] = value
    os.environ[f"DATAFLOW_{suffix}"] = value


def getenv_brand_str(name: str, default: str = "") -> str:
    """String helper — never returns None."""
    val = getenv_brand(name, default)
    return default if val is None else val


def getenv_brand_bool(name: str, default: bool = False) -> bool:
    """Boolean flag: ``1/true/yes/on`` enable, ``0/false/no/off`` disable.

    Unset or unrecognised values return ``default``.
    """
    raw = (getenv_brand(name) or "").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default
