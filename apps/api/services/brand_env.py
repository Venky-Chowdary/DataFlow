"""Brand-aware environment reads for the Datawrap cutover.

Prefer ``DATAWRAP_*``. Fall back to ``DATAFLOW_*`` so Railway / existing
deploys keep working until operators rename variables. Never invent defaults
when the caller passed none — unset stays unset.

An empty (or blank) ``DATAWRAP_X`` counts as unset when ``DATAFLOW_X`` has a
value: a blank line left in an environment block must not switch off a
setting the legacy name turns on (security review). When both names hold
different values the new name wins, and the API says so at startup
(:func:`warn_brand_flag_conflicts`), naming the flag and never a value.
"""

from __future__ import annotations

import os
from typing import Any, Optional


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
    legacy = os.environ.get(f"DATAFLOW_{suffix}")
    if wrap is not None and (wrap.strip() or legacy is None or not legacy.strip()):
        return wrap
    if legacy is not None:
        return legacy
    return default


def brand_flag_conflicts() -> list[str]:
    """Suffixes set under both names to different (non-empty) values."""
    found = []
    for name, wrap in os.environ.items():
        if not name.startswith("DATAWRAP_"):
            continue
        suffix = name[len("DATAWRAP_") :]
        legacy = os.environ.get(f"DATAFLOW_{suffix}")
        if legacy is not None and wrap.strip() and legacy.strip() and wrap.strip() != legacy.strip():
            found.append(suffix)
    return sorted(found)


def warn_brand_flag_conflicts(logger: Any) -> list[str]:
    """Log one startup warning per flag set differently under both names.

    Names the flag only: either value may be a secret.
    """
    conflicts = brand_flag_conflicts()
    for suffix in conflicts:
        logger.warning(
            "DATAWRAP_%s and DATAFLOW_%s are both set, to different values: DATAWRAP_%s is used. "
            "Remove the one you do not mean.",
            suffix, suffix, suffix,
        )
    return conflicts


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
