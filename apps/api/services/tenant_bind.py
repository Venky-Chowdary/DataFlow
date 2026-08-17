"""Phase D2 — bind Host-derived tenant to authenticated identity.

``Host`` is a routing hint. When it resolves to a tenant, the principal must
claim that tenant (``tenant_id`` / ``tenant_ids`` on the user or API key).
Unclaimed principals are allowed only when bind mode is soft (non-production
default) so single-tenant bootstrap keeps working.
"""

from __future__ import annotations

from typing import Any

from services.brand_env import getenv_brand
from services.platform_config import is_production


def _bind_mode() -> str:
    """Return ``strict`` | ``soft`` | ``off``.

    * ``strict`` — Host tenant requires a matching claim (production default).
    * ``soft`` — enforce only when the principal already has tenant claims.
    * ``off`` — never enforce (legacy / emergency).
    """
    raw = getenv_brand("AUTH_TENANT_BIND", "").strip().lower()
    if raw in ("0", "false", "no", "off"):
        return "off"
    if raw in ("1", "true", "yes", "strict"):
        return "strict"
    if raw in ("soft", "auto"):
        return "soft"
    return "strict" if is_production() else "soft"


def user_tenant_ids(user: dict[str, Any] | None) -> list[str]:
    if not user:
        return []
    out: list[str] = []
    single = str(user.get("tenant_id") or "").strip()
    if single:
        out.append(single)
    raw = user.get("tenant_ids")
    if isinstance(raw, str) and raw.strip():
        out.extend(p.strip() for p in raw.split(",") if p.strip())
    elif isinstance(raw, (list, tuple)):
        for item in raw:
            tid = str(item or "").strip()
            if tid:
                out.append(tid)
    # Dedup preserve order
    seen: set[str] = set()
    ordered: list[str] = []
    for tid in out:
        if tid not in seen:
            seen.add(tid)
            ordered.append(tid)
    return ordered


def principal_allowed_for_tenant(user: dict[str, Any] | None, tenant_id: str | None) -> bool:
    """True when the authenticated principal may act under ``tenant_id``."""
    tid = (tenant_id or "").strip()
    if not tid:
        return True
    mode = _bind_mode()
    if mode == "off":
        return True
    claimed = user_tenant_ids(user)
    if claimed:
        return tid in claimed
    # No claim on identity
    if mode == "strict":
        return False
    return True  # soft
