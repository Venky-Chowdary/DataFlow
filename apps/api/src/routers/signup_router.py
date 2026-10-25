"""Self-serve signup routes — every endpoint gated by DATAWRAP_SIGNUP_ENABLED.

The whole router carries one dependency: when the flag is off each route
answers 404, so mounting this router unconditionally cannot change the
flag-off surface. Mounted at ``/api/v1`` (and at the root compat mount,
matching ``auth_router``).
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from services.signup_config import signup_enabled, turnstile_site_key


def _require_signup_enabled() -> None:
    """404 — not 401/403 — so the flag-off surface looks exactly like today."""
    if not signup_enabled():
        raise HTTPException(status_code=404, detail="Not Found")


router = APIRouter(
    prefix="/auth",
    tags=["signup"],
    dependencies=[Depends(_require_signup_enabled)],
)


@router.get("/signup/config")
async def signup_config():
    """Public client config for the signup flow (no secrets)."""
    return {
        "enabled": True,
        "turnstile_site_key": turnstile_site_key(),
    }
