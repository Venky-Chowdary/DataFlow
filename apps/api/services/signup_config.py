"""Self-serve signup feature flag and shared tunables.

Every route and page belonging to self-serve signup is gated by
``DATAWRAP_SIGNUP_ENABLED`` (``DATAFLOW_SIGNUP_ENABLED`` legacy fallback),
default OFF. With the flag unset the new router answers 404 and the rest of
the platform is byte-for-byte what it was before this module existed, so the
feature can merge to the deploy branch without changing production.

All reads are live (``getenv_brand`` at call time) — an operator can kill the
feature without a code deploy, matching the one-switch-kill rollout plan.
"""

from __future__ import annotations

from services.brand_env import getenv_brand

_TRUE = ("1", "true", "yes", "on")


def signup_enabled() -> bool:
    """Master switch for every self-serve signup route, page and worker."""
    return (getenv_brand("SIGNUP_ENABLED", "0") or "0").strip().lower() in _TRUE


def turnstile_site_key() -> str:
    """Cloudflare Turnstile site key — public by design, safe to hand the SPA."""
    return (getenv_brand("TURNSTILE_SITE_KEY", "") or "").strip()
