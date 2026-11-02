"""SSO email-domain routing (P4-6).

Admins claim email domains on their SSO config (``email_domains`` in
integrations_store). The login/signup UI asks this service which IdP an
address belongs to so it can route the user to the right SSO start URL
instead of showing a password box. Claims are admin-managed data — a
domain that isn't claimed simply gets the normal password flow.
"""

from __future__ import annotations

from typing import Any

from services.integrations_store import get_sso_configs, validate_sso_config


def provider_for_email(email: str) -> str | None:
    """SSO type ('saml', 'oidc', 'azure_ad') claiming this domain, or None."""
    domain = (email or "").strip().lower().split("@", 1)[-1]
    if not domain or "@" not in (email or ""):
        return None
    try:
        configs = get_sso_configs()
    except Exception:  # noqa: BLE001 — routing must never break login
        return None
    for sso_type, cfg in configs.items():
        if not cfg.get("enabled"):
            continue
        # Never route a sign-in to a provider that cannot complete it.
        if not validate_sso_config(sso_type).get("ready"):
            continue
        claimed = [str(d).strip().lower()
                   for d in (cfg.get("email_domains") or [])]
        if domain in claimed:
            return sso_type
    return None


def route_for_email(email: str) -> dict[str, Any]:
    """Routing hint for the client: provider + start URL, or sso: null."""
    provider = provider_for_email(email)
    if provider is None:
        return {"sso": None}
    return {"sso": provider,
            "start_url": f"/api/v1/auth/sso/{provider}/start"}
