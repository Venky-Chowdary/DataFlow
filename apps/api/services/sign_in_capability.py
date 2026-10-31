"""Whether an email can actually sign in to this deployment today.

Last-admin protection must count only administrators who can log in: an
invitation creates a membership, not an account, so an invited admin with no
login is no help to a workspace whose real admin was just demoted (QA
enterprise #25 — ``qa-e2@example.com`` was counted and the owner's demotion
passed). An email can sign in when one of these holds:

- it resolves through ``auth_service.lookup_user`` (environment-provisioned
  admin/users, or a stored active account with a password);
- it has a stored active account and an SSO provider is enabled;
- SSO auto-provisioning is on, an SSO provider is enabled, and its domain is
  allowed (no domain list = every domain, as ``sso_authorization`` admits).

A stored account that is disabled or banned can never sign in, even if the
environment also names it.
"""

from __future__ import annotations

from typing import Any


def _sso_enabled() -> bool:
    try:
        from services.integrations_store import get_sso_configs

        configs: dict[str, dict[str, Any]] = get_sso_configs()
    except Exception:  # noqa: BLE001 - unreadable SSO config = no SSO
        return False
    return any(bool((cfg or {}).get("enabled")) for cfg in configs.values())


def can_sign_in(email: str) -> bool:
    normalized = (email or "").strip().lower()
    if not normalized:
        return False
    from services.user_store import get_user_record
    from src.services.auth_service import lookup_user

    stored = get_user_record(normalized)
    if stored and str(stored.get("status") or "active") != "active":
        return False
    if lookup_user(normalized) is not None:
        return True
    if not _sso_enabled():
        return False
    if stored is not None:
        return True
    try:
        from services.sso_authorization import _sso_allowed_domains, _sso_auto_provision
    except Exception:  # noqa: BLE001
        return False
    if not _sso_auto_provision():
        return False
    domains = _sso_allowed_domains()
    return not domains or normalized.rsplit("@", 1)[-1] in domains
