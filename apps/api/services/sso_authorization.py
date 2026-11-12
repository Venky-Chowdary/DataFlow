"""Shared authorization policy for identities returned by SSO providers."""

from __future__ import annotations

import logging

from fastapi import HTTPException

from services.brand_env import getenv_brand
from services import user_store
from src.services.auth_service import lookup_user

logger = logging.getLogger(__name__)


def _sso_allowed_domains() -> set[str]:
    raw = getenv_brand("SSO_ALLOWED_DOMAINS", "").strip()
    if not raw:
        return set()
    return {domain.strip().lower().lstrip("@") for domain in raw.split(",") if domain.strip()}


def _sso_auto_provision() -> bool:
    return getenv_brand("SSO_AUTO_PROVISION", "0").lower() in ("1", "true", "yes")


def warn_sso_auto_provision_without_domains(log: logging.Logger) -> bool:
    """Startup notice: auto-provisioning is on and no domain may be provisioned.

    Since audit M-11 such a deployment refuses every new SSO identity (people
    with an account still sign in). Returns whether it warned.
    """
    if not _sso_auto_provision() or _sso_allowed_domains():
        return False
    log.warning(
        "SSO_AUTO_PROVISION is on but SSO_ALLOWED_DOMAINS is empty: no new SSO user will be "
        "provisioned (existing accounts still sign in). Set SSO_ALLOWED_DOMAINS "
        "(DATAWRAP_SSO_ALLOWED_DOMAINS) to the email domains that may be provisioned."
    )
    return True


def _is_sso_email_allowed(email: str) -> bool:
    normalized = email.strip().lower()
    if lookup_user(normalized):
        return True
    domain = normalized.split("@")[-1] if "@" in normalized else ""
    return bool(domain and domain in _sso_allowed_domains())


def _audit_disabled_sso_user() -> None:
    try:
        from services.audit_log import append_audit_event

        append_audit_event(
            action="auth.sso.failure",
            resource="/auth/sso/authorization",
            level="error",
            details={"provider": "sso", "reason": "account_disabled"},
        )
    except Exception:
        logger.error("SSO failure audit append failed (sso_type=sso reason=audit_write_failed)")


def require_sso_authorization(email: str) -> None:
    """Raise when an SSO identity is disabled or outside the configured policy."""
    stored = user_store.get_user(email)
    if stored and stored.get("status") == "disabled":
        logger.warning("SSO request failed (sso_type=sso issuer= reason=account_disabled)")
        _audit_disabled_sso_user()
        raise HTTPException(status_code=403, detail="account_disabled")
    if _sso_auto_provision():
        allowed_domains = _sso_allowed_domains()
        if not allowed_domains:
            # Fail closed (audit M-11): an empty list is "no domain may be
            # provisioned", not "every identity the provider returns". People
            # who already have an account still sign in.
            if lookup_user(email.strip().lower()):
                return
            logger.warning(
                "SSO request failed (sso_type=sso issuer= reason=auto_provision_without_allowed_domains)"
            )
            raise HTTPException(
                status_code=403,
                detail=(
                    "SSO auto-provisioning is on but no email domain is allowed: an administrator "
                    "must set SSO_ALLOWED_DOMAINS (DATAWRAP_SSO_ALLOWED_DOMAINS) to the domains "
                    "that may be provisioned."
                ),
            )
        if email.split("@")[-1].lower() not in allowed_domains:
            raise HTTPException(
                status_code=403,
                detail="SSO email domain is not on this deployment's allowed SSO domain list",
            )
        return
    if not _is_sso_email_allowed(email):
        raise HTTPException(
            status_code=403,
            detail="SSO user is not authorized for this workspace",
        )
