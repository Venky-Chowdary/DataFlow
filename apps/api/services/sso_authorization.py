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
        if allowed_domains and email.split("@")[-1].lower() not in allowed_domains:
            raise HTTPException(
                status_code=403,
                detail="SSO email domain is not in DATAFLOW_SSO_ALLOWED_DOMAINS",
            )
        return
    if not _is_sso_email_allowed(email):
        raise HTTPException(
            status_code=403,
            detail="SSO user is not authorized for this workspace",
        )
