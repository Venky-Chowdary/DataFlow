"""Audit M-11: SSO auto-provisioning without an allowed-domain list admits no new identity.

``SSO_AUTO_PROVISION=1`` with an empty ``SSO_ALLOWED_DOMAINS`` provisioned
*any* identity the identity provider returned: a shared IdP (a public OIDC
provider, a multi-tenant directory) could mint itself an account. Empty now
means "no domain is allowed": only people who already have an account sign in,
and the refusal says which setting is missing.
"""

from __future__ import annotations

import pytest
from fastapi import HTTPException


def _flags(monkeypatch, *, auto: str, domains: str | None) -> None:
    for prefix in ("DATAWRAP_", "DATAFLOW_"):
        monkeypatch.delenv(prefix + "SSO_AUTO_PROVISION", raising=False)
        monkeypatch.delenv(prefix + "SSO_ALLOWED_DOMAINS", raising=False)
    monkeypatch.setenv("DATAWRAP_SSO_AUTO_PROVISION", auto)
    if domains is not None:
        monkeypatch.setenv("DATAWRAP_SSO_ALLOWED_DOMAINS", domains)


@pytest.fixture
def known_user(monkeypatch):
    """One existing account; everyone else is a stranger to this deployment."""
    from services import sso_authorization, user_store

    known = {"email": "member@corp.example", "role": "member"}
    monkeypatch.setattr(user_store, "get_user", lambda email: None)
    monkeypatch.setattr(
        sso_authorization, "lookup_user", lambda email: known if email == known["email"] else None
    )
    return known["email"]


def test_auto_provision_without_a_domain_list_admits_no_new_identity(monkeypatch, known_user):
    from services.sso_authorization import require_sso_authorization

    for domains in (None, "", "  ", " , "):
        _flags(monkeypatch, auto="1", domains=domains)
        with pytest.raises(HTTPException) as refused:
            require_sso_authorization("stranger@anywhere.example")
        assert refused.value.status_code == 403
        assert "SSO_ALLOWED_DOMAINS" in str(refused.value.detail)
        # Someone who already has an account still signs in.
        require_sso_authorization(known_user)


def test_auto_provision_with_a_domain_list_is_unchanged(monkeypatch, known_user):
    from services.sso_authorization import require_sso_authorization

    _flags(monkeypatch, auto="1", domains="corp.example, partner.example")
    require_sso_authorization("new.hire@corp.example")
    require_sso_authorization("someone@PARTNER.example")
    with pytest.raises(HTTPException) as refused:
        require_sso_authorization("stranger@anywhere.example")
    assert refused.value.status_code == 403


def test_without_auto_provision_only_known_accounts_and_listed_domains(monkeypatch, known_user):
    from services.sso_authorization import require_sso_authorization

    _flags(monkeypatch, auto="0", domains=None)
    require_sso_authorization(known_user)
    with pytest.raises(HTTPException):
        require_sso_authorization("stranger@anywhere.example")


def test_sign_in_capability_does_not_promise_what_authorization_refuses(monkeypatch):
    """The "can this email sign in?" answer (invites, password reset) uses the same rule."""
    from services import sign_in_capability

    monkeypatch.setattr(sign_in_capability, "_sso_enabled", lambda: True)
    monkeypatch.setattr("services.user_store.get_user_record", lambda email: None)
    monkeypatch.setattr("src.services.auth_service.lookup_user", lambda email: None)
    reader = sign_in_capability.can_sign_in
    _flags(monkeypatch, auto="1", domains=None)
    assert reader("stranger@anywhere.example") is False
    _flags(monkeypatch, auto="1", domains="corp.example")
    assert reader("new.hire@corp.example") is True
    assert reader("stranger@anywhere.example") is False
