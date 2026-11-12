"""Security review (low): SSO fail-closed is announced at startup.

Audit M-11 made ``SSO_AUTO_PROVISION=1`` with no ``SSO_ALLOWED_DOMAINS``
refuse every new SSO identity (existing accounts still sign in). That is a
breaking change for such a deployment, so the API now says so once at
startup instead of leaving operators to discover it from refused sign-ins.
"""

from __future__ import annotations

import inspect
import logging

import pytest


def _flags(monkeypatch, *, auto: str | None, domains: str | None) -> None:
    for prefix in ("DATAWRAP_", "DATAFLOW_"):
        monkeypatch.delenv(prefix + "SSO_AUTO_PROVISION", raising=False)
        monkeypatch.delenv(prefix + "SSO_ALLOWED_DOMAINS", raising=False)
    if auto is not None:
        monkeypatch.setenv("DATAWRAP_SSO_AUTO_PROVISION", auto)
    if domains is not None:
        monkeypatch.setenv("DATAFLOW_SSO_ALLOWED_DOMAINS", domains)


def test_auto_provision_without_a_domain_list_warns(monkeypatch, caplog):
    from services.sso_authorization import warn_sso_auto_provision_without_domains

    _flags(monkeypatch, auto="1", domains=None)
    with caplog.at_level(logging.WARNING):
        assert warn_sso_auto_provision_without_domains(logging.getLogger("datawrap.security")) is True
    assert "SSO_ALLOWED_DOMAINS" in caplog.text and "no new" in caplog.text


@pytest.mark.parametrize(("auto", "domains"), [("1", "corp.example"), ("0", None), (None, None)])
def test_no_warning_when_domains_are_set_or_auto_provision_is_off(monkeypatch, caplog, auto, domains):
    from services.sso_authorization import warn_sso_auto_provision_without_domains

    _flags(monkeypatch, auto=auto, domains=domains)
    with caplog.at_level(logging.WARNING):
        assert warn_sso_auto_provision_without_domains(logging.getLogger("datawrap.security")) is False
    assert "SSO_ALLOWED_DOMAINS" not in caplog.text


def test_the_api_warns_at_startup():
    from src import main

    assert "warn_sso_auto_provision_without_domains" in inspect.getsource(main.lifespan)
