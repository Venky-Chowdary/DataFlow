"""Settings -> SSO, through the real routes on Mongo (QA enterprise #13, #14, #15).

QA (production 557f556c):

* #13 ``PATCH /workspace/sso/saml {enabled: true}`` with blank entity ID, URL
  and certificate saved ``enabled=true`` - validation only *reported*
  "Missing ...". Root cause: ``integrations_store.update_sso_config`` wrote
  whatever it was given; ``validate_sso_config`` ran after the write.
* #14 ``email_domains`` saved as ``[]``. Root cause: ``SsoConfigBody`` had no
  ``email_domains`` field, so pydantic dropped it before the store saw it, and
  ``/auth/sso/route`` could never route anyone.
* #15 no way to clear a stored client secret (blank means keep).
"""

from __future__ import annotations

from r8_ent_harness import ent  # noqa: F401

OIDC_COMPLETE = {
    "issuer": "https://idp.qa.invalid",
    "client_id": "datawrap",
    "client_secret": "s3cret-value-r8",
    "redirect_uri": "https://app.qa.invalid/api/v1/auth/sso/oidc/callback",
}


def _sso(ent) -> dict:
    res = ent.admin.get("/api/v1/workspace/sso")
    assert res.status_code == 200, res.text
    return res.json()["providers"]


def test_saml_cannot_be_enabled_with_required_fields_blank(ent):
    refused = ent.admin.patch(
        "/api/v1/workspace/sso/saml",
        json={"enabled": True, "entity_id": "", "sso_url": "", "x509_cert": ""},
    )
    assert refused.status_code == 400, refused.text
    detail = refused.json()["detail"]
    assert set(detail["missing_fields"]) == {"entity_id", "sso_url", "x509_cert"}
    assert _sso(ent)["saml"]["enabled"] is False  # nothing was written

    # A disabled draft saves incomplete.
    draft = ent.admin.patch(
        "/api/v1/workspace/sso/saml", json={"enabled": False, "entity_id": "urn:datawrap:sp"}
    )
    assert draft.status_code == 200, draft.text
    assert draft.json()["config"]["entity_id"] == "urn:datawrap:sp"
    assert draft.json()["validation"]["missing_fields"] == ["sso_url", "x509_cert"]

    # Enabling the draft later is still refused until it is complete...
    still = ent.admin.patch("/api/v1/workspace/sso/saml", json={"enabled": True})
    assert still.status_code == 400
    assert still.json()["detail"]["missing_fields"] == ["sso_url", "x509_cert"]
    # ...and allowed once it is.
    ok = ent.admin.patch(
        "/api/v1/workspace/sso/saml",
        json={
            "enabled": True,
            "sso_url": "https://idp.qa.invalid/saml",
            "x509_cert": "MIIC" + "A" * 60,
        },
    )
    assert ok.status_code == 200, ok.text
    assert ok.json()["validation"]["ok"] is True

    # Blanking a required field of an enabled provider is refused the same way.
    blank = ent.admin.patch("/api/v1/workspace/sso/saml", json={"entity_id": ""})
    assert blank.status_code == 400
    assert _sso(ent)["saml"]["entity_id"] == "urn:datawrap:sp"


def test_email_domains_persist_and_drive_sso_routing(ent, monkeypatch):
    # /auth/sso/route lives on the self-serve sign-in surface (on in production).
    monkeypatch.setenv("DATAWRAP_SIGNUP_ENABLED", "1")
    saved = ent.admin.patch(
        "/api/v1/workspace/sso/oidc",
        json={**OIDC_COMPLETE, "enabled": True, "email_domains": ["QA.invalid", "@corp.qa.invalid"]},
    )
    assert saved.status_code == 200, saved.text
    assert saved.json()["config"]["email_domains"] == ["qa.invalid", "corp.qa.invalid"]
    assert _sso(ent)["oidc"]["email_domains"] == ["qa.invalid", "corp.qa.invalid"]
    stored = ent.db["workspace_integrations"].find_one({"_id": "integrations"})
    assert stored["sso"]["oidc"]["email_domains"] == ["qa.invalid", "corp.qa.invalid"]

    public = ent.anonymous()
    routed = public.post("/api/v1/auth/sso/route", json={"email": "person@qa.invalid"})
    assert routed.status_code == 200, routed.text
    assert routed.json() == {"sso": "oidc", "start_url": "/api/v1/auth/sso/oidc/start"}
    assert public.post("/api/v1/auth/sso/route", json={"email": "x@CORP.qa.invalid"}).json()["sso"] == "oidc"
    assert public.post("/api/v1/auth/sso/route", json={"email": "x@other.invalid"}).json() == {"sso": None}

    # A malformed domain is refused, not silently dropped.
    bad = ent.admin.patch("/api/v1/workspace/sso/oidc", json={"email_domains": ["not a domain"]})
    assert bad.status_code == 400
    # One domain routes to one provider.
    clash = ent.admin.patch("/api/v1/workspace/sso/saml", json={"email_domains": ["qa.invalid"]})
    assert clash.status_code == 400
    assert "oidc" in clash.json()["detail"]["message"]


def test_client_secret_can_be_cleared_explicitly_and_blank_keeps_it(ent):
    assert ent.admin.patch("/api/v1/workspace/sso/oidc", json=OIDC_COMPLETE).status_code == 200
    kept = ent.admin.patch("/api/v1/workspace/sso/oidc", json={"client_secret": "", "client_id": "dw2"})
    assert kept.status_code == 200
    assert kept.json()["config"]["client_secret"] == "••••••••"  # blank = keep

    cleared = ent.admin.patch("/api/v1/workspace/sso/oidc", json={"clear_client_secret": True})
    assert cleared.status_code == 200, cleared.text
    assert cleared.json()["config"]["client_secret"] == ""
    assert "client_secret" in cleared.json()["validation"]["missing_fields"]
    stored = ent.db["workspace_integrations"].find_one({"_id": "integrations"})
    assert stored["sso"]["oidc"]["client_secret"] == ""

    # Clearing the secret of an enabled provider would break sign-in: refused.
    assert ent.admin.patch(
        "/api/v1/workspace/sso/oidc", json={**OIDC_COMPLETE, "enabled": True}
    ).status_code == 200
    refused = ent.admin.patch("/api/v1/workspace/sso/oidc", json={"clear_client_secret": True})
    assert refused.status_code == 400
    assert refused.json()["detail"]["missing_fields"] == ["client_secret"]
