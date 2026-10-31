/**
 * QA enterprise (557f556c) #13/#14/#15: SAML could be enabled blank, email
 * domains were dropped on save, and a stored client secret could not be
 * cleared. The editor now sends the domains, an explicit clear, and warns
 * before enabling an incomplete provider.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { buildSsoPatch, missingForEnable, parseEmailDomains } from "./ssoSettings.js";

describe("parseEmailDomains", () => {
  it("normalizes, strips @ and de-duplicates", () => {
    assert.deepEqual(parseEmailDomains(" Acme.com, @corp.io\nacme.com; "), ["acme.com", "corp.io"]);
    assert.deepEqual(parseEmailDomains(""), []);
  });
});

describe("buildSsoPatch", () => {
  it("always sends email_domains so a save never drops them", () => {
    const body = buildSsoPatch({ enabled: false, issuer: "https://idp" }, "qa.invalid", false);
    assert.deepEqual(body.email_domains, ["qa.invalid"]);
    assert.equal(body.clear_client_secret, undefined);
  });

  it("sends an explicit clear instead of a blank secret", () => {
    const body = buildSsoPatch({ enabled: false, client_secret: "" }, "", true);
    assert.equal(body.clear_client_secret, true);
    assert.equal("client_secret" in body, false);
  });
});

describe("missingForEnable", () => {
  it("lists blank SAML fields", () => {
    assert.deepEqual(
      missingForEnable("saml", { enabled: true, entity_id: "", sso_url: "", x509_cert: "" }, undefined, false),
      ["Entity ID", "SSO URL", "IdP certificate"],
    );
  });

  it("counts a saved (masked) secret as set unless it is being cleared", () => {
    const draft = { enabled: true, issuer: "https://idp", client_id: "c", client_secret: "", redirect_uri: "https://cb" };
    const saved = { enabled: false, client_secret: "••••••••" };
    assert.deepEqual(missingForEnable("oidc", draft, saved, false), []);
    assert.deepEqual(missingForEnable("oidc", draft, saved, true), ["Client secret"]);
  });
});
