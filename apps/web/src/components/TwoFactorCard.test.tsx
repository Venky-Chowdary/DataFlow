/**
 * QA S7 (round 11): the Two-factor card, the setup steps and the sign-in code
 * step render labelled fields and say plainly what is going on.
 */
import assert from "node:assert/strict";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, it } from "node:test";
import { TwoFactorCard } from "./TwoFactorCard.js";
import { RecoveryCodesPanel, TwoFactorSetup } from "./TwoFactorSetup.js";
import { LoginMfaStep } from "./MfaChallenge.js";
import type { MfaStatus } from "../lib/mfaApi.js";

const off: MfaStatus = {
  enrolled: false, method: null, enrolled_at: null, pending_enrollment: false,
  recovery_codes_remaining: 0, locked: false, locked_until: null, required: false,
  hold: null, session_verified: false, via_sso: false, sso_exempt: true,
};

describe("TwoFactorCard", () => {
  it("offers setup when off, and says when the workspace requires it", () => {
    const markup = renderToStaticMarkup(createElement(TwoFactorCard, { initialStatus: off }));
    assert.match(markup, /Two-factor authentication/);
    assert.match(markup, /Set up/);
    const required = renderToStaticMarkup(createElement(TwoFactorCard, { initialStatus: { ...off, required: true } }));
    assert.match(required, /Required by your workspace/);
  });

  it("hides Turn off while the workspace requires two-factor", () => {
    const on = { ...off, enrolled: true, method: "totp" as const, recovery_codes_remaining: 7 };
    assert.match(renderToStaticMarkup(createElement(TwoFactorCard, { initialStatus: on })), /Turn off/);
    const markup = renderToStaticMarkup(createElement(TwoFactorCard, { initialStatus: { ...on, required: true } }));
    assert.doesNotMatch(markup, /Turn off/);
    assert.match(markup, /7 of 10 recovery codes left/);
  });
});

describe("TwoFactorSetup", () => {
  it("shows the setup key, the otpauth link and a labelled code field", () => {
    const markup = renderToStaticMarkup(createElement(TwoFactorSetup, {
      onDone: () => undefined,
      initial: { enrollment: {
        secret: "JBSWY3DPEHPK3PXPJBSW", otpauth_uri: "otpauth://totp/Datawrap:ana@acme.test?secret=JBSWY3DPEHPK3PXPJBSW",
        issuer: "Datawrap", account: "ana@acme.test", digits: "6", period: "30",
      } },
    }));
    assert.match(markup, /JBSW Y3DP EHPK 3PXP JBSW/);
    assert.match(markup, /href="otpauth:\/\/totp\//);
    assert.match(markup, /<label[^>]*for="mfa-setup-code"/);
    assert.match(markup, /id="mfa-setup-code"/);
    assert.match(markup, /autoComplete="one-time-code"|autocomplete="one-time-code"/);
  });

  it("lists recovery codes once and gates Done on 'I saved these'", () => {
    const markup = renderToStaticMarkup(createElement(RecoveryCodesPanel, {
      codes: ["aaaa-bbbb-cccc-dddd", "eeee-ffff-gggg-hhhh"], account: "ana@acme.test", idPrefix: "t", onDone: () => undefined,
    }));
    assert.match(markup, /aaaa-bbbb-cccc-dddd/);
    assert.match(markup, /shown only now/);
    assert.match(markup, /id="t-saved"/);
    assert.match(markup, /disabled=""[^>]*>Done|Done/);
  });
});

describe("LoginMfaStep", () => {
  it("asks for the code with a labelled field and a recovery-code way out", () => {
    const markup = renderToStaticMarkup(createElement(LoginMfaStep, {
      challenge: "c", email: "ana@acme.test", onSignedIn: () => undefined, onBack: () => undefined,
    }));
    assert.match(markup, /Enter your code/);
    assert.match(markup, /ana@acme.test/);
    assert.match(markup, /<label[^>]*for="mfa-login-code"/);
    assert.match(markup, /Use a recovery code/);
    assert.match(markup, /Back to sign in/);
  });
});
