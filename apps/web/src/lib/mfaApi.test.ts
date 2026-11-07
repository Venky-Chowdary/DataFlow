/**
 * QA S7 (round 11): two-factor client helpers — the login branch, input
 * shaping and the copy shown for a refused code.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { ApiError } from "./api.js";
import {
  formatSetupKey,
  isCompleteTotp,
  isMfaChallenge,
  isRecoveryCodeShape,
  mfaErrorMessage,
  normalizeTotpInput,
  recoveryCodesFile,
} from "./mfaApi.js";

describe("two-factor helpers", () => {
  it("tells a challenge (202) from a session", () => {
    assert.equal(isMfaChallenge({ mfa_required: true, challenge: "abc.def", challenge_expires_at: 1, methods: [] }), true);
    assert.equal(isMfaChallenge({ token: "t", expires_at: 1, user: { email: "a@b.c", name: "A", role: "member" } }), false);
  });

  it("keeps only six digits of an authenticator code", () => {
    assert.equal(normalizeTotpInput("123 456"), "123456");
    assert.equal(normalizeTotpInput("12-34-56-78"), "123456");
    assert.equal(normalizeTotpInput("abc"), "");
    assert.equal(isCompleteTotp("123456"), true);
    assert.equal(isCompleteTotp("12345"), false);
  });

  it("accepts recovery codes regardless of case and separators", () => {
    assert.equal(isRecoveryCodeShape("abcd-efgh-jkmn-pqrs"), true);
    assert.equal(isRecoveryCodeShape("ABCD EFGH JKMN PQRS"), true);
    assert.equal(isRecoveryCodeShape("abcd-efgh"), false);
  });

  it("groups the setup key in fours", () => {
    assert.equal(formatSetupKey("JBSWY3DPEHPK3PXPJBSW"), "JBSW Y3DP EHPK 3PXP JBSW");
  });

  it("writes a recovery-code file naming the account", () => {
    const file = recoveryCodesFile(["aaaa-bbbb-cccc-dddd"], "ana@acme.test");
    assert.match(file, /ana@acme.test/);
    assert.match(file, /aaaa-bbbb-cccc-dddd/);
    assert.match(file, /works once/);
  });

  it("explains a lockout in plain words and keeps the API's reason otherwise", () => {
    assert.match(mfaErrorMessage(new ApiError("x", 429)), /locked/);
    assert.equal(mfaErrorMessage(new ApiError("That code was already used.", 401)), "That code was already used.");
  });
});
