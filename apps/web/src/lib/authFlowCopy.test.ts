import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  changeEmailConfirmError,
  changeEmailRequestError,
  codeEntryError,
  mailSendError,
  signupRefusal,
} from "./authFlowCopy.js";

describe("codeEntryError", () => {
  it("counts down attempts on a wrong code", () => {
    assert.equal(
      codeEntryError({ code: "invalid_code", attemptsRemaining: 4 }, "x"),
      "That code isn't right. 4 attempts left.",
    );
    assert.equal(
      codeEntryError({ code: "invalid_code", attemptsRemaining: 1 }, "x"),
      "That code isn't right. 1 attempt left.",
    );
    assert.equal(
      codeEntryError({ code: "invalid_code", attemptsRemaining: 0 }, "x"),
      "That code isn't right. Try again.",
    );
    assert.equal(
      codeEntryError({ code: "invalid_code", attemptsRemaining: null }, "x"),
      "That code isn't right. Try again.",
    );
  });

  it("says expired for expired or used codes", () => {
    for (const code of ["code_expired", "otp_expired"]) {
      assert.equal(
        codeEntryError({ code }, "x"),
        "This code has expired. Request a new one.",
      );
    }
  });

  it("falls back to the server message, then the local fallback", () => {
    assert.equal(codeEntryError({ code: "other", message: "nope" }, "x"), "nope");
    assert.equal(codeEntryError({}, "x"), "x");
  });
});

describe("mailSendError", () => {
  it("names a retry window for quota and rate limits", () => {
    assert.equal(
      mailSendError({ code: "email_quota_exceeded", retryAfterSec: 300 }, "x"),
      "We couldn't send your code right now — try again in 5 min.",
    );
    assert.equal(
      mailSendError({ code: "signup_rate_limited", retryAfterSec: 120 }, "x"),
      "Too many attempts from this network — try again in 2 min.",
    );
    // A zero/short retry window still reads as at least one minute.
    assert.match(
      mailSendError({ code: "email_quota_exceeded", retryAfterSec: 0 }, "x"),
      /1 min\.$/,
    );
  });
});

describe("changeEmailRequestError", () => {
  it("maps the request-step refusals to specific copy", () => {
    assert.equal(changeEmailRequestError({ code: "same_address" }), "That's already your sign-in email.");
    assert.equal(changeEmailRequestError({ code: "email_taken" }), "That address already belongs to another account.");
    assert.equal(changeEmailRequestError({ code: "unavailable" }), "Email change is temporarily unavailable — try again shortly.");
    assert.match(changeEmailRequestError({ code: "email_quota_exceeded", retryAfterSec: 300 }), /try again in 5 min/);
    assert.equal(changeEmailRequestError({ code: "other" }), "Couldn't send the code. Try again.");
  });
});

describe("changeEmailConfirmError", () => {
  it("reuses the code-entry map plus email-change specifics", () => {
    assert.equal(
      changeEmailConfirmError({ code: "invalid_code", attemptsRemaining: 3 }),
      "That code isn't right. 3 attempts left.",
    );
    assert.equal(changeEmailConfirmError({ code: "code_expired" }), "This code has expired. Request a new one.");
    assert.equal(changeEmailConfirmError({ code: "email_taken" }), "That address already belongs to another account.");
    assert.equal(changeEmailConfirmError({ code: "unavailable" }), "Email change is temporarily unavailable — try again shortly.");
    assert.equal(changeEmailConfirmError({ code: "other" }), "Couldn't confirm the change. Try again.");
  });
});

/** UX-28: refusals showed raw API text ("this address can't be used for signup") next to a "Valid" badge. */
describe("signupRefusal", () => {
  it("puts an address refusal on the email field as a sentence", () => {
    assert.deepEqual(signupRefusal({ code: "blocked_address", message: "this address can't be used for signup" }), {
      field: "email",
      text: "This address can't be used to sign up. Use your work email.",
    });
    assert.equal(signupRefusal({ code: "undeliverable_domain", message: "that email domain can't receive mail" }).field, "email");
    assert.equal(signupRefusal({ code: "invalid_email" }).field, "email");
  });
  it("puts a password refusal on the password field", () => {
    assert.equal(signupRefusal({ code: "pwned_password", message: "x" }).field, "password");
    assert.equal(signupRefusal({ code: "invalid_password", message: "x" }).field, "password");
  });
  it("never shows raw API text: unknown messages become sentences", () => {
    const r = signupRefusal({ code: "something_new", message: "couldn't verify you're human — try again" });
    assert.equal(r.field, null);
    assert.equal(r.text, "Couldn't verify you're human — try again.");
  });
  it("keeps the quota and velocity copy", () => {
    assert.match(signupRefusal({ code: "signup_rate_limited", retryAfterSec: 120 }).text, /Too many attempts/);
  });
});
