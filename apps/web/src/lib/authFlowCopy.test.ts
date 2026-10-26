import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  changeEmailConfirmError,
  changeEmailRequestError,
  codeEntryError,
  mailSendError,
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
