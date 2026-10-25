import assert from "node:assert/strict";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, it } from "node:test";
import { UnverifiedAlert } from "./LoginPage.js";

/**
 * Back-fill for a3bb18e1: the email_unverified alert action must be the
 * styled AuthButton link variant — a bare <button> regressed to the
 * native beveled look.
 */
describe("UnverifiedAlert", () => {
  it("renders the action as a link-styled AuthButton", () => {
    const markup = renderToStaticMarkup(
      createElement(UnverifiedAlert, {
        challengeId: "otc_1",
        email: "ada@example.io",
        error: "Verify your email to continue.",
        onVerifyPending: () => {},
      }),
    );
    assert.match(markup, /role="alert"/);
    assert.match(markup, /Enter verification code/);
    assert.match(markup, /lp-btn--link/);
    assert.doesNotMatch(markup, /<button(?![^>]*lp-btn)/);
  });

  it("omits the action when no verify handler is wired", () => {
    const markup = renderToStaticMarkup(
      createElement(UnverifiedAlert, {
        challengeId: "otc_1",
        email: "ada@example.io",
        error: "Verify your email to continue.",
      }),
    );
    assert.doesNotMatch(markup, /Enter verification code/);
  });
});
