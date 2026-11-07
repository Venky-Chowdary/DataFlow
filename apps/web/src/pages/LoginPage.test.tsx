import assert from "node:assert/strict";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, it } from "node:test";
import { LoginAuxRows, UnverifiedAlert } from "./LoginPage.js";

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

/**
 * Review-8 item 6: "Forgot password?" sits right-aligned on its own line
 * under the password field; the remember checkbox is the line below —
 * sharing one row wrapped the checkbox copy to two lines.
 */
describe("LoginAuxRows", () => {
  it("puts the forgot link on its own row above the checkbox row", () => {
    const markup = renderToStaticMarkup(
      createElement(LoginAuxRows, {
        remember: true,
        onRemember: () => {},
        showForgot: true,
        onForgot: () => {},
      }),
    );
    assert.match(markup, /lp-login-forgot-row/);
    assert.match(markup, /Forgot password\?/);
    // the link must NOT share the checkbox's row
    const forgotRow = markup.indexOf("lp-login-forgot-row");
    const checkRow = markup.indexOf("lp-login-remember");
    assert.ok(forgotRow > -1 && checkRow > -1);
    assert.ok(forgotRow < checkRow, "forgot link must render before the checkbox row");
    const rowHtml = markup.slice(markup.indexOf('class="lp-login-row"'));
    assert.doesNotMatch(rowHtml, /Forgot password\?/);
  });
});
