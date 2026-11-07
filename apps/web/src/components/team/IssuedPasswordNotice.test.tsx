/**
 * QA (557f556c): "Create a login (one-time password)" was checked by default,
 * so a casual invite put a password on screen (and in demo recordings).
 * The invite defaults to the email flow, and an issued password stays masked
 * until the admin asks to see it.
 */
import assert from "node:assert/strict";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, it } from "node:test";
import { IssuedPasswordNotice } from "./IssuedPasswordNotice.js";
import { INVITE_DEFAULTS } from "../../lib/teamInvite.js";

describe("team invite defaults", () => {
  it("does not create a login unless the admin asks for one", () => {
    assert.equal(INVITE_DEFAULTS.createLogin, false);
  });
});

describe("IssuedPasswordNotice", () => {
  const props = { email: "ada@acme.example", password: "Xk9!q-7Lm2-Pw", onDone: () => {} };

  it("keeps the one-time password off screen until revealed", () => {
    const markup = renderToStaticMarkup(createElement(IssuedPasswordNotice, props));
    assert.doesNotMatch(markup, /Xk9!q-7Lm2-Pw/);
    assert.match(markup, /Reveal password/);
    assert.match(markup, /ada@acme\.example/);
  });

  it("shows it when the admin has revealed it", () => {
    const markup = renderToStaticMarkup(createElement(IssuedPasswordNotice, { ...props, initiallyRevealed: true }));
    assert.match(markup, /Xk9!q-7Lm2-Pw/);
    assert.match(markup, /Hide/);
  });
});
