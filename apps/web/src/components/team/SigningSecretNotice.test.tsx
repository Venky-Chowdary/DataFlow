/**
 * QA enterprise (557f556c) #46: webhooks carried no signature. Each webhook
 * channel now has a signing secret, shown once (masked until revealed).
 */
import assert from "node:assert/strict";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, it } from "node:test";
import { SigningSecretNotice } from "./SigningSecretNotice.js";

describe("SigningSecretNotice", () => {
  const props = { label: "Ops hook", secret: "whsec_abc123", onDone: () => {} };

  it("keeps the secret masked until revealed and explains the header", () => {
    const markup = renderToStaticMarkup(createElement(SigningSecretNotice, props));
    assert.doesNotMatch(markup, /whsec_abc123/);
    assert.match(markup, /X-Datawrap-Signature/);
    assert.match(markup, /Shown once/);
  });

  it("shows the secret when revealed", () => {
    const markup = renderToStaticMarkup(createElement(SigningSecretNotice, { ...props, initiallyRevealed: true }));
    assert.match(markup, /whsec_abc123/);
  });
});
