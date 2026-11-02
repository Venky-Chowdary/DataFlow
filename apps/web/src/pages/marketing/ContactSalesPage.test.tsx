import assert from "node:assert/strict";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, it } from "node:test";
import { ContactSalesPage, contactSalesError } from "./ContactSalesPage.js";
import { turnstileRenderOptions } from "../../components/TurnstileWidget.js";

/** D103: a lead the API refused must say so — never a false "sent". */
describe("contactSalesError", () => {
  it("names the direct address on every refusal", () => {
    for (const [status, data] of [
      [429, { error: "rate_limited" }],
      [400, { error: "turnstile_required" }],
      [422, { error: "invalid email address" }],
      [401, { detail: "Authentication required" }],
      [500, null],
    ] as const) {
      assert.match(contactSalesError(status, data), /sales@datawrap\.io|Check the form/);
    }
  });

  it("tells a rate-limited visitor to write directly", () => {
    assert.match(contactSalesError(429, { error: "rate_limited" }), /Too many submissions/);
  });

  it("asks for the human check again on a Turnstile refusal", () => {
    assert.match(contactSalesError(400, { error: "turnstile_required" }), /verify this browser/);
  });

  it("surfaces the server's validation reason", () => {
    assert.equal(contactSalesError(422, { error: "invalid email address" }), "Check the form: invalid email address.");
  });
});

describe("ContactSalesPage", () => {
  it("renders the form (not the success panel) before a submission", () => {
    const markup = renderToStaticMarkup(createElement(ContactSalesPage, { onNavigate: () => {} }));
    assert.match(markup, /Request pilot/);
    assert.doesNotMatch(markup, /Request received/);
  });
});

describe("turnstileRenderOptions action", () => {
  it("names the form so the API can refuse a token minted elsewhere", () => {
    assert.equal(turnstileRenderOptions("k", "light", () => {}, "contact_sales").action, "contact_sales");
    assert.equal("action" in turnstileRenderOptions("k", "light", () => {}), false);
  });
});
