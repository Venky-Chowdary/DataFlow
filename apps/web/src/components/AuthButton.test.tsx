import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { renderToStaticMarkup } from "react-dom/server";
import { AuthButton } from "./AuthButton.js";
import { DtIcon } from "./DtIcon.js";

describe("AuthButton", () => {
  it("renders the primary brand variant by default", () => {
    const html = renderToStaticMarkup(<AuthButton>Continue</AuthButton>);
    assert.match(html, /lp-btn/);
    assert.match(html, /lp-btn--brand/);
    assert.match(html, /type="button"/);
    assert.match(html, />Continue</);
  });

  it("maps every variant to its lp class", () => {
    for (const [variant, cls] of [
      ["primary", "lp-btn--brand"],
      ["secondary", "lp-btn--outline"],
      ["link", "lp-btn--link"],
      ["destructive", "lp-btn--danger"],
    ] as const) {
      const html = renderToStaticMarkup(<AuthButton variant={variant}>x</AuthButton>);
      assert.match(html, new RegExp(cls), `${variant} should emit ${cls}`);
    }
  });

  it("emits size classes and block", () => {
    const html = renderToStaticMarkup(
      <AuthButton size="lg" block>
        x
      </AuthButton>,
    );
    assert.match(html, /lp-btn--lg/);
    assert.match(html, /lp-btn--block/);
  });

  it("marks disabled with the attribute and aria-disabled", () => {
    const html = renderToStaticMarkup(<AuthButton disabled>x</AuthButton>);
    assert.match(html, /disabled=""|disabled /);
    assert.match(html, /aria-disabled="true"/);
  });

  it("loading shows a spinner, busy label, aria-busy and blocks resubmit", () => {
    const html = renderToStaticMarkup(
      <AuthButton loading loadingLabel="Sending…">
        Send code
      </AuthButton>,
    );
    assert.match(html, /lp-btn-spinner/);
    assert.match(html, /aria-busy="true"/);
    assert.match(html, /aria-disabled="true"/);
    assert.match(html, /Sending…/);
    assert.doesNotMatch(html, /Send code/);
    assert.match(html, /disabled/);
  });

  it("renders leading and trailing icons", () => {
    const html = renderToStaticMarkup(
      <AuthButton
        variant="link"
        leadingIcon={<DtIcon name="chevron-left" />}
        trailingIcon={<DtIcon name="arrow-right" />}
      >
        Back
      </AuthButton>,
    );
    assert.match(html, /lp-btn--link/);
    assert.match(html, /svg/);
    assert.match(html, /Back/);
  });
});
