import assert from "node:assert/strict";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, it } from "node:test";
import { focusFirstInvalid, ResendRow } from "../components/AuthFlow.js";
import { ChangeEmailCard } from "../components/ChangeEmailCard.js";

describe("ResendRow", () => {
  it("renders the cooldown state muted, aria-disabled, and announces via aria-live", () => {
    const markup = renderToStaticMarkup(
      createElement(ResendRow, { challengeId: "otc_x", initialCooldown: 42 }),
    );
    assert.match(markup, /Resend in 42s/);
    assert.match(markup, /aria-disabled="true"/);
    assert.match(markup, /disabled=""/);
    assert.match(markup, /aria-live="polite"/);
    assert.match(markup, /request a new code in 42 seconds/);
  });

  it("renders an active resend link once the cooldown is zero", () => {
    const markup = renderToStaticMarkup(
      createElement(ResendRow, { challengeId: "otc_x", initialCooldown: 0 }),
    );
    assert.match(markup, /Resend code/);
    assert.doesNotMatch(markup, /disabled=""/);
  });
});

describe("focusFirstInvalid", () => {
  it("focuses the first aria-invalid control inside the card", () => {
    const focused: string[] = [];
    const fakeField = { focus: () => focused.push("focused") };
    const root = {
      querySelector: (selector: string) => {
        assert.match(selector, /aria-invalid/);
        return fakeField;
      },
    };
    focusFirstInvalid(root as unknown as HTMLElement);
    assert.deepEqual(focused, ["focused"]);
  });

  it("does nothing on a card with no invalid fields", () => {
    const root = { querySelector: () => null };
    assert.doesNotThrow(() =>
      focusFirstInvalid(root as unknown as HTMLElement));
  });

  it("does nothing without a root", () => {
    assert.doesNotThrow(() => focusFirstInvalid(null));
  });
});

describe("ChangeEmailCard", () => {
  it("renders the request step with a labeled field and df2 classes", () => {
    const markup = renderToStaticMarkup(
      createElement(ChangeEmailCard, { enabled: true }),
    );
    assert.match(markup, /df2-settings/);
    assert.match(markup, /New sign-in email|new-email|email/i);
    assert.match(markup, /df2-btn/);
  });
});
